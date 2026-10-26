# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming observability — listener + async Delta writer
# MAGIC
# MAGIC Registers a `StreamingQueryListener` that captures one progress record per
# MAGIC micro-batch (for **every** query on the session) and persists it to a Delta
# MAGIC table via a non-blocking background worker. Mirrors §1 of the design doc.
# MAGIC
# MAGIC - Raw table lands the full progress JSON in a **VARIANT** column — no schema
# MAGIC   migration when Spark's progress payload changes across versions.
# MAGIC - Typed facts/views are built on top in `05_model_views.sql` (§6).

# COMMAND ----------

CATALOG = "observability"   # change to your Unity Catalog
RAW_TABLE = f"{CATALOG}.streaming.query_progress_raw"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
  payload   VARIANT,
  ingest_ts TIMESTAMP
) USING delta
""")

# COMMAND ----------

import json, threading, time
from queue import Queue, Empty, Full
from pyspark.sql.streaming import StreamingQueryListener

progress_queue: "Queue[str]" = Queue(maxsize=10000)   # bounded: back-pressure, never OOM
dropped_counter = {"n": 0}                             # monitoring-blind signal (§5)


class MetricsListener(StreamingQueryListener):
    """One listener logs EVERY query on the session to ONE table."""

    def onQueryStarted(self, event):
        pass  # synchronous — keep trivial

    def onQueryProgress(self, event):
        try:
            progress_queue.put_nowait(event.progress.json)  # cheap enqueue only
        except Full:
            dropped_counter["n"] += 1                        # never block the Spark thread

    def onQueryIdle(self, event):
        pass  # waiting for data — NOT a failure

    def onQueryTerminated(self, event):
        try:
            progress_queue.put_nowait(json.dumps({
                "id": str(event.id), "runId": str(event.runId),
                "terminated": True, "exception": event.exception,
            }))
        except Full:
            dropped_counter["n"] += 1


def _delta_writer(stop_event, flush_secs=10, max_batch=500):
    """Drain the queue, buffer, and batch-append to Delta off the Spark thread."""
    buffer, last_flush = [], time.monotonic()
    while not (stop_event.is_set() and progress_queue.empty()):
        try:
            buffer.append(progress_queue.get(timeout=1.0))
        except Empty:
            pass
        due = (time.monotonic() - last_flush) >= flush_secs
        if buffer and (len(buffer) >= max_batch or due):
            # NaN/Infinity are not valid JSON; parse_json rejects them. Spark's
            # progress payload can contain them (e.g. rates before the first
            # batch), so scrub them to null before the write.
            (spark.createDataFrame([(r,) for r in buffer], "payload STRING")
                  .selectExpr(
                      "parse_json(regexp_replace(payload, "
                      r"'(?<=:)\\s*(NaN|-?Infinity)', 'null')) AS payload",
                      "current_timestamp() AS ingest_ts")
                  .write.format("delta").mode("append").saveAsTable(RAW_TABLE))
            buffer.clear()
            last_flush = time.monotonic()


# Register exactly once per session.
try:
    stop_event  # type: ignore[name-defined]
    print("listener already registered this session")
except NameError:
    stop_event = threading.Event()
    threading.Thread(target=_delta_writer, args=(stop_event,), daemon=True).start()
    listener = MetricsListener()
    spark.streams.addListener(listener)   # register BEFORE starting any query
    print("listener registered; writer thread started ->", RAW_TABLE)

# On controlled shutdown: stop_event.set(); spark.streams.removeListener(listener)
