# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming pipeline: Kafka → join static dims → transform → Delta
# MAGIC
# MAGIC The **consumption** pipeline. Reads a Kafka topic of trade events, parses the
# MAGIC JSON payload, **joins** the static `instrument` and `account` dimensions
# MAGIC (stream-static joins), derives a few transforms, and writes enriched trades
# MAGIC to a Delta sink. Point it at your own Kafka via `bootstrap` and the
# MAGIC `subscribe` topic.
# MAGIC
# MAGIC Observability wired in per the design doc:
# MAGIC - `df.observe("dq", …)` → per-batch data-quality metrics land in the same
# MAGIC   progress event the listener captures (§2 Custom / DQ).
# MAGIC - `maxOffsetsPerTrigger` caps rows read per trigger — a throughput knob you
# MAGIC   tune; watch its effect on Kafka backlog in the model (§3).
# MAGIC - the sink write stamps `ingest_ts` so we can compute **output freshness**.
# MAGIC
# MAGIC Requires (run first in the session): `00_static_dims` (the join tables) and
# MAGIC `01_observability_listener` (listener registered), plus a reachable Kafka
# MAGIC cluster — pass its bootstrap servers as `bootstrap`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, TimestampType

CATALOG = "observability"   # change to your Unity Catalog
SINK_TABLE = f"{CATALOG}.trading.enriched_trades"

TRADE_SCHEMA = StructType([
    StructField("trade_id", LongType()),
    StructField("ts", TimestampType()),
    StructField("symbol", StringType()),
    StructField("account_id", StringType()),
    StructField("side", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
])


def start_pipeline(bootstrap, max_offsets_per_trigger=3000, trigger_interval="5 seconds", name="enrich_trades"):
    instrument = spark.table(f"{CATALOG}.trading.instrument")
    account = spark.table(f"{CATALOG}.trading.account")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", "trades")
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", max_offsets_per_trigger)   # cap rows per trigger (tune for your throughput)
           .load())

    parsed = (raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
                 .select("t.*"))

    # --- df.observe(): per-batch data-quality metrics (§2) -----------------
    observed = parsed.observe(
        "dq",
        F.count(F.expr("1")).alias("rows"),
        F.sum(F.expr("CASE WHEN symbol IS NULL THEN 1 ELSE 0 END")).alias("null_symbol"),
        F.sum(F.expr("CASE WHEN price <= 0 THEN 1 ELSE 0 END")).alias("bad_price"),
        F.min("ts").alias("min_event_ts"),
        F.max("ts").alias("max_event_ts"),
    )

    # --- stream-static joins + transforms ----------------------------------
    enriched = (observed
        .where(F.col("symbol").isNotNull() & (F.col("price") > 0))   # drop malformed before enrich
        .join(F.broadcast(instrument), "symbol", "left")
        .join(F.broadcast(account), "account_id", "left")
        .withColumn("notional", F.round(F.col("price") * F.col("quantity"), 2))
        .withColumn("ingest_ts", F.current_timestamp()))

    return (enriched.writeStream
        .format("delta")
        # /local_disk0 is ephemeral; use a durable path (UC Volume / cloud storage) in production.
        .option("checkpointLocation", f"/local_disk0/chk/{name}")
        .outputMode("append")
        .queryName(name)
        .trigger(processingTime=trigger_interval)
        .toTable(SINK_TABLE))
