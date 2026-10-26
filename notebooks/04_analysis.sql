-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Analysis — answering the §3–§5 questions in SQL
-- MAGIC
-- MAGIC All queries run off the `stream_batch_progress` view (§6). The trigger
-- MAGIC interval in the reference pipeline is 5s; adjust `:trigger_ms` to match if changed.

-- COMMAND ----------

USE CATALOG observability;
USE SCHEMA streaming;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## The headline (§2/§3): throughput looks flat while backlog grows
-- MAGIC Rates stay roughly constant (consumer throttled) even as the Kafka backlog
-- MAGIC climbs — proof that **rates alone are misleading** and backlog is the signal.

-- COMMAND ----------

SELECT
  batch_id,
  batch_timestamp,
  ROUND(input_rows_per_sec)     AS in_rps,
  ROUND(processed_rows_per_sec) AS proc_rps,
  max_offsets_behind,
  ROUND(est_bytes_behind / 1e6, 1) AS mb_behind
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
ORDER BY batch_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Can the job keep up? (§3) — scheduling pressure + backlog trend
-- MAGIC `scheduling_pressure = batch_duration / trigger_interval` (derived heuristic).
-- MAGIC `backlog_delta` > 0 across consecutive batches = falling behind.

-- COMMAND ----------

SELECT
  batch_id,
  batch_duration_ms,
  ROUND(batch_duration_ms / 5000.0, 2) AS scheduling_pressure,   -- trigger = 5000 ms
  max_offsets_behind,
  max_offsets_behind - LAG(max_offsets_behind) OVER (ORDER BY batch_id) AS backlog_delta,
  CASE
    WHEN max_offsets_behind - LAG(max_offsets_behind) OVER (ORDER BY batch_id) > 0 THEN 'falling_behind'
    WHEN max_offsets_behind - LAG(max_offsets_behind) OVER (ORDER BY batch_id) < 0 THEN 'catching_up'
    ELSE 'steady'
  END AS backlog_state
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
ORDER BY batch_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Partition skew (§3) — gap between max and avg offsets behind
-- MAGIC A large max-vs-avg gap points at one hot partition stalling the batch.

-- COMMAND ----------

SELECT
  batch_id,
  max_offsets_behind,
  ROUND(avg_offsets_behind) AS avg_offsets_behind,
  min_offsets_behind,
  max_offsets_behind - ROUND(avg_offsets_behind) AS skew_gap
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
ORDER BY batch_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Where is time going? (§3) — durationMs phase breakdown

-- COMMAND ----------

SELECT
  batch_id,
  batch_duration_ms,
  dur_add_batch_ms,
  dur_latest_offset_ms,
  dur_query_planning_ms,
  dur_wal_commit_ms
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
ORDER BY batch_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Output freshness (§3) — age of the latest committed sink row
-- MAGIC End-to-end latency your downstream consumers actually feel.

-- COMMAND ----------

SELECT
  MAX(ingest_ts)                                         AS latest_sink_commit,
  current_timestamp()                                    AS now,
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(ingest_ts))), 1) AS output_freshness_seconds
FROM observability.trading.enriched_trades;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Data quality (§2) — malformed records caught per batch via df.observe()

-- COMMAND ----------

SELECT
  batch_id,
  dq_rows,
  dq_null_symbol,
  dq_bad_price,
  ROUND(100.0 * (dq_null_symbol + dq_bad_price) / NULLIF(dq_rows, 0), 2) AS malformed_pct
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
ORDER BY batch_id;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Alert-style rollups (§5) — simple threshold checks over the window

-- COMMAND ----------

SELECT
  query_name,
  COUNT(*)                                            AS batches,
  ROUND(percentile(batch_duration_ms, 0.95))          AS p95_batch_ms,
  MAX(max_offsets_behind)                             AS peak_backlog,
  -- falling-behind if backlog at the end exceeds the start materially
  bool_or(max_offsets_behind > 100000)                AS backlog_breached_100k
FROM stream_batch_progress
WHERE query_name = 'enrich_trades'
GROUP BY query_name;
