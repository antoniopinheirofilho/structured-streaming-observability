-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Observability data model — views on the VARIANT raw table (§6)
-- MAGIC
-- MAGIC `query_progress_raw` holds one VARIANT `payload` per progress event. These
-- MAGIC views parse typed columns with the `:` / `variant_get` accessors, so the
-- MAGIC raw table never needs a schema migration when Spark's payload changes.
-- MAGIC
-- MAGIC All views and columns carry `COMMENT`s so AI/BI Genie (and anyone browsing
-- MAGIC Catalog Explorer) understands the semantics without reading this code.

-- COMMAND ----------

USE CATALOG observability;
USE SCHEMA streaming;

-- COMMAND ----------
-- MAGIC %md ## Fact: stream_batch_progress (one row per query per micro-batch)

-- COMMAND ----------

CREATE OR REPLACE VIEW stream_batch_progress (
  query_id                COMMENT 'Stable StreamingQuery id; persists across query restarts. Use as the join key for a logical stream.',
  run_id                  COMMENT 'Run id; a new value is assigned on every start/restart of the query.',
  batch_id                COMMENT 'Micro-batch sequence number for the run. Can re-execute after failure, so it is NOT a unique business event.',
  query_name              COMMENT 'Human-readable query name set via .queryName(); e.g. enrich_trades for the consumption pipeline.',
  batch_timestamp         COMMENT 'Wall-clock time Spark finalized this micro-batch progress event (UTC).',
  num_input_rows          COMMENT 'Rows read from all sources in this micro-batch.',
  input_rows_per_sec      COMMENT 'Input rate: rows arriving per second, computed from rows Spark actually processed (NOT true producer demand). Misleading alone when the stream is throttled/busy — use with backlog.',
  processed_rows_per_sec  COMMENT 'Processing rate: rows processed per second this batch. Converges with input rate under continuous load even as backlog grows.',
  batch_duration_ms       COMMENT 'Total wall-clock milliseconds to execute this micro-batch (= durationMs.triggerExecution).',
  dur_add_batch_ms        COMMENT 'durationMs.addBatch: time writing the batch to the sink / shuffle / compute. Growing => sink or compute bound.',
  dur_latest_offset_ms    COMMENT 'durationMs.latestOffset: time querying the source for the latest offsets. Growing => source (Kafka broker) slow.',
  dur_query_planning_ms   COMMENT 'durationMs.queryPlanning: planning time. Growing => too many small files / overgrown query plan.',
  dur_wal_commit_ms       COMMENT 'durationMs.walCommit: time committing the write-ahead log to checkpoint storage. Growing => checkpoint object-store latency.',
  dur_trigger_execution_ms COMMENT 'durationMs.triggerExecution: end-to-end trigger execution time (equals batch_duration_ms).',
  max_offsets_behind      COMMENT 'Kafka maxOffsetsBehindLatest: worst-partition consumer lag in offsets. Primary falling-behind signal; catches partition skew that averages hide.',
  avg_offsets_behind      COMMENT 'Kafka avgOffsetsBehindLatest: average consumer lag in offsets across partitions.',
  min_offsets_behind      COMMENT 'Kafka minOffsetsBehindLatest: best-partition consumer lag in offsets.',
  est_bytes_behind        COMMENT 'Kafka estimatedTotalBytesBehindLatest: estimated bytes awaiting consumption. Best proxy for upstream producer pressure.',
  state_num_rows_total    COMMENT 'stateOperators[0].numRowsTotal: rows held in state (stateful queries only; null for this stateless pipeline).',
  state_memory_used_bytes COMMENT 'stateOperators[0].memoryUsedBytes: state store memory in bytes. Climbing while watermark advances => possible state leak.',
  rows_dropped_by_watermark COMMENT 'stateOperators[0].numRowsDroppedByWatermark: late rows dropped by the watermark. High => watermark may be too aggressive.',
  dq_rows                 COMMENT 'Custom df.observe("dq") metric: total rows seen this batch.',
  dq_null_symbol          COMMENT 'Custom df.observe("dq") metric: rows with a null symbol (malformed) this batch.',
  dq_bad_price            COMMENT 'Custom df.observe("dq") metric: rows with price <= 0 (malformed) this batch.',
  dq_min_event_ts         COMMENT 'Custom df.observe("dq") metric: minimum event timestamp in the batch (data freshness floor).',
  dq_max_event_ts         COMMENT 'Custom df.observe("dq") metric: maximum event timestamp in the batch (data freshness ceiling).',
  ingest_ts               COMMENT 'Time this progress record was written to the observability table by the async Delta writer.'
)
COMMENT 'One row per streaming query per micro-batch, parsed from the VARIANT progress payload. The core fact for streaming health: throughput, batch timing, durationMs phase breakdown, Kafka backlog, state, and custom data-quality (df.observe) metrics. Grain: (query_id, run_id, batch_id). Source: observability.streaming.query_progress_raw (Spark StreamingQueryListener).'
AS
SELECT
  payload:id::string                                                 AS query_id,
  payload:runId::string                                              AS run_id,
  payload:batchId::bigint                                            AS batch_id,
  payload:name::string                                               AS query_name,
  payload:timestamp::timestamp                                       AS batch_timestamp,
  payload:numInputRows::bigint                                       AS num_input_rows,
  payload:inputRowsPerSecond::double                                 AS input_rows_per_sec,
  payload:processedRowsPerSecond::double                             AS processed_rows_per_sec,
  payload:batchDuration::bigint                                      AS batch_duration_ms,
  payload:durationMs:addBatch::bigint                                AS dur_add_batch_ms,
  payload:durationMs:latestOffset::bigint                            AS dur_latest_offset_ms,
  payload:durationMs:queryPlanning::bigint                           AS dur_query_planning_ms,
  payload:durationMs:walCommit::bigint                               AS dur_wal_commit_ms,
  payload:durationMs:triggerExecution::bigint                        AS dur_trigger_execution_ms,
  payload:sources[0]:metrics:maxOffsetsBehindLatest::bigint          AS max_offsets_behind,
  payload:sources[0]:metrics:avgOffsetsBehindLatest::double          AS avg_offsets_behind,
  payload:sources[0]:metrics:minOffsetsBehindLatest::bigint          AS min_offsets_behind,
  payload:sources[0]:metrics:estimatedTotalBytesBehindLatest::double AS est_bytes_behind,
  payload:stateOperators[0]:numRowsTotal::bigint                     AS state_num_rows_total,
  payload:stateOperators[0]:memoryUsedBytes::bigint                  AS state_memory_used_bytes,
  payload:stateOperators[0]:numRowsDroppedByWatermark::bigint        AS rows_dropped_by_watermark,
  payload:observedMetrics:dq:rows::bigint                            AS dq_rows,
  payload:observedMetrics:dq:null_symbol::bigint                     AS dq_null_symbol,
  payload:observedMetrics:dq:bad_price::bigint                       AS dq_bad_price,
  payload:observedMetrics:dq:min_event_ts::timestamp                 AS dq_min_event_ts,
  payload:observedMetrics:dq:max_event_ts::timestamp                 AS dq_max_event_ts,
  ingest_ts
FROM query_progress_raw
WHERE payload:batchId IS NOT NULL;

-- COMMAND ----------
-- MAGIC %md ## Fact: stream_lifecycle_events (query terminated markers)

-- COMMAND ----------

CREATE OR REPLACE VIEW stream_lifecycle_events (
  query_id   COMMENT 'Stable StreamingQuery id of the terminated query.',
  run_id     COMMENT 'Run id that terminated.',
  terminated COMMENT 'Always true in this view: the query reported onQueryTerminated.',
  exception  COMMENT 'Exception message if the query terminated abnormally; null for a clean stop.',
  ingest_ts  COMMENT 'Time this lifecycle record was written to the observability table.'
)
COMMENT 'Query lifecycle events (terminations) captured from StreamingQueryListener.onQueryTerminated. A non-null exception indicates a failure/flapping condition (§5 alerting). Source: observability.streaming.query_progress_raw.'
AS
SELECT
  payload:id::string          AS query_id,
  payload:runId::string       AS run_id,
  payload:terminated::boolean AS terminated,
  payload:exception::string   AS exception,
  ingest_ts
FROM query_progress_raw
WHERE payload:terminated::boolean = true;
