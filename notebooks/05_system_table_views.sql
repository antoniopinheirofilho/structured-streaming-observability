-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Cluster-metrics layer — views on system tables (§4 / §6)
-- MAGIC
-- MAGIC Stream-level metrics say the query is slow; **cluster metrics say whether the
-- MAGIC box is the cause.** These views sit on the Databricks system tables and
-- MAGIC complete the data model:
-- MAGIC
-- MAGIC - `cluster_node_minute` — per-node CPU/memory/swap (+ Spark JVM heap), from
-- MAGIC   `system.compute.node_timeline`.
-- MAGIC - `dim_job_cluster` — job→run→cluster mapping, from `system.lakeflow`
-- MAGIC   (resolves real **Jobs**; all-purpose clusters are not represented here).
-- MAGIC - `stream_cluster_correlation` — convenience join of batch progress to node
-- MAGIC   minutes, so §4 correlation can be reviewed against real node data.
-- MAGIC
-- MAGIC > `node_timeline` lands within ~1h and has **90-day** retention (vs 365d for
-- MAGIC > `lakeflow`). Snapshot into a managed table if you need longer history.

-- COMMAND ----------

USE CATALOG observability;
USE SCHEMA streaming;

-- COMMAND ----------
-- MAGIC %md ## cluster_node_minute — per-node resource metrics

-- COMMAND ----------

CREATE OR REPLACE VIEW cluster_node_minute (
  workspace_id        COMMENT 'Databricks workspace id the node belongs to.',
  cluster_id          COMMENT 'Compute cluster id. Join key to dim_job_cluster (job clusters) or to a known cluster (all-purpose).',
  instance_id         COMMENT 'Cloud instance id of the individual node.',
  node_type           COMMENT 'Instance type of the node (e.g. rd-fleet.xlarge).',
  is_driver           COMMENT 'True for the driver node, false for workers. Driver pressure and worker pressure have different remedies (§4).',
  start_time          COMMENT 'Start of the one-minute measurement window (UTC).',
  end_time            COMMENT 'End of the one-minute measurement window (UTC).',
  cpu_busy_pct        COMMENT 'CPU busy percent = cpu_user_percent + cpu_system_percent. Sustained ~100% on workers => compute bound; on driver => driver bound.',
  cpu_io_wait_pct     COMMENT 'CPU iowait percent. Elevated => shuffle spill / checkpoint I/O / slow object store.',
  mem_used_pct        COMMENT 'Memory used percent of the node. Includes background processes, NOT just the Spark JVM heap. Driver sustained > 80-85% => OOM risk.',
  mem_swap_pct        COMMENT 'Swap used percent. Any sustained > 0 => swapping => GC pauses => batch-duration spikes => backlog.',
  jvm_heap_used_bytes COMMENT 'Spark JVM heap bytes in use (true heap, unlike mem_used_pct). Pair with heap limit to see real heap pressure.',
  jvm_heap_limit_bytes COMMENT 'Spark JVM heap limit in bytes.'
)
COMMENT 'Per-node, per-minute compute metrics for driver/worker health during streaming. One row per node per minute. Source: system.compute.node_timeline (~1h latency, 90-day retention). Join to stream_batch_progress by cluster + time-overlap to explain batch-duration spikes (§4).'
AS
SELECT
  workspace_id,
  cluster_id,
  instance_id,
  node_type,
  driver                                                 AS is_driver,
  start_time,
  end_time,
  ROUND(cpu_user_percent + cpu_system_percent, 2)        AS cpu_busy_pct,
  ROUND(cpu_wait_percent, 2)                             AS cpu_io_wait_pct,
  ROUND(mem_used_percent, 2)                             AS mem_used_pct,
  ROUND(mem_swap_percent, 2)                             AS mem_swap_pct,
  spark_jvm_heap_usage_bytes                             AS jvm_heap_used_bytes,
  spark_jvm_heap_limit_bytes                             AS jvm_heap_limit_bytes
FROM system.compute.node_timeline
WHERE start_time > current_timestamp() - INTERVAL 7 DAYS;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## dim_job_cluster — job→run→cluster mapping
-- MAGIC Job clusters get a new cluster_id on every restart, so resolve via the job.
-- MAGIC `compute_ids` is populated from Aug 2024 onward.

-- COMMAND ----------

CREATE OR REPLACE VIEW dim_job_cluster (
  workspace_id      COMMENT 'Workspace id of the job.',
  job_id            COMMENT 'Job id.',
  job_name          COMMENT 'Current job name (latest change) — filter by this to find a stream job cluster at query time.',
  job_run_id        COMMENT 'Specific job run id.',
  task_key          COMMENT 'Task within the run.',
  cluster_id        COMMENT 'Compute cluster id used by the task. Join key to cluster_node_minute.',
  period_start_time COMMENT 'Start of the task-run time slice (UTC). Use for time-overlap join to node minutes.',
  period_end_time   COMMENT 'End of the task-run time slice (UTC).',
  result_state      COMMENT 'Task run result state (e.g. SUCCESS, FAILED).'
)
COMMENT 'Dimension mapping Databricks Jobs to the compute clusters that ran them, over the last 30 days. Resolves a stream job to its (restart-churned) cluster_id by job name. Source: system.lakeflow.jobs + job_task_run_timeline. Only real Jobs appear here — all-purpose clusters are not represented. Join to cluster_node_minute on cluster_id + time overlap (§4/§6).'
AS
WITH latest_job AS (
  SELECT workspace_id, job_id, name AS job_name
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
)
SELECT
  t.workspace_id,
  t.job_id,
  j.job_name,
  t.job_run_id,
  t.task_key,
  EXPLODE(t.compute_ids) AS cluster_id,
  t.period_start_time,
  t.period_end_time,
  t.result_state
FROM system.lakeflow.job_task_run_timeline t
JOIN latest_job j USING (workspace_id, job_id)
WHERE array_size(t.compute_ids) > 0
  AND t.period_end_time > current_timestamp() - INTERVAL 30 DAYS;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## stream_cluster_correlation — batch spikes vs node pressure
-- MAGIC Correlates each micro-batch with the driver/worker node metrics of the
-- MAGIC cluster that ran it. The cluster is resolved **dynamically by job name**
-- MAGIC through `dim_job_cluster` + time overlap — never a hard-coded `cluster_id`,
-- MAGIC which churns on every job restart (§4). Filter by `job_name` when querying.

-- COMMAND ----------

CREATE OR REPLACE VIEW stream_cluster_correlation (
  job_name            COMMENT 'Name of the Databricks Job whose cluster ran this batch, resolved via dim_job_cluster. Filter on this.',
  query_name          COMMENT 'Streaming query name (e.g. enrich_trades).',
  batch_id            COMMENT 'Micro-batch id.',
  batch_timestamp     COMMENT 'Batch finalize time (UTC).',
  batch_duration_ms   COMMENT 'Batch execution time in ms. Spikes here are what you correlate against node pressure.',
  max_offsets_behind  COMMENT 'Kafka worst-partition lag at this batch.',
  is_driver           COMMENT 'Whether the correlated node metrics are for the driver (true) or a worker (false).',
  cpu_busy_pct        COMMENT 'Node CPU busy percent during the minute covering this batch.',
  mem_used_pct        COMMENT 'Node memory used percent during the minute covering this batch.',
  mem_swap_pct        COMMENT 'Node swap percent during the minute covering this batch.'
)
COMMENT 'Correlation fact (§4): joins each streaming micro-batch (stream_batch_progress) to the driver/worker node metrics (cluster_node_minute) of the cluster that ran it, resolving the cluster DYNAMICALLY by job name via dim_job_cluster + time overlap — no hard-coded cluster_id. Filter by job_name. Lets you see whether a batch-duration spike lines up with driver/worker CPU or memory pressure. Only streams run as Databricks Jobs are represented (all-purpose-cluster runs do not appear in dim_job_cluster).'
AS
SELECT
  d.job_name,
  b.query_name,
  b.batch_id,
  b.batch_timestamp,
  b.batch_duration_ms,
  b.max_offsets_behind,
  n.is_driver,
  n.cpu_busy_pct,
  n.mem_used_pct,
  n.mem_swap_pct
FROM stream_batch_progress b
JOIN dim_job_cluster d
  ON  b.batch_timestamp >= d.period_start_time
  AND b.batch_timestamp <  d.period_end_time
JOIN cluster_node_minute n
  ON  n.cluster_id = d.cluster_id            -- cluster resolved via dim_job_cluster, not hard-coded
  AND b.batch_timestamp >= n.start_time
  AND b.batch_timestamp <  n.end_time;
