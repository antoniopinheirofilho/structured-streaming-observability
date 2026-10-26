-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Table & column comments for the managed tables
-- MAGIC
-- MAGIC Applies table- and column-level `COMMENT`s to the physical tables so AI/BI
-- MAGIC Genie and Catalog Explorer understand them. Views carry their comments in
-- MAGIC their own DDL (05, 07); this covers the managed Delta tables.

-- COMMAND ----------

USE CATALOG observability;

-- COMMAND ----------
-- MAGIC %md ## streaming.query_progress_raw (VARIANT ingest layer)

-- COMMAND ----------

COMMENT ON TABLE streaming.query_progress_raw IS
  'Raw ingest layer for streaming observability: one row per Spark StreamingQueryListener progress event, stored as a VARIANT payload so the table never needs a schema migration when Spark changes the progress JSON. Parse typed facts in the views on top (stream_batch_progress, stream_lifecycle_events). Written by an async background Delta writer fed from a bounded in-memory queue.';
ALTER TABLE streaming.query_progress_raw ALTER COLUMN payload COMMENT
  'Full StreamingQueryProgress JSON for one micro-batch (or a terminated marker), as VARIANT. Access nested fields with the : accessor, e.g. payload:sources[0]:metrics:maxOffsetsBehindLatest.';
ALTER TABLE streaming.query_progress_raw ALTER COLUMN ingest_ts COMMENT
  'Timestamp the async writer appended this record to Delta (not the batch time; see payload:timestamp for batch time).';

-- COMMAND ----------
-- MAGIC %md ## trading.instrument (static dimension)

-- COMMAND ----------

COMMENT ON TABLE trading.instrument IS
  'Static instrument reference dimension (synthetic). Joined to the trades stream on symbol to enrich each trade with sector, exchange, tick size, and average daily volume.';
ALTER TABLE trading.instrument ALTER COLUMN symbol COMMENT 'Ticker symbol; primary key and join key to the trades stream.';
ALTER TABLE trading.instrument ALTER COLUMN name COMMENT 'Company / instrument display name.';
ALTER TABLE trading.instrument ALTER COLUMN sector COMMENT 'GICS-style sector classification.';
ALTER TABLE trading.instrument ALTER COLUMN exchange COMMENT 'Primary listing exchange (e.g. NASDAQ, NYSE).';
ALTER TABLE trading.instrument ALTER COLUMN tick_size COMMENT 'Minimum price increment for the instrument.';
ALTER TABLE trading.instrument ALTER COLUMN adv COMMENT 'Average daily volume (shares), synthetic reference value.';

-- COMMAND ----------
-- MAGIC %md ## trading.account (static dimension)

-- COMMAND ----------

COMMENT ON TABLE trading.account IS
  'Static trading-account reference dimension (synthetic). Joined to the trades stream on account_id to enrich each trade with desk, region, and tier.';
ALTER TABLE trading.account ALTER COLUMN account_id COMMENT 'Account identifier; primary key and join key to the trades stream.';
ALTER TABLE trading.account ALTER COLUMN desk COMMENT 'Trading desk (e.g. EQ-CASH, MARKET-MAKING, PROP).';
ALTER TABLE trading.account ALTER COLUMN region COMMENT 'Booking region (AMER, EMEA, APAC).';
ALTER TABLE trading.account ALTER COLUMN tier COMMENT 'Account service tier (GOLD, SILVER, BRONZE).';

-- COMMAND ----------
-- MAGIC %md ## trading.enriched_trades (streaming sink)

-- COMMAND ----------

COMMENT ON TABLE trading.enriched_trades IS
  'Streaming sink: trades read from the Kafka trades topic, parsed, filtered of malformed records, enriched via stream-static joins to instrument and account, with a derived notional. One row per valid trade. Written by the enrich_trades Structured Streaming query. ingest_ts supports end-to-end output-freshness measurement (§3).';
ALTER TABLE trading.enriched_trades ALTER COLUMN trade_id COMMENT 'Monotonic trade identifier from the source event.';
ALTER TABLE trading.enriched_trades ALTER COLUMN ts COMMENT 'Event time of the trade (from the producer).';
ALTER TABLE trading.enriched_trades ALTER COLUMN symbol COMMENT 'Ticker symbol; join key to instrument.';
ALTER TABLE trading.enriched_trades ALTER COLUMN account_id COMMENT 'Account identifier; join key to account.';
ALTER TABLE trading.enriched_trades ALTER COLUMN side COMMENT 'Trade side: BUY or SELL.';
ALTER TABLE trading.enriched_trades ALTER COLUMN price COMMENT 'Executed price per unit (always > 0 here; malformed negatives are filtered upstream).';
ALTER TABLE trading.enriched_trades ALTER COLUMN quantity COMMENT 'Executed quantity (units).';
ALTER TABLE trading.enriched_trades ALTER COLUMN name COMMENT 'Instrument name (enriched from instrument).';
ALTER TABLE trading.enriched_trades ALTER COLUMN sector COMMENT 'Instrument sector (enriched from instrument).';
ALTER TABLE trading.enriched_trades ALTER COLUMN exchange COMMENT 'Listing exchange (enriched from instrument).';
ALTER TABLE trading.enriched_trades ALTER COLUMN tick_size COMMENT 'Instrument tick size (enriched from instrument).';
ALTER TABLE trading.enriched_trades ALTER COLUMN adv COMMENT 'Average daily volume (enriched from instrument).';
ALTER TABLE trading.enriched_trades ALTER COLUMN desk COMMENT 'Trading desk (enriched from account).';
ALTER TABLE trading.enriched_trades ALTER COLUMN region COMMENT 'Booking region (enriched from account).';
ALTER TABLE trading.enriched_trades ALTER COLUMN tier COMMENT 'Account tier (enriched from account).';
ALTER TABLE trading.enriched_trades ALTER COLUMN notional COMMENT 'Derived trade notional = price * quantity.';
ALTER TABLE trading.enriched_trades ALTER COLUMN ingest_ts COMMENT 'Sink write time; current_timestamp() at the batch. Used for output-freshness (age of latest committed output).';
