# Databricks notebook source
# MAGIC %md
# MAGIC # Static dimension tables (trading theme)
# MAGIC
# MAGIC Two small, static Delta dimensions the streaming pipeline joins/enriches
# MAGIC against (§ "joins and transformation with static datasets"):
# MAGIC - `instrument` — symbol reference data (sector, exchange, tick size, ADV).
# MAGIC - `account`    — trading account reference data (desk, region, tier).
# MAGIC
# MAGIC Generic synthetic data — no customer-specific content.

# COMMAND ----------

CATALOG = "observability"   # change to your Unity Catalog
SCHEMA = "trading"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# --- instrument dimension -------------------------------------------------
from pyspark.sql import functions as F

symbols = [
    # symbol, name, sector, exchange, tick_size, adv (avg daily volume)
    ("AAPL", "Apple Inc.",        "Technology",   "NASDAQ", 0.01, 55_000_000),
    ("MSFT", "Microsoft Corp.",   "Technology",   "NASDAQ", 0.01, 25_000_000),
    ("AMZN", "Amazon.com Inc.",   "Consumer",     "NASDAQ", 0.01, 40_000_000),
    ("JPM",  "JPMorgan Chase",    "Financials",   "NYSE",   0.01, 10_000_000),
    ("XOM",  "Exxon Mobil",       "Energy",       "NYSE",   0.01, 18_000_000),
    ("TSLA", "Tesla Inc.",        "Consumer",     "NASDAQ", 0.01, 95_000_000),
    ("NVDA", "NVIDIA Corp.",      "Technology",   "NASDAQ", 0.01, 45_000_000),
    ("BRK.B","Berkshire Hath. B", "Financials",   "NYSE",   0.01, 4_000_000),
    ("UNH",  "UnitedHealth Grp.", "Health Care",  "NYSE",   0.01, 3_000_000),
    ("GE",   "GE Aerospace",      "Industrials",  "NYSE",   0.01, 5_000_000),
]
instrument = spark.createDataFrame(
    symbols, "symbol string, name string, sector string, exchange string, tick_size double, adv long"
)
instrument.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("instrument")
print("instrument rows:", spark.table("instrument").count())

# COMMAND ----------

# --- account dimension ----------------------------------------------------
accounts = [(f"ACC{i:04d}",
             ["EQ-CASH", "EQ-DERIV", "MARKET-MAKING", "PROP"][i % 4],
             ["AMER", "EMEA", "APAC"][i % 3],
             ["GOLD", "SILVER", "BRONZE"][i % 3]) for i in range(1, 201)]
account = spark.createDataFrame(accounts, "account_id string, desk string, region string, tier string")
account.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("account")
print("account rows:", spark.table("account").count())

# COMMAND ----------

display(spark.table("instrument"))
