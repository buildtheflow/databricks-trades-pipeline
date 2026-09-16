# Databricks notebook source
# This is a .py notebook file — in Databricks, each cell is separated by # COMMAND ----------
# Run interactively, cell by cell, for exploration and prototyping

# MAGIC %md
# MAGIC # Trades Medallion Pipeline — Exploration Notebook
# MAGIC
# MAGIC Ingests raw trade JSON files and processes through Bronze → Silver → Gold layers.
# MAGIC Use this notebook for:
# MAGIC - Exploring raw data structure
# MAGIC - Prototyping transformations
# MAGIC - Validating data quality rules before promoting to production scripts

# COMMAND ----------

# MAGIC %md ## 0. Setup — imports and config

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
from datetime import datetime

# In Databricks, spark is already available — no need to create a session
# spark = SparkSession.builder.getOrCreate()  # only needed locally

# Config — in prod these come from a config file or Databricks widgets
RAW_PATH      = "dbfs:/mnt/landing/trades/raw/"
BRONZE_PATH   = "dbfs:/mnt/delta/bronze/trades/"
SILVER_PATH   = "dbfs:/mnt/delta/silver/trades/"
GOLD_PATH     = "dbfs:/mnt/delta/gold/trade_summary/"
CATALOG       = "finmarket"
BRONZE_TABLE  = f"{CATALOG}.bronze.raw_trades"
SILVER_TABLE  = f"{CATALOG}.silver.trades"
GOLD_TABLE    = f"{CATALOG}.gold.trade_summary"

print(f"Run started: {datetime.now()}")

# COMMAND ----------

# MAGIC %md ## 1. BRONZE — Raw ingest from JSON files

# COMMAND ----------

# Define schema explicitly — never infer in production (slow + unreliable)
raw_schema = StructType([
    StructField("trade_id",       StringType(),    True),
    StructField("trader_id",      StringType(),    True),
    StructField("instrument_id",  StringType(),    True),
    StructField("trade_date",     StringType(),    True),   # raw as string, parse in Silver
    StructField("settle_date",    StringType(),    True),
    StructField("quantity",       DoubleType(),    True),
    StructField("price",          DoubleType(),    True),
    StructField("currency",       StringType(),    True),
    StructField("side",           StringType(),    True),   # BUY or SELL
    StructField("status",         StringType(),    True),
    StructField("source_system",  StringType(),    True),
])

# Read raw JSON — explore what landed
raw_df = (
    spark.read
    .schema(raw_schema)
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")       # capture bad rows rather than failing
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(RAW_PATH)
)

print(f"Raw record count: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# Explore — check for nulls, unexpected values
display(raw_df.limit(20))

# COMMAND ----------

# Check null rates across key columns
null_summary = raw_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["trade_id", "trader_id", "instrument_id", "quantity", "price", "currency", "side"]
])
display(null_summary)

# COMMAND ----------

# Check distinct values on categorical columns
display(raw_df.groupBy("side").count())
display(raw_df.groupBy("status").count())
display(raw_df.groupBy("currency").count())

# COMMAND ----------

# Add audit columns and write to Bronze Delta table
# Bronze = raw as-is, just add metadata — no transformations

bronze_df = (
    raw_df
    .withColumn("_ingested_at",  F.current_timestamp())
    .withColumn("_source_file",  F.input_file_name())
    .withColumn("_batch_date",   F.current_date())
)

# Write as Delta — append mode, partitioned by batch date
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "false")
    .save(BRONZE_PATH)
)

# Register in Unity Catalog / Hive metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

print(f"Bronze write complete. Total rows: {spark.read.format('delta').load(BRONZE_PATH).count()}")

# COMMAND ----------

# MAGIC %md ## 2. SILVER — Cleanse, validate, type-cast

# COMMAND ----------

# Read from Bronze
bronze_read_df = spark.read.format("delta").load(BRONZE_PATH)

# Explore what needs cleansing
display(bronze_read_df.select("trade_date", "settle_date", "side", "status").limit(10))

# COMMAND ----------

# Apply cleansing transformations
silver_df = (
    bronze_read_df

    # Parse dates from string to proper date type
    .withColumn("trade_date",   F.to_date(F.col("trade_date"),   "yyyy-MM-dd"))
    .withColumn("settle_date",  F.to_date(F.col("settle_date"),  "yyyy-MM-dd"))

    # Standardise categoricals — upper, trim
    .withColumn("side",         F.upper(F.trim(F.col("side"))))
    .withColumn("currency",     F.upper(F.trim(F.col("currency"))))
    .withColumn("status",       F.upper(F.trim(F.col("status"))))
    .withColumn("source_system",F.upper(F.trim(F.col("source_system"))))

    # Derive notional value
    .withColumn("notional_value", F.round(F.col("quantity") * F.col("price"), 2))

    # Signed quantity — negative for SELL
    .withColumn("signed_quantity",
        F.when(F.col("side") == "SELL", F.col("quantity") * -1)
         .otherwise(F.col("quantity"))
    )

    # Data quality flags
    .withColumn("dq_pass",
        F.col("trade_id").isNotNull() &
        F.col("quantity").isNotNull() &
        F.col("price").isNotNull() &
        (F.col("quantity") > 0) &
        (F.col("price") > 0) &
        F.col("side").isin("BUY", "SELL") &
        F.col("trade_date").isNotNull()
    )

    # Keep audit trail from Bronze
    .withColumn("_silver_processed_at", F.current_timestamp())

    # Drop corrupt records
    .filter(F.col("_corrupt_record").isNull())
    .drop("_corrupt_record")
)

print(f"Silver total: {silver_df.count()}")
print(f"DQ pass: {silver_df.filter(F.col('dq_pass') == True).count()}")
print(f"DQ fail: {silver_df.filter(F.col('dq_pass') == False).count()}")

# COMMAND ----------

# Explore DQ failures before writing — understand what failed and why
display(silver_df.filter(F.col("dq_pass") == False))

# COMMAND ----------

# Write Silver — only DQ-passing records to main table
# DQ failures go to a quarantine path for investigation
silver_pass_df = silver_df.filter(F.col("dq_pass") == True)
silver_fail_df = silver_df.filter(F.col("dq_pass") == False)

(
    silver_pass_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .partitionBy("trade_date")
    .save(SILVER_PATH)
)

# Write quarantine
if silver_fail_df.count() > 0:
    (
        silver_fail_df.write
        .format("delta")
        .mode("append")
        .save(SILVER_PATH.replace("/trades/", "/trades_quarantine/"))
    )
    print(f"WARNING: {silver_fail_df.count()} records quarantined")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print("Silver write complete")

# COMMAND ----------

# MAGIC %md ## 3. GOLD — Aggregate for reporting

# COMMAND ----------

# Read Silver
silver_read_df = spark.read.format("delta").load(SILVER_PATH)

# Explore before aggregating — understand the grain
display(silver_read_df.groupBy("trade_date", "currency").count().orderBy("trade_date"))

# COMMAND ----------

# Build Gold — daily trade summary by trader, instrument, currency
gold_df = (
    silver_read_df
    .groupBy("trade_date", "trader_id", "instrument_id", "currency", "side")
    .agg(
        F.count("trade_id")                   .alias("trade_count"),
        F.sum("quantity")                     .alias("total_quantity"),
        F.sum("notional_value")               .alias("total_notional"),
        F.avg("price")                        .alias("avg_price"),
        F.min("price")                        .alias("min_price"),
        F.max("price")                        .alias("max_price"),
        F.countDistinct("instrument_id")      .alias("distinct_instruments"),
    )
    .withColumn("_gold_processed_at", F.current_timestamp())
)

display(gold_df.orderBy("trade_date", "trader_id"))

# COMMAND ----------

# Write Gold
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")
    .partitionBy("trade_date")
    .save(GOLD_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
    USING DELTA
    LOCATION '{GOLD_PATH}'
""")

# Final counts
print(f"Gold rows written: {gold_df.count()}")
print(f"Run complete: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quick validation — compare row counts across layers
# MAGIC Run this after every pipeline run to catch any layer-level drops

# COMMAND ----------

bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
silver_count = spark.read.format("delta").load(SILVER_PATH).count()
gold_count   = spark.read.format("delta").load(GOLD_PATH).count()

print(f"Bronze : {bronze_count:,}")
print(f"Silver : {silver_count:,}  ({round(silver_count/bronze_count*100,1)}% of Bronze)")
print(f"Gold   : {gold_count:,}  (aggregated grain)")
