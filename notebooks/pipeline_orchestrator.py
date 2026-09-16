# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Trades Medallion Pipeline — Orchestrator
# MAGIC
# MAGIC **Hybrid pattern: this notebook orchestrates, Python modules do the work.**
# MAGIC
# MAGIC | Layer  | Reads from                                    | Writes to                              |
# MAGIC |--------|-----------------------------------------------|----------------------------------------|
# MAGIC | Bronze | `/Volumes/finmarket/landing/raw_trades/`      | `finmarket.bronze.trades`              |
# MAGIC | Silver | `finmarket.bronze.trades`                     | `finmarket.silver.trades`              |
# MAGIC | Gold   | `finmarket.silver.trades`                     | `finmarket.gold.trade_summary`         |
# MAGIC
# MAGIC **Expected counts (validated in exploration notebook):**
# MAGIC Bronze = 10, Silver = 8 (80%), Quarantine = 2, Gold = 8 (aggregated grain)

# COMMAND ----------

# MAGIC %md ## Cell 1 — Setup

# COMMAND ----------

import sys
import json
import logging
from datetime import datetime
import os

# Detect environment from widget and set correct bundle path
env = dbutils.widgets.get("env")

bundle_path = f"/Workspace/Users/rakesh.singh1004@gmail.com/.bundle/trades-medallion-pipeline/{env}/files/src"
sys.path.insert(0, bundle_path)

print(f"sys.path set to: {bundle_path}")

from pipeline.config import load_config
import pipeline.bronze as bronze_layer
import pipeline.silver as silver_layer
import pipeline.gold   as gold_layer

# Widgets — passed as parameters from the Workflow job
dbutils.widgets.text("env",   "dev", "Environment (dev / prod)")
dbutils.widgets.text("layer", "all", "Layer (bronze / silver / gold / all)")

env   = dbutils.widgets.get("env")
layer = dbutils.widgets.get("layer")

config = load_config(env=env)
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

print(f"{'='*55}")
print(f"Pipeline start")
print(f"  run_id  : {run_id}")
print(f"  env     : {env}")
print(f"  layer   : {layer}")
print(f"  catalog : {config.catalog}")
print(f"  raw     : {config.paths.raw}")
print(f"{'='*55}")

# COMMAND ----------

# MAGIC %md ## Cell 2 — Verify landing volume has data

# COMMAND ----------

import os
files = os.listdir(config.paths.raw)
print(f"Files in landing volume: {files}")

if not files:
    raise FileNotFoundError(
        f"No files found at {config.paths.raw}. "
        f"Upload JSON files to /Volumes/finmarket/landing/raw_trades/ first."
    )

# COMMAND ----------

# MAGIC %md ## Cell 3 — Bronze: raw ingest

# COMMAND ----------

bronze_result = {}

if layer in ("bronze", "all"):
    print(">>> Starting Bronze layer...")
    try:
        bronze_result = bronze_layer.run(spark, config)
        print(f"Bronze complete")
        print(f"  Rows written : {bronze_result['rows_written']:,}")
        print(f"  Table        : {config.tables.bronze}")
    except Exception as e:
        print(f"BRONZE FAILED: {e}")
        dbutils.notebook.exit(f"FAILED at Bronze: {e}")
else:
    print(f"Skipping Bronze (layer={layer})")

# COMMAND ----------

# MAGIC %md ## Cell 4 — Silver: cleanse and validate

# COMMAND ----------

silver_result = {}

if layer in ("silver", "all"):
    print(">>> Starting Silver layer...")
    try:
        silver_result = silver_layer.run(spark, config)
        print(f"Silver complete")
        print(f"  Rows written     : {silver_result['rows_written']:,}")
        print(f"  Rows quarantined : {silver_result['rows_quarantined']:,}")
        print(f"  Table            : {config.tables.silver}")

        if silver_result["rows_quarantined"] > 0:
            quarantine_pct = round(
                silver_result["rows_quarantined"] /
                (silver_result["rows_written"] + silver_result["rows_quarantined"]) * 100, 1
            )
            print(f"  WARNING: {quarantine_pct}% rows quarantined — check finmarket.silver.trades_quarantine")
    except Exception as e:
        print(f"SILVER FAILED: {e}")
        dbutils.notebook.exit(f"FAILED at Silver: {e}")
else:
    print(f"Skipping Silver (layer={layer})")

# COMMAND ----------

# MAGIC %md ## Cell 5 — Gold: aggregate for reporting

# COMMAND ----------

gold_result = {}

if layer in ("gold", "all"):
    print(">>> Starting Gold layer...")
    try:
        gold_result = gold_layer.run(spark, config)
        print(f"Gold complete")
        print(f"  Rows written : {gold_result['rows_written']:,}")
        print(f"  Table        : {config.tables.gold}")
    except Exception as e:
        print(f"GOLD FAILED: {e}")
        dbutils.notebook.exit(f"FAILED at Gold: {e}")
else:
    print(f"Skipping Gold (layer={layer})")

# COMMAND ----------

# MAGIC %md ## Cell 6 — Cross-layer validation

# COMMAND ----------

if layer == "all":
    print(">>> Cross-layer validation...")
    try:
        b = spark.read.table(config.tables.bronze).count()
        s = spark.read.table(config.tables.silver).count()
        g = spark.read.table(config.tables.gold).count()

        silver_pct = round(s / b * 100, 1) if b > 0 else 0

        print(f"{'='*55}")
        print(f"Layer count summary")
        print(f"  Bronze : {b:,}")
        print(f"  Silver : {s:,}  ({silver_pct}% of Bronze)")
        print(f"  Gold   : {g:,}  (aggregated grain)")
        print(f"{'='*55}")

        if silver_pct < 90:
            print(f"WARNING: Silver retention {silver_pct}% — check quarantine table")

    except Exception as e:
        print(f"Validation warning (non-fatal): {e}")

# COMMAND ----------

# MAGIC %md ## Cell 7 — Gold preview

# COMMAND ----------

if layer in ("gold", "all"):
    print("Gold table preview:")
    display(
        spark.table(config.tables.gold)
        .orderBy("trade_date", "trader_id")
    )

# COMMAND ----------

# MAGIC %md ## Cell 8 — Quarantine preview

# COMMAND ----------

quarantine_table = "finmarket.silver.trades_quarantine"

try:
    q_df    = spark.table(quarantine_table)
    q_count = q_df.count()
    if q_count > 0:
        print(f"Quarantine: {q_count} rows — DQ failures:")
        display(q_df.select("trade_id", "trader_id", "quantity", "price", "side", "dq_pass"))
    else:
        print("Quarantine table is empty — all rows passed DQ")
except Exception:
    print("No quarantine table yet — no DQ failures this run")

# COMMAND ----------

# MAGIC %md ## Cell 9 — Exit with audit summary

# COMMAND ----------

audit = {
    "run_id" : run_id,
    "env"    : env,
    "layer"  : layer,
    "bronze" : bronze_result,
    "silver" : silver_result,
    "gold"   : gold_result,
    "status" : "SUCCESS",
}

print(f"Pipeline complete")
print(json.dumps(audit, indent=2))

dbutils.notebook.exit(json.dumps(audit))
