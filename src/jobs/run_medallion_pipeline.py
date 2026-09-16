"""
run_medallion_pipeline.py
Production entry point — called by Databricks Workflow or Airflow.

Usage:
    spark-submit run_medallion_pipeline.py --env prod
    spark-submit run_medallion_pipeline.py --env dev --layer silver

Databricks Workflow task params:
    ["--env", "prod"]
    or for a single layer:
    ["--env", "prod", "--layer", "bronze"]
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

# Add src to path (not needed if installed as a wheel)
sys.path.insert(0, "/dbfs/FileStore/pipeline_src")

from pipeline.config import load_config
import pipeline.bronze as bronze_layer
import pipeline.silver as silver_layer
import pipeline.gold   as gold_layer

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("medallion_pipeline")


def parse_args():
    parser = argparse.ArgumentParser(description="Trades Medallion Pipeline")
    parser.add_argument("--env",   required=True, choices=["dev", "prod"], help="Environment")
    parser.add_argument("--layer", required=False, choices=["bronze", "silver", "gold", "all"],
                        default="all", help="Which layer to run (default: all)")
    return parser.parse_args()


def validate_layer_counts(spark, config) -> None:
    """Cross-layer row count validation — log warnings if counts look wrong."""
    try:
        b = spark.read.format("delta").load(config.paths.bronze).count()
        s = spark.read.format("delta").load(config.paths.silver).count()
        g = spark.read.format("delta").load(config.paths.gold).count()

        silver_pct = round(s / b * 100, 1) if b > 0 else 0

        logger.info("=== LAYER COUNT SUMMARY ===")
        logger.info(f"  Bronze : {b:,}")
        logger.info(f"  Silver : {s:,}  ({silver_pct}% of Bronze)")
        logger.info(f"  Gold   : {g:,}  (aggregated grain)")

        if silver_pct < 90:
            logger.warning(f"Silver retention below 90% — check quarantine table")
    except Exception as e:
        logger.warning(f"Count validation skipped: {e}")


def main():
    args   = parse_args()
    config = load_config(env=args.env)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(f"Pipeline start | run_id={run_id} | env={args.env} | layer={args.layer}")

    spark = (
        SparkSession.builder
        .appName(f"trades_medallion_{args.env}_{run_id}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    audit = {"run_id": run_id, "env": args.env, "layers": []}

    try:
        if args.layer in ("bronze", "all"):
            result = bronze_layer.run(spark, config)
            audit["layers"].append(result)

        if args.layer in ("silver", "all"):
            result = silver_layer.run(spark, config)
            audit["layers"].append(result)

        if args.layer in ("gold", "all"):
            result = gold_layer.run(spark, config)
            audit["layers"].append(result)

        if args.layer == "all":
            validate_layer_counts(spark, config)

        logger.info(f"Pipeline complete | run_id={run_id} | audit={audit}")

    except Exception as e:
        logger.error(f"Pipeline FAILED | run_id={run_id} | error={e}", exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
