"""
bronze.py
Bronze layer — raw ingest from Unity Catalog Volume into Delta table.
Audit columns only — no transformations.
Uses MERGE on trade_id — idempotent, safe to rerun.

Validated against working exploration notebook (2026-09-14).
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
from pipeline.config import PipelineConfig
from pipeline.schema import RAW_SCHEMA

logger = logging.getLogger(__name__)


def read_raw(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read raw JSON files from landing Volume."""
    logger.info(f"Reading raw files from: {config.paths.raw}")

    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(config.paths.raw)
    )

    count = df.count()
    logger.info(f"Raw records read: {count:,}")

    if count == 0:
        raise ValueError(
            f"No records found at {config.paths.raw}. "
            f"Check that JSON files exist in the Volume."
        )

    return df


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add Bronze audit metadata.
    _metadata.file_path is the correct way to capture source file
    in Unity Catalog Volumes (input_file_name() does not work reliably).
    Adds _corrupt_record column if not already present (PERMISSIVE mode
    only adds it when corrupt rows exist).
    """
    df = (
        df
        .withColumn("_inserted_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_date",  F.current_date())
    )

    # Ensure _corrupt_record exists for consistent schema on saveAsTable
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast(StringType()))

    return df


def write_bronze(df: DataFrame, config: PipelineConfig, spark: SparkSession) -> int:
    """
    Write to Bronze Delta table using MERGE on trade_id.
    Idempotent — reruns never create duplicates.
    First run creates the table; subsequent runs merge new trade_ids only.
    """
    logger.info(f"Writing Bronze to: {config.tables.bronze}")

    if spark.catalog.tableExists(config.tables.bronze):
        logger.info("Table exists — running MERGE on trade_id")
        delta_table = DeltaTable.forName(spark, config.tables.bronze)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), "target.trade_id = source.trade_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        logger.info("First run — creating Bronze table")
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("_batch_date")
            .option("mergeSchema", "true")
            .saveAsTable(config.tables.bronze)
        )

    count = spark.table(config.tables.bronze).count()
    logger.info(f"Bronze table total rows: {count:,}")
    return count


def run(spark: SparkSession, config: PipelineConfig) -> dict:
    """Run the full Bronze layer. Returns audit dict."""
    logger.info("=== BRONZE LAYER START ===")
    raw_df    = read_raw(spark, config)
    bronze_df = add_audit_columns(raw_df)
    count     = write_bronze(bronze_df, config, spark)
    logger.info("=== BRONZE LAYER COMPLETE ===")
    return {"layer": "bronze", "rows_written": count}
