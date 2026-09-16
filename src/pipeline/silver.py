"""
silver.py
Silver layer — cleanse, validate, type-cast Bronze data.
DQ failures written to quarantine table, not silently dropped.

Validated against working exploration notebook (2026-09-14).
Key decisions from notebook:
- Silver uses overwrite mode (full reload each run)
- Quarantine uses append mode (preserve history of failures)
- DQ flag checks: trade_id, trader_id, instrument_id, quantity, price, side, trade_date
- Expected counts: Bronze=10, Silver=8, Quarantine=2 (T-009 null trader, T-010 negative qty)
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pipeline.config import PipelineConfig

logger = logging.getLogger(__name__)


def read_bronze(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read from Bronze Delta table in Unity Catalog."""
    logger.info(f"Reading Bronze from: {config.tables.bronze}")
    return spark.table(config.tables.bronze)


def cleanse(df: DataFrame) -> DataFrame:
    """
    Apply type casting and standardisation transformations.
    Matches exactly what was validated in exploration notebook.
    _corrupt_record check is guarded — only present when reading
    JSON with PERMISSIVE mode in production, not in unit tests.
    """
    df = (
        df
        # Parse dates — format confirmed in notebook: yyyy-MM-dd
        .withColumn("trade_date",    F.to_date(F.col("trade_date"),  "yyyy-MM-dd"))
        .withColumn("settle_date",   F.to_date(F.col("settle_date"), "yyyy-MM-dd"))

        # Standardise categoricals
        .withColumn("side",          F.upper(F.trim(F.col("side"))))
        .withColumn("currency",      F.upper(F.trim(F.col("currency"))))
        .withColumn("status",        F.upper(F.trim(F.col("status"))))
        .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))

        # Derived columns
        .withColumn("notional_value",
            F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn("signed_quantity",
            F.when(F.col("side") == "SELL", -1 * F.col("quantity"))
             .otherwise(F.col("quantity")))

        # Silver audit column
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    # Drop corrupt records from Bronze permissive read
    # Guarded — _corrupt_record only exists when reading JSON with PERMISSIVE mode
    if "_corrupt_record" in df.columns:
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    return df


def apply_dq_flags(df: DataFrame) -> DataFrame:
    """
    Tag each row with dq_pass flag and dq_fail_reason.
    A trade must have a valid trader_id — null trader_id is not
    a valid trade in any financial system and goes to quarantine.
    Expected: Bronze=10, Silver=8, Quarantine=2 (T-009, T-010).
    """
    return (
        df
        .withColumn("dq_pass",
            F.col("trade_id").isNotNull() &
            F.col("trader_id").isNotNull() &      # T-009 null trader_id → quarantine
            F.col("instrument_id").isNotNull() &
            F.col("quantity").isNotNull() &
            F.col("price").isNotNull() &
            (F.col("quantity") > 0) &             # T-010 negative quantity → quarantine
            (F.col("price") > 0) &
            F.col("side").isin("BUY", "SELL") &
            F.col("trade_date").isNotNull()
        )
        .withColumn("dq_fail_reason",
            F.when(F.col("trade_id").isNull(),          F.lit("NULL trade_id"))
             .when(F.col("trader_id").isNull(),         F.lit("NULL trader_id"))
             .when(F.col("instrument_id").isNull(),     F.lit("NULL instrument_id"))
             .when(F.col("quantity").isNull(),          F.lit("NULL quantity"))
             .when(F.col("price").isNull(),             F.lit("NULL price"))
             .when(F.col("quantity") <= 0,              F.lit("quantity <= 0"))
             .when(F.col("price") <= 0,                 F.lit("price <= 0"))
             .when(~F.col("side").isin("BUY", "SELL"),  F.lit("invalid side"))
             .when(F.col("trade_date").isNull(),         F.lit("NULL trade_date"))
             .otherwise(F.lit(None))
        )
    )


def check_dq_threshold(df: DataFrame, config: PipelineConfig) -> None:
    """Raise if quarantine rate exceeds configured threshold. Prod only."""
    total      = df.count()
    fail_count = df.filter(F.col("dq_pass") == False).count()

    if total == 0:
        raise ValueError("Silver: zero records after cleansing.")

    fail_pct = (fail_count / total) * 100
    logger.warning(f"DQ: {fail_count:,} of {total:,} rows failed ({fail_pct:.1f}%)")

    if config.dq.fail_on_quarantine and fail_pct > config.dq.max_quarantine_pct:
        raise RuntimeError(
            f"DQ threshold breached: {fail_pct:.1f}% failures "
            f"exceeds limit of {config.dq.max_quarantine_pct}%"
        )


def write_silver(df: DataFrame, config: PipelineConfig) -> dict:
    """
    Split pass/fail. Write passing rows to Silver (overwrite),
    failures to quarantine (append — preserve history).
    """
    silver_pass_df = df.filter(F.col("dq_pass") == True)
    silver_fail_df = df.filter(F.col("dq_pass") == False)

    pass_count = silver_pass_df.count()
    fail_count = silver_fail_df.count()

    # Silver — overwrite each run (full reload)
    logger.info(f"Writing {pass_count:,} rows to: {config.tables.silver}")
    (
        silver_pass_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("trade_date")
        .saveAsTable(config.tables.silver)
    )

    # Quarantine — append to preserve failure history
    if fail_count > 0:
        quarantine = config.tables.silver.replace(".trades", ".trades_quarantine")
        logger.warning(f"Writing {fail_count:,} rows to quarantine: {quarantine}")
        (
            silver_fail_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(quarantine)
        )

    logger.info("Silver write complete")
    return {"layer": "silver", "rows_written": pass_count, "rows_quarantined": fail_count}


def run(spark: SparkSession, config: PipelineConfig) -> dict:
    """Run the full Silver layer. Returns audit dict."""
    logger.info("=== SILVER LAYER START ===")
    bronze_df = read_bronze(spark, config)
    cleansed  = cleanse(bronze_df)
    flagged   = apply_dq_flags(cleansed)
    check_dq_threshold(flagged, config)
    result    = write_silver(flagged, config)
    logger.info("=== SILVER LAYER COMPLETE ===")
    return result
