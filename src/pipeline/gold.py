"""
gold.py
Gold layer — aggregate Silver into reporting-ready summary table.
Grain: trade_date / trader_id / instrument_id / currency / side.

Validated against working exploration notebook (2026-09-14).
Gold uses overwrite mode — full rebuild each run from Silver.
Final validation confirmed: Bronze=10, Silver=9 (90%), Gold=9.
"""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pipeline.config import PipelineConfig

logger = logging.getLogger(__name__)


def read_silver(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read from Silver Delta table in Unity Catalog."""
    logger.info(f"Reading Silver from: {config.tables.silver}")
    return spark.table(config.tables.silver)


def aggregate(df: DataFrame) -> DataFrame:
    """
    Build Gold aggregation — daily summary by trader, instrument, currency, side.
    Matches groupBy and agg confirmed in exploration notebook.
    """
    return (
        df
        .groupBy("trade_date", "trader_id", "instrument_id", "currency", "side")
        .agg(
            F.count("trade_id")              .alias("trade_count"),
            F.sum("quantity")                .alias("total_quantity"),
            F.sum("notional_value")          .alias("total_notional"),
            F.avg("price")                   .alias("avg_price"),
            F.min("price")                   .alias("min_price"),
            F.max("price")                   .alias("max_price"),
            F.countDistinct("instrument_id") .alias("distinct_instruments"),
        )
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def write_gold(df: DataFrame, config: PipelineConfig) -> int:
    """Write to Gold Delta table. Overwrite each run. Returns row count."""
    count = df.count()
    logger.info(f"Writing {count:,} rows to: {config.tables.gold}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .partitionBy("trade_date")
        .saveAsTable(config.tables.gold)
    )

    logger.info(f"Gold write complete: {count:,} rows")
    return count


def run(spark: SparkSession, config: PipelineConfig) -> dict:
    """Run the full Gold layer. Returns audit dict."""
    logger.info("=== GOLD LAYER START ===")
    silver_df = read_silver(spark, config)
    gold_df   = aggregate(silver_df)
    count     = write_gold(gold_df, config)
    logger.info("=== GOLD LAYER COMPLETE ===")
    return {"layer": "gold", "rows_written": count}
