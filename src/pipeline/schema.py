"""
schema.py
Centralised schema definitions — never infer schema in production.
Matches exactly what was validated in the exploration notebook.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

RAW_SCHEMA = StructType([
    StructField("trade_id",       StringType(), True),
    StructField("trader_id",      StringType(), True),
    StructField("instrument_id",  StringType(), True),
    StructField("trade_date",     StringType(), True),  # raw string — parsed to date in Silver
    StructField("settle_date",    StringType(), True),
    StructField("quantity",       DoubleType(), True),
    StructField("price",          DoubleType(), True),
    StructField("currency",       StringType(), True),
    StructField("side",           StringType(), True),  # BUY or SELL
    StructField("status",         StringType(), True),
    StructField("source_system",  StringType(), True),
])
