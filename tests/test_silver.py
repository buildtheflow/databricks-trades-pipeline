"""
test_silver.py
Unit tests for Silver layer transformations.
Run locally with: pytest tests/test_silver.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import date

import sys
sys.path.insert(0, "src")

from pipeline.silver import cleanse, apply_dq_flags


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_data(spark):
    """Minimal Bronze-style rows for testing."""
    data = [
        ("T001", "TR1", "AAPL", "2024-01-15", "2024-01-17", 100.0, 185.50, "usd", "buy",  "SETTLED", "SYSTEM_A"),
        ("T002", "TR2", "MSFT", "2024-01-15", "2024-01-17", 200.0, 410.00, "USD", "SELL", "SETTLED", "SYSTEM_A"),
        ("T003", None,  "TSLA", "2024-01-15", "2024-01-17",  50.0, 220.00, "GBP", "BUY",  "PENDING", "SYSTEM_B"),
        ("T004", "TR4", "NVDA", "2024-01-15", "2024-01-17",  -5.0, 650.00, "USD", "BUY",  "SETTLED", "SYSTEM_A"),
        (None,   "TR5", "AMZN", "2024-01-15", "2024-01-17",  75.0, 175.00, "USD", "BUY",  "SETTLED", "SYSTEM_B"),
    ]
    schema = StructType([
        StructField("trade_id",       StringType(), True),
        StructField("trader_id",      StringType(), True),
        StructField("instrument_id",  StringType(), True),
        StructField("trade_date",     StringType(), True),
        StructField("settle_date",    StringType(), True),
        StructField("quantity",       DoubleType(), True),
        StructField("price",          DoubleType(), True),
        StructField("currency",       StringType(), True),
        StructField("side",           StringType(), True),
        StructField("status",         StringType(), True),
        StructField("source_system",  StringType(), True),
    ])
    return spark.createDataFrame(data, schema)


def test_cleanse_uppercases_side(sample_data):
    result = cleanse(sample_data)
    sides = [r.side for r in result.select("side").collect()]
    assert all(s == s.upper() for s in sides), "side should be uppercased"


def test_cleanse_uppercases_currency(sample_data):
    result = cleanse(sample_data)
    currencies = [r.currency for r in result.select("currency").collect()]
    assert all(c == c.upper() for c in currencies), "currency should be uppercased"


def test_cleanse_parses_trade_date(sample_data):
    result = cleanse(sample_data)
    dates = [r.trade_date for r in result.select("trade_date").collect()]
    assert all(isinstance(d, date) for d in dates if d is not None), "trade_date should be date type"


def test_cleanse_derives_notional(sample_data):
    result = cleanse(sample_data).filter(F.col("trade_id") == "T001")
    row = result.collect()[0]
    assert row.notional_value == round(100.0 * 185.50, 2)


def test_cleanse_signed_quantity_sell(sample_data):
    result = cleanse(sample_data).filter(F.col("trade_id") == "T002")
    row = result.collect()[0]
    assert row.signed_quantity < 0, "SELL should have negative signed_quantity"


def test_dq_flags_null_trader_fails(sample_data):
    cleansed = cleanse(sample_data)
    flagged  = apply_dq_flags(cleansed)
    row = flagged.filter(F.col("trade_id") == "T003").collect()[0]
    assert row.dq_pass == False
    assert row.dq_fail_reason == "NULL trader_id"


def test_dq_flags_negative_quantity_fails(sample_data):
    cleansed = cleanse(sample_data)
    flagged  = apply_dq_flags(cleansed)
    row = flagged.filter(F.col("trade_id") == "T004").collect()[0]
    assert row.dq_pass == False
    assert row.dq_fail_reason == "quantity <= 0"


def test_dq_flags_null_trade_id_fails(sample_data):
    cleansed = cleanse(sample_data)
    flagged  = apply_dq_flags(cleansed)
    row = flagged.filter(F.col("trader_id") == "TR5").collect()[0]
    assert row.dq_pass == False


def test_dq_flags_valid_row_passes(sample_data):
    cleansed = cleanse(sample_data)
    flagged  = apply_dq_flags(cleansed)
    row = flagged.filter(F.col("trade_id") == "T001").collect()[0]
    assert row.dq_pass == True
    assert row.dq_fail_reason is None
