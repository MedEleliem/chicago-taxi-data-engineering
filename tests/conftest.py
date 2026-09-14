from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("chicago-taxi-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def silver_df(spark: SparkSession):
    rows = [
        ("1", "taxi-1", "2026-01-01 14:00:00", "2026-01-01 15:00:00", 8, 32, 3600, 60.0, 10.0, "10.00", "2.00", "12.00", "CREDIT CARD", "A", True, 0.2),
        ("2", "taxi-2", "2026-01-01 15:00:00", "2026-01-01 15:30:00", 8, 33, 1800, 30.0, 5.0, "20.00", "0.00", "20.00", "CASH", "B", False, 0.0),
        ("3", "taxi-3", "2026-01-02 10:00:00", "2026-01-02 13:30:00", 9, 33, 12600, 210.0, 0.0, "350.00", "200.00", "550.00", "CREDIT CARD", "A", True, 0.5714),
    ]
    columns = [
        "trip_id",
        "taxi_id",
        "trip_start_timestamp",
        "trip_end_timestamp",
        "pickup_community_area",
        "dropoff_community_area",
        "trip_seconds",
        "trip_duration_minutes",
        "trip_miles",
        "fare",
        "tips",
        "trip_total",
        "payment_type",
        "company",
        "has_tip",
        "tip_rate",
    ]
    df = spark.createDataFrame(rows, columns)
    return (
        df.withColumn("trip_start_timestamp", F.to_timestamp("trip_start_timestamp"))
        .withColumn("trip_end_timestamp", F.to_timestamp("trip_end_timestamp"))
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .withColumn("trip_year", F.year("trip_start_timestamp"))
        .withColumn("trip_month", F.month("trip_start_timestamp"))
        .withColumn("trip_hour", F.hour("trip_start_timestamp"))
        .withColumn("fare", F.col("fare").cast("decimal(12,2)"))
        .withColumn("tips", F.col("tips").cast("decimal(12,2)"))
        .withColumn("trip_total", F.col("trip_total").cast("decimal(12,2)"))
    )
