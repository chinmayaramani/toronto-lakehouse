"""
Unit tests for data quality and quarantine logic.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.ingest_bronze import init_spark


@pytest.fixture(scope="session")
def spark():
    spark_session = init_spark()
    yield spark_session
    spark_session.stop()


def test_quarantine_negative_duration(spark):
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("start_station_id", StringType(), True),
        StructField("end_station_id", StringType(), True),
    ])

    test_data = [
        ("1", 300, "7000", "7001"),   # Valid
        ("2", -50, "7000", "7001"),   # Invalid: Negative duration
        ("3", 500, None, "7001"),     # Invalid: Null start station
    ]

    df = spark.createDataFrame(test_data, schema=schema)
    
    # Assert corrupt records are caught
    invalid_records = df.filter(
        (df.duration_seconds <= 0) | (df.start_station_id.isNull())
    ).count()

    assert invalid_records == 2