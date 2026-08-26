"""
STAGE 2: SILVER LAYER PROCESSING & QUARANTINE ENGINE (TO-Lakehouse)
Objective: Standardize schemas, enforce data quality constraints,
quarantine corrupt records, and persist clean data to a partitioned Delta Lake table.
"""

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    when,
    current_timestamp,
)


def init_spark() -> pyspark.sql.SparkSession:
    """Initialize local Spark Session configured with Delta Lake extensions."""
    builder = (
        pyspark.sql.SparkSession.builder.appName("TO-Lakehouse-Silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def process_silver(
    spark: pyspark.sql.SparkSession, bronze_path: str, silver_path: str, quarantine_path: str
):
    print(f"📖 Reading raw Bronze Delta table from: {bronze_path}...")
    bronze_df = spark.read.format("delta").load(bronze_path)

    # 1. Inspect raw column names (handling variations in municipal data naming conventions)
    cols = bronze_df.columns
    print(f"🔍 Discovered raw columns: {cols}")

    # Standardize column selection dynamically
    trip_id_col = next((c for c in cols if "id" in c.lower() and "trip" in c.lower() or c.lower() == "trip_id"), cols[0])
    duration_col = next((c for c in cols if "duration" in c.lower()), cols[1])
    start_time_col = next((c for c in cols if "start_time" in c.lower() or "start time" in c.lower()), cols[2])
    end_time_col = next((c for c in cols if "end_time" in c.lower() or "end time" in c.lower()), cols[3])
    start_station_col = next((c for c in cols if "start_station_id" in c.lower() or "from_station_id" in c.lower()), cols[4])
    end_station_col = next((c for c in cols if "end_station_id" in c.lower() or "to_station_id" in c.lower()), cols[5])
    user_type_col = next((c for c in cols if "user_type" in c.lower() or "user type" in c.lower()), cols[6])

    # 2. Schema Standardization and Type Casting
    standardized_df = (
        bronze_df.select(
            col(trip_id_col).cast("string").alias("trip_id"),
            col(duration_col).cast("integer").alias("duration_seconds"),
            to_timestamp(col(start_time_col)).alias("start_time"),
            to_timestamp(col(end_time_col)).alias("end_time"),
            col(start_station_col).cast("string").alias("start_station_id"),
            col(end_station_col).cast("string").alias("end_station_id"),
            col(user_type_col).cast("string").alias("user_type"),
            col("ingestion_timestamp"),
        )
        .withColumn("trip_date", to_date(col("start_time")))
        .dropDuplicates(["trip_id"])
    )

    # 3. Data Quality Validation Rules & Error Tagging
    validated_df = standardized_df.withColumn(
        "error_reason",
        when(col("duration_seconds") <= 0, "INVALID_DURATION_NON_POSITIVE")
        .when(col("duration_seconds") > 86400, "INVALID_DURATION_EXCEEDS_24HR")
        .when(col("start_time").isNull(), "NULL_START_TIMESTAMP")
        .when(col("start_station_id").isNull(), "NULL_START_STATION")
        .when(col("end_station_id").isNull(), "NULL_END_STATION")
        .otherwise(None),
    )

    # Split Clean vs Corrupted Records
    clean_df = validated_df.filter(col("error_reason").isNull()).drop("error_reason").withColumn(
        "processed_timestamp", current_timestamp()
    )
    quarantine_df = validated_df.filter(col("error_reason").isNotNull()).withColumn(
        "quarantined_timestamp", current_timestamp()
    )

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    print(f"✅ Clean Silver Records: {clean_count:,}")
    print(f"⚠️  Quarantined Corrupt Records: {quarantine_count:,}")

    # 4. Write Clean Records to Partitioned Silver Delta Table
    print(f"💾 Writing partitioned Silver Delta Lake at: {silver_path}...")
    (
        clean_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("trip_date")
        .save(silver_path)
    )

    # 5. Write Quarantined Records for Data Quality Auditing
    if quarantine_count > 0:
        print(f"💾 Writing Quarantine Delta Lake at: {quarantine_path}...")
        (
            quarantine_df.write.format("delta")
            .mode("overwrite")
            .save(quarantine_path)
        )

    print("✅ Silver Layer Processing completed successfully.")


if __name__ == "__main__":
    BRONZE_DELTA_PATH = "data/lakehouse/bronze/bike_share_trips"
    SILVER_DELTA_PATH = "data/lakehouse/silver/bike_share_trips"
    QUARANTINE_DELTA_PATH = "data/lakehouse/silver/quarantine_trips"

    spark_session = init_spark()
    process_silver(spark_session, BRONZE_DELTA_PATH, SILVER_DELTA_PATH, QUARANTINE_DELTA_PATH)
    spark_session.stop()