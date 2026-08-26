"""
STAGE 2: SILVER LAYER PROCESSING & QUARANTINE ENGINE (TO-Lakehouse)
Objective: Enforce data quality validation, quarantine corrupt records,
and perform an idempotent ACID MERGE (Upsert) into the partitioned Silver Delta table.
"""

import os
import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
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
    spark: pyspark.sql.SparkSession,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
):
    print(f"📖 Reading raw Bronze Delta table from: {bronze_path}...")
    bronze_df = spark.read.format("delta").load(bronze_path)

    # 1. Standardize types and build audit columns
    standardized_df = (
        bronze_df.select(
            col("Trip_Id").alias("trip_id"),
            col("Trip_Duration").alias("duration_seconds"),
            to_timestamp(col("Start_Time")).alias("start_time"),
            to_timestamp(col("End_Time")).alias("end_time"),
            col("Start_Station_Id").alias("start_station_id"),
            col("End_Station_Id").alias("end_station_id"),
            col("User_Type").alias("user_type"),
            col("ingestion_timestamp"),
        )
        .withColumn("trip_date", to_date(col("start_time")))
        .dropDuplicates(["trip_id"])
    )

    # 2. Data Quality Constraints & Quarantine Routing
    validated_df = standardized_df.withColumn(
        "error_reason",
        when(col("duration_seconds") <= 0, "INVALID_DURATION_NON_POSITIVE")
        .when(col("duration_seconds") > 86400, "INVALID_DURATION_EXCEEDS_24HR")
        .when(col("start_time").isNull(), "NULL_START_TIMESTAMP")
        .when(col("start_station_id").isNull(), "NULL_START_STATION")
        .when(col("end_station_id").isNull(), "NULL_END_STATION")
        .otherwise(None),
    )

    clean_df = validated_df.filter(col("error_reason").isNull()).drop(
        "error_reason"
    ).withColumn("processed_timestamp", current_timestamp())

    quarantine_df = validated_df.filter(col("error_reason").isNotNull()).withColumn(
        "quarantined_timestamp", current_timestamp()
    )

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    print(f"✅ Clean Silver Records: {clean_count:,}")
    print(f"⚠️  Quarantined Corrupt Records: {quarantine_count:,}")

    # 3. IDEMPOTENT ACID MERGE (UPSERT) INTO SILVER DELTA LAKE
    if not DeltaTable.isDeltaTable(spark, silver_path):
        print(f"🚀 Initializing Silver Delta Lake table at: {silver_path}...")
        (
            clean_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("trip_date")
            .save(silver_path)
        )
    else:
        print(f"🔄 Performing ACID MERGE (Upsert) on target Silver table at: {silver_path}...")
        target_delta = DeltaTable.forPath(spark, silver_path)
        (
            target_delta.alias("target")
            .merge(
                clean_df.alias("source"),
                "target.trip_id = source.trip_id AND target.trip_date = source.trip_date",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # 4. Save Quarantine Delta Lake
    if quarantine_count > 0:
        print(f"💾 Writing Quarantine Delta Lake at: {quarantine_path}...")
        quarantine_df.write.format("delta").mode("overwrite").save(quarantine_path)

    print("✅ Silver Layer Upsert & Validation complete successfully.")


if __name__ == "__main__":
    BRONZE_DELTA_PATH = "data/lakehouse/bronze/bike_share_trips"
    SILVER_DELTA_PATH = "data/lakehouse/silver/bike_share_trips"
    QUARANTINE_DELTA_PATH = "data/lakehouse/silver/quarantine_trips"

    spark_session = init_spark()
    process_silver(
        spark_session,
        BRONZE_DELTA_PATH,
        SILVER_DELTA_PATH,
        QUARANTINE_DELTA_PATH,
    )
    spark_session.stop()