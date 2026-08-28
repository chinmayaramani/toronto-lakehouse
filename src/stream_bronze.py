"""
STAGE 1B: STREAMING BRONZE LAYER INGESTION (TO-Lakehouse)
Objective: Ingest streaming micro-batches of Bike Share trip events into an
immutable, append-only Bronze Delta Lake table using PySpark Structured Streaming.
"""

import os
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def init_spark() -> pyspark.sql.SparkSession:
    """Initialize local Spark Session configured with Delta Lake extensions."""
    builder = (
        pyspark.sql.SparkSession.builder.appName("TO-Lakehouse-Streaming-Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def start_bronze_stream(
    spark: pyspark.sql.SparkSession,
    stream_input_dir: str,
    bronze_path: str,
    checkpoint_path: str,
    processing_time: str = "5 seconds",
):
    """
    Stream continuous CSV/JSON payloads into Bronze Delta Lake table
    with checkpointing and schema enforcement.
    """
    os.makedirs(stream_input_dir, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    # Explicit schema contract
    stream_schema = StructType(
        [
            StructField("Trip_Id", StringType(), True),
            StructField("Trip_Duration", IntegerType(), True),
            StructField("Start_Station_Id", StringType(), True),
            StructField("Start_Time", StringType(), True),
            StructField("Start_Station_Name", StringType(), True),
            StructField("End_Station_Id", StringType(), True),
            StructField("End_Time", StringType(), True),
            StructField("End_Station_Name", StringType(), True),
            StructField("Bike_Id", StringType(), True),
            StructField("User_Type", StringType(), True),
            StructField("Bike_Model", StringType(), True),
        ]
    )

    print(f"📡 Watching for incoming micro-batches in: {stream_input_dir}")

    # Read streaming data from input directory
    streaming_df = (
        spark.readStream.option("header", "true")
        .option("maxFilesPerTrigger", 1)
        .schema(stream_schema)
        .csv(stream_input_dir)
    )

    # Ingestion metadata columns
    enriched_stream_df = streaming_df.withColumn(
        "ingestion_timestamp", current_timestamp()
    ).withColumn("source_file", input_file_name())

    print(f"🚀 Writing stream directly to Bronze Delta Table at: {bronze_path}")

    # Structured Streaming Sink to Delta Lake
    query = (
        enriched_stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=processing_time)
        .start(bronze_path)
    )

    return query


if __name__ == "__main__":
    STREAM_INPUT_DIR = "data/raw/streaming_landing"
    BRONZE_PATH = "data/lakehouse/bronze/bike_share_trips"
    CHECKPOINT_PATH = "data/lakehouse/checkpoints/bronze_stream"

    spark_session = init_spark()
    streaming_query = start_bronze_stream(
        spark_session, STREAM_INPUT_DIR, BRONZE_PATH, CHECKPOINT_PATH
    )

    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Streaming ingestion stopped safely.")
        streaming_query.stop()
        spark_session.stop()