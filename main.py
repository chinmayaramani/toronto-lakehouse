"""
TO-Lakehouse Pipeline Orchestrator (CLI)
Executes end-to-end Medallion Architecture (Bronze -> Silver -> Gold).
"""

import argparse
from src.build_gold import run_gold_pipeline
from src.ingest_bronze import download_raw_data, init_spark as init_bronze_spark, ingest_to_bronze
from src.process_silver import init_spark as init_silver_spark, process_silver_layer
from src.stream_bronze import init_spark as init_stream_spark, start_bronze_stream


def main():
    parser = argparse.ArgumentParser(description="TO-Lakehouse Pipeline Runner")
    parser.add_argument(
        "--mode",
        choices=["batch", "stream", "all"],
        default="batch",
        help="Execution mode: batch, stream, or all",
    )
    args = parser.parse_args()

    raw_dir = "data/raw"
    streaming_landing = "data/raw/streaming_landing"
    bronze_path = "data/lakehouse/bronze/bike_share_trips"
    silver_path = "data/lakehouse/silver/bike_share_trips"
    quarantine_path = "data/lakehouse/silver/quarantine_trips"
    checkpoint_path = "data/lakehouse/checkpoints/bronze_stream"
    gold_dir = "data/lakehouse/gold"

    if args.mode == "stream":
        print("🚀 Starting Spark Structured Streaming Bronze Ingestion...")
        spark = init_stream_spark()
        query = start_bronze_stream(spark, streaming_landing, bronze_path, checkpoint_path)
        query.awaitTermination()

    elif args.mode in ["batch", "all"]:
        print("▶️ Executing Batch Medallion Pipeline...")
        download_raw_data(raw_dir)

        # Bronze
        spark_bronze = init_bronze_spark()
        ingest_to_bronze(spark_bronze, raw_dir, bronze_path)
        spark_bronze.stop()

        # Silver
        spark_silver = init_silver_spark()
        process_silver_layer(spark_silver, bronze_path, silver_path, quarantine_path)
        spark_silver.stop()

        # Gold
        run_gold_pipeline(silver_path, gold_dir)
        print("🏆 Medallion Pipeline execution completed successfully.")


if __name__ == "__main__":
    main()