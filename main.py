"""
TO-Lakehouse Pipeline Orchestrator (CLI)
Executes end-to-end Medallion Architecture (Bronze -> Silver -> Gold).
"""

import argparse
import time
import duckdb

from src.build_gold import build_gold_marts
from src.ingest_bronze import download_raw_data, init_spark as init_bronze_spark, ingest_to_bronze
from src.process_silver import init_spark as init_silver_spark, process_silver
from src.stream_bronze import init_spark as init_stream_spark, start_bronze_stream


def print_summary(start_time, bronze_count, silver_count, quarantine_count, gold_dir):
    duration = round(time.time() - start_time, 2)
    
    # Query row counts from generated Gold Parquet marts using DuckDB
    con = duckdb.connect()
    dim_stations_cnt = con.execute(f"SELECT count(*) FROM read_parquet('{gold_dir}/dim_stations.parquet')").fetchone()[0]
    fct_ridership_cnt = con.execute(f"SELECT count(*) FROM read_parquet('{gold_dir}/fct_daily_station_ridership.parquet')").fetchone()[0]
    fct_kpis_cnt = con.execute(f"SELECT count(*) FROM read_parquet('{gold_dir}/fct_daily_system_kpis.parquet')").fetchone()[0]
    con.close()

    print("\n" + "=" * 65)
    print("           TO-LAKEHOUSE PIPELINE EXECUTION SUMMARY")
    print("=" * 65)
    print(f"[*] Bronze Layer : Ingested raw batch into append-only Delta table")
    print(f"    └── Total Records Ingested : {bronze_count:,}")
    print(f"[*] Silver Layer : Schema validated & quarantined via PySpark")
    print(f"    └── Valid Records Merged   : {silver_count:,}")
    print(f"    └── Corrupt Rows Isolated  : {quarantine_count:,} (Quarantine Table)")
    print(f"[*] Gold Layer   : Star Schema marts modeled via DuckDB (Parquet)")
    print(f"    ├── dim_stations           : {dim_stations_cnt:,} rows")
    print(f"    ├── fct_daily_station_ridership : {fct_ridership_cnt:,} rows")
    print(f"    └── fct_daily_system_kpis       : {fct_kpis_cnt:,} rows")
    print("-" * 65)
    print(f"[✓] End-to-End Pipeline completed successfully in {duration}s")
    print("=" * 65 + "\n")


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
        print("Starting Spark Structured Streaming Bronze Ingestion...")
        spark = init_stream_spark()
        query = start_bronze_stream(spark, streaming_landing, bronze_path, checkpoint_path)
        query.awaitTermination()

    elif args.mode in ["batch", "all"]:
        print("Executing Batch Medallion Pipeline...")
        start_time = time.time()
        
        download_raw_data(raw_dir)

        # Bronze
        spark_bronze = init_bronze_spark()
        ingest_to_bronze(spark_bronze, raw_dir, bronze_path)
        bronze_count = spark_bronze.read.format("delta").load(bronze_path).count()
        spark_bronze.stop()

        # Silver
        spark_silver = init_silver_spark()
        process_silver(spark_silver, bronze_path, silver_path, quarantine_path)
        silver_count = spark_silver.read.format("delta").load(silver_path).count()
        quarantine_count = spark_silver.read.format("delta").load(quarantine_path).count()
        spark_silver.stop()

        # Gold
        build_gold_marts(silver_path, gold_dir)

        # Output Summary Box
        print_summary(start_time, bronze_count, silver_count, quarantine_count, gold_dir)


if __name__ == "__main__":
    main()