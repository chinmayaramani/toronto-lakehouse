"""
TO-Lakehouse: End-to-End Pipeline Orchestration Runner
Executes: Bronze Ingestion -> Silver Validation/Quarantine -> Gold Dimensional Modeling
"""

import sys
import time
from src.build_gold import build_gold_marts
from src.ingest_bronze import download_raw_data, ingest_to_bronze, init_spark
from src.process_silver import process_silver


def run_pipeline():
  total_start = time.time()
  print("=" * 60)
  print("🚀 STARTING TO-LAKEHOUSE DATA PIPELINE EXECUTION")
  print("=" * 60)

  raw_dir = "data/raw"
  bronze_path = "data/lakehouse/bronze/bike_share_trips"
  silver_path = "data/lakehouse/silver/bike_share_trips"
  quarantine_path = "data/lakehouse/silver/quarantine_trips"
  gold_dir = "data/lakehouse/gold"

  # --- PHASE 1: BRONZE ---
  print("\n[PHASE 1/3] EXECUTING BRONZE RAW INGESTION...")
  p1_start = time.time()
  download_raw_data(raw_dir)
  spark = init_spark()
  ingest_to_bronze(spark, raw_dir, bronze_path)
  print(f"⏱️  Bronze phase completed in {round(time.time() - p1_start, 2)}s")

  # --- PHASE 2: SILVER ---
  print("\n[PHASE 2/3] EXECUTING SILVER VALIDATION & QUARANTINE...")
  p2_start = time.time()
  process_silver(spark, bronze_path, silver_path, quarantine_path)
  spark.stop()  # Free memory
  print(f"⏱️  Silver phase completed in {round(time.time() - p2_start, 2)}s")

  # --- PHASE 3: GOLD ---
  print("\n[PHASE 3/3] EXECUTING GOLD DIMENSIONAL MODELING (DUCKDB)...")
  p3_start = time.time()
  build_gold_marts(silver_path, gold_dir)
  print(f"⏱️  Gold phase completed in {round(time.time() - p3_start, 2)}s")

  total_duration = round(time.time() - total_start, 2)
  print("\n" + "=" * 60)
  print(f"🎉 PIPELINE RUN SUCCEEDED IN {total_duration}s")
  print("=" * 60)


if __name__ == "__main__":
  try:
    run_pipeline()
  except Exception as e:
    print(f"❌ Pipeline Execution Failed: {str(e)}")
    sys.exit(1)