"""
EVENT GENERATOR (Simulator)
Simulates continuous arrival of trip event micro-batches into the landing zone.
"""

import os
import shutil
import time
import pandas as pd


def generate_stream_batches(
    source_raw_dir: str = "data/raw",
    landing_dir: str = "data/raw/streaming_landing",
    batch_size: int = 500,
    interval_seconds: int = 3,
):
    os.makedirs(landing_dir, exist_ok=True)

    # Find an existing raw CSV file to slice micro-batches from
    csv_files = [
        os.path.join(source_raw_dir, f)
        for f in os.listdir(source_raw_dir)
        if f.endswith(".csv") and not f.startswith(".")
    ]

    if not csv_files:
        print("❌ No raw CSV files found in data/raw. Run ingest_bronze.py once first to download data.")
        return

    source_csv = csv_files[0]
    print(f"📂 Simulating stream from: {source_csv}")

    df_iterator = pd.read_csv(source_csv, chunksize=batch_size)

    for i, batch_df in enumerate(df_iterator, start=1):
        target_file = os.path.join(landing_dir, f"batch_{i}_{int(time.time())}.csv")
        batch_df.to_csv(target_file, index=False)
        print(f"📦 Emitted micro-batch #{i} ({len(batch_df)} records) -> {target_file}")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    generate_stream_batches()