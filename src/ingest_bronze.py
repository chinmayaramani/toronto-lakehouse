"""
STAGE 1: BRONZE LAYER INGESTION (TO-Lakehouse)
Objective: Ingest raw City of Toronto Bike Share transactional data into
an immutable, append-only Bronze Lakehouse Delta table with explicit schema enforcement.
"""

import os
import zipfile
import requests
import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def init_spark() -> pyspark.sql.SparkSession:
    """Initialize local Spark Session configured with Delta Lake extensions."""
    builder = (
        pyspark.sql.SparkSession.builder.appName("TO-Lakehouse-Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def download_raw_data(raw_dir: str):
    """Download and extract official City of Toronto Bike Share Ridership dataset."""
    os.makedirs(raw_dir, exist_ok=True)
    zip_path = os.path.join(raw_dir, "bikeshare_ridership.zip")

    package_url = (
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/"
        "package_show?id=bike-share-toronto-ridership-data"
    )

    if not any(fname.endswith(".csv") for fname in os.listdir(raw_dir)):
        print("📥 Sourcing dataset URL from City of Toronto Open Data CKAN API...")
        res = requests.get(package_url).json()
        resources = res["result"]["resources"]

        zip_resources = [r for r in resources if r.get("format", "").lower() == "zip"]
        target_resource = zip_resources[-1] if zip_resources else resources[0]
        download_url = target_resource["url"]

        print(f"📥 Downloading: {target_resource.get('name', 'Ridership Data')}...")
        headers = {"User-Agent": "Mozilla/5.0"}
        download_stream = requests.get(download_url, headers=headers, stream=True)

        with open(zip_path, "wb") as f:
            for chunk in download_stream.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        print("✅ Download completed.")

        print("📂 Extracting CSV files to raw data directory...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(raw_dir)
        print("✅ Extraction complete.")
    else:
        print("📁 Raw CSV data already detected locally. Skipping download.")


def ingest_to_bronze(
    spark: pyspark.sql.SparkSession, raw_dir: str, bronze_path: str
):
    """
    Ingest CSV files using explicit schema definitions and append to Bronze Delta table.
    """
    print("🚀 Reading raw CSV records with explicit schema enforcement...")

    # Explicit schema contract
    raw_schema = StructType([
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
    ])

    raw_df = (
        spark.read.option("header", "true")
        .schema(raw_schema)
        .csv(f"{raw_dir}/*.csv")
    )

    # Ingestion Metadata: Auditing and Data Lineage
    bronze_df = raw_df.withColumn(
        "ingestion_timestamp", current_timestamp()
    ).withColumn("source_file", input_file_name())

    record_count = bronze_df.count()
    print(f"📊 Total raw records processed: {record_count:,}")

    # Write to Bronze Delta Lake with schema overwrite permission
    print(f"💾 Appending to Bronze Delta Lake at: {bronze_path}")
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )

    print("✅ Bronze Ingestion complete successfully.")


if __name__ == "__main__":
    RAW_DIR = "data/raw"
    BRONZE_PATH = "data/lakehouse/bronze/bike_share_trips"

    download_raw_data(RAW_DIR)
    spark_session = init_spark()
    ingest_to_bronze(spark_session, RAW_DIR, BRONZE_PATH)
    spark_session.stop()