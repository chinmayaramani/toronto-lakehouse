"""
STAGE 3: GOLD LAYER DIMENSIONAL MODELING (TO-Lakehouse)
Objective: Query Silver Delta Lake using DuckDB, build Star Schema
Dimensional Data Marts (Facts & Dimensions), and output analytical Parquet marts.
"""

import os
import duckdb


def build_gold_marts(silver_path: str, gold_dir: str):
    os.makedirs(gold_dir, exist_ok=True)
    print("🦆 Initializing DuckDB engine...")
    con = duckdb.connect(database=":memory:")

    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    print(f"📖 Reading Silver Delta table directly into DuckDB from: {silver_path}")

    con.execute(f"""
        CREATE OR REPLACE VIEW silver_trips AS 
        SELECT * FROM delta_scan('{silver_path}');
    """)

    # 1. Inspect distinct user types to handle schema drift
    distinct_types = con.execute("SELECT DISTINCT user_type FROM silver_trips;").fetchall()
    print(f"🔍 Discovered User Types: {distinct_types}")

    # 2. BUILD DIMENSION TABLE: dim_stations
    print("⭐ Building Dimension: dim_stations...")
    con.execute("""
        CREATE OR REPLACE TABLE dim_stations AS
        WITH stations AS (
            SELECT DISTINCT start_station_id AS station_id FROM silver_trips WHERE start_station_id IS NOT NULL
            UNION
            SELECT DISTINCT end_station_id AS station_id FROM silver_trips WHERE end_station_id IS NOT NULL
        )
        SELECT 
            station_id,
            'Toronto Municipal Bike Station' AS station_network,
            CURRENT_TIMESTAMP AS dwh_created_at
        FROM stations;
    """)

    # 3. BUILD FACT TABLE: fct_daily_station_ridership (Star Schema)
    print("⭐ Building Fact: fct_daily_station_ridership...")
    con.execute("""
        CREATE OR REPLACE TABLE fct_daily_station_ridership AS
        SELECT 
            trip_date,
            start_station_id AS station_id,
            COALESCE(user_type, 'Unknown') AS user_type,
            COUNT(trip_id) AS total_trips_started,
            ROUND(AVG(duration_seconds) / 60.0, 2) AS avg_duration_minutes,
            ROUND(SUM(duration_seconds) / 3600.0, 2) AS total_hours_utilized
        FROM silver_trips
        GROUP BY trip_date, start_station_id, user_type;
    """)

    # 4. BUILD AGGREGATE MART: fct_daily_system_kpis
    print("⭐ Building Mart: fct_daily_system_kpis...")
    con.execute("""
        CREATE OR REPLACE TABLE fct_daily_system_kpis AS
        SELECT 
            trip_date,
            COUNT(trip_id) AS total_trips,
            COUNT(CASE WHEN LOWER(user_type) LIKE '%annual%' OR LOWER(user_type) LIKE '%member%' THEN 1 END) AS member_trips,
            COUNT(CASE WHEN LOWER(user_type) LIKE '%casual%' THEN 1 END) AS casual_trips,
            ROUND(
                COUNT(CASE WHEN LOWER(user_type) LIKE '%annual%' OR LOWER(user_type) LIKE '%member%' THEN 1 END) * 100.0 / COUNT(trip_id), 
                2
            ) AS member_share_pct,
            ROUND(AVG(duration_seconds) / 60.0, 2) AS avg_trip_duration_minutes
        FROM silver_trips
        GROUP BY trip_date
        ORDER BY trip_date ASC;
    """)

    # Export Gold marts as columnar Parquet files
    print(f"💾 Exporting Gold analytical marts to: {gold_dir}")
    con.execute(f"COPY dim_stations TO '{gold_dir}/dim_stations.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY fct_daily_station_ridership TO '{gold_dir}/fct_daily_station_ridership.parquet' (FORMAT PARQUET);")
    con.execute(f"COPY fct_daily_system_kpis TO '{gold_dir}/fct_daily_system_kpis.parquet' (FORMAT PARQUET);")

    # Display preview cleanly
    print("\n📊 Refined Gold System KPIs Preview:")
    preview_df = con.execute("SELECT * FROM fct_daily_system_kpis LIMIT 5;").df()
    print(preview_df.to_string(index=False))

    print("\n✅ Gold Layer Processing completed successfully.")
    con.close()


if __name__ == "__main__":
    SILVER_DELTA_PATH = "data/lakehouse/silver/bike_share_trips"
    GOLD_DIR = "data/lakehouse/gold"

    build_gold_marts(SILVER_DELTA_PATH, GOLD_DIR)