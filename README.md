# TO-Lakehouse: Municipal Telemetry Medallion Architecture

A production-style 3-tier **Medallion Data Lakehouse** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) processing over **550,000+ transactional ridership records** from the City of Toronto Open Data platform using **PySpark, Structured Streaming, Delta Lake, and DuckDB**.

---

## Architecture Overview

```text
[ City of Toronto Open Data API / Live Micro-Batches ]
                       │
                       ▼ (PySpark Structured Streaming & Batch Ingestion)
               [ BRONZE LAYER ]  -> Immutable, Append-Only Delta Lake Archive
                       │            ↳ Checkpoint Management & Lineage Auditing
                       │
                       ▼ (PySpark Transformation Engine)
               [ SILVER LAYER ]  -> Schema Standardized & Deduplicated Delta Tables (ACID MERGE)
                       │            ↳ [ Quarantine: Invalid Durations / Null Anomalies ]
                       │
                       ▼ (DuckDB In-Memory OLAP Engine)
               [  GOLD LAYER  ]  -> Star Schema Dimensional Marts & Aggregations
```

---

## Tech Stack & Key Technical Decisions

* **Streaming & Compute Engine (PySpark 3.5.1):** Dual-mode pipeline supporting both bulk batch processing and **PySpark Structured Streaming** with trigger-based micro-batching and fault-tolerant checkpointing.
* **Storage Format (Delta Lake 3.2.0):** Guarantees **ACID transactions** via `_delta_log`, enabling safe concurrent stream writes, schema enforcement, and idempotent `MERGE` (upsert) deduplication in the Silver layer.
* **Resilient Data Quality (Quarantine Pattern):** Routes invalid records (`duration <= 0`, `duration > 24hr`, null stations) into an isolated quarantine table for monitoring rather than failing the entire pipeline.
* **In-Memory Analytics (DuckDB):** Directly scans Silver Delta Lake partitions to build Star-Schema fact and dimension marts (`dim_stations`, `fct_daily_station_ridership`, `fct_daily_system_kpis`) with sub-second execution without heavy Spark cluster overhead.

---

## Quickstart

1. **Clone the repository:**
   ```bash
   git clone https://github.com/chinmayaramani/toronto-lakehouse.git
   cd toronto-lakehouse
   ```

2. **Set up virtual environment & dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Execute the pipeline:**

   * **Run the complete batch Medallion pipeline:**
     ```bash
     python main.py --mode batch
     ```

   * **Run real-time streaming ingestion:**
     ```bash
     # Terminal 1: Start Spark Structured Streaming consumer
     python src/stream_bronze.py

     # Terminal 2: Start micro-batch event generator
     python src/generate_stream_events.py
     ```
