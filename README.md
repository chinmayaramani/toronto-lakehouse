# 🚴 TO-Lakehouse: Municipal Telemetry Medallion Architecture

A production-style 3-tier **Medallion Data Lakehouse** (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) processing over **550,000+ transactional ridership records** from the City of Toronto Open Data platform using **PySpark, Delta Lake, and DuckDB**.

---

## 🏛️ Architecture Overview

```text
[ City of Toronto Open Data CKAN API ]
                  │
                  ▼ (Python Ingestion)
          [ BRONZE LAYER ]  -> Immutable, Append-Only Delta Lake Archive
                  │
                  ▼ (PySpark Transformation Engine)
          [ SILVER LAYER ]  -> Schema Standardized & Partitioned Delta Tables
                  │            ↳ [ Quarantine: Invalid Durations / Null Anomalies ]
                  │
                  ▼ (DuckDB In-Memory OLAP Engine)
          [  GOLD LAYER  ]  -> Star Schema Dimensional Marts (Parquet)
```

---

## 🛠️ Tech Stack & Key Technical Decisions

* **Distributed Compute (PySpark 3.5.1):** Scalable batch DataFrame processing, data typing, and partition management.
* **Storage Format (Delta Lake 3.2.0):** Guarantees **ACID transactions** via transaction log validation (`_delta_log`), preventing partial batch corruption.
* **Resilient Data Quality (Quarantine Pattern):** Routes invalid records (`duration <= 0`, `duration > 24hr`, null stations) into an isolated quarantine table for monitoring rather than failing the entire pipeline.
* **In-Memory Analytics (DuckDB):** Directly scans Silver Delta Lake partitions to build Star-Schema fact and dimension marts (`dim_stations`, `fct_daily_station_ridership`, `fct_daily_system_kpis`) with sub-second execution without heavy Spark cluster overhead.

---

## 🚀 Quickstart

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/chinmayaramani/toronto-lakehouse.git](https://github.com/chinmayaramani/toronto-lakehouse.git)
   cd toronto-lakehouse
   ```

2. **Set up virtual environment & dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Execute the end-to-end pipeline:**
   ```bash
   python main.py
   ```