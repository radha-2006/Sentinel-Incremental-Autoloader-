# Project Sentinel: Enterprise-Grade Incremental Medallion Pipeline

Project Sentinel is a production-ready, cloud-native data ingestion pipeline built using **Databricks Auto Loader** and **Delta Lake** inside a **Medallion Architecture (Bronze → Silver → Gold)**. It is designed to simulate a high-throughput, low-latency financial transaction pipeline (such as UPI), featuring incremental ingestion, deduplication, schema evolution, and real-time operational KPI aggregation.

---

## 1. Architecture Flow

```
[Raw Inbound Files] ──> landing in ADLS Gen2 `/raw_landing/`
                               │
                               ▼ (Auto Loader: cloudFiles, availableNow=True)
[Bronze Delta Table] ─> Raw ingest + metadata (`file_name`, `arrival_time`, `ingestion_time`)
                               │
                               ▼ (foreachBatch Micro-Batch Stream + Merge Upsert)
[Silver Delta Table] ─> Cleansed, deduplicated, and mapped to target schemas
                               │
                               ▼ (Structured Stream Group-By Aggregations)
[Gold Delta Table] ──> Key Performance Indicators & Anomaly Alerts
```

### Architectural Highlights
- **Exactly-Once Processing**: Re-runs are fully idempotent. Auto Loader tracks file offset states using a RocksDB store, and Delta Lake leverages transaction log checkpoints.
- **Dynamic Schema Evolution**: Auto Loader (`schemaEvolutionMode = addNewColumns`) and Delta Lake (`schema.autoMerge.enabled = true`) dynamically expand schemas without DDL changes or pipeline crashes.
- **Late-Arriving Data**: The Silver layer utilizes a `MERGE INTO` upsert matching on `transaction_id` to update historical anomalies or corrections in-place rather than duplicating rows.
- **Cost-Optimized Triggers**: Structured streaming runs with `trigger(availableNow=True)`, allowing job clusters to spin up, ingest all backlogged files, write Delta blocks, and immediately shut down.

---

## 2. Directory Structure

Inside this folder, the project layout is structured as follows:

```
project/
│
├── README.md                           <-- Master Documentation (This file)
├── local_simulator.py                  <-- Local Python simulation engine (Pandas/SQLite)
├── requirements.txt                    <-- Python simulation dependencies
│
├── datasets/                           <-- Source CSV files
│   ├── Fact_Sales_1.csv                <-- Base transactions (Batch 1)
│   ├── Fact_Sales_2.csv                <-- Incremental & late data (Batch 2)
│   └── 2010-12-08.csv                  <-- Schema evolution test file (Batch 3)
│
├── notebooks/                          <-- Production Databricks Notebooks
│   ├── 00_setup_and_configs.py         <-- Dynamic parameters and Spark configs
│   ├── 01_bronze_ingestion.py          <-- Auto Loader ingestion
│   ├── 02_silver_cleansing.py          <-- Micro-batch dropna, deduplication, and Merge
│   └── 03_gold_metrics.py              <-- Complete-mode aggregations and KPI metrics
│
├── src/                                <-- Reusable Python source modules
│   ├── pipeline_utils.py               <-- Structured JSON logs & directory creation
│   └── data_generator.py               <-- Simulated file copy handler for landing zone
│
├── adf/                                <-- Azure Data Factory Assets
│   └── adf_pipeline.json               <-- ADF multi-notebook orchestration JSON template
│
├── databricks/                         <-- Databricks Workflow Jobs
│   └── databricks_workflow.json        <-- Databricks workflow JSON job template
│
└── sql/                                <-- Delta Table Maintenance
    └── maintenance_commands.sql        <-- SQL compaction, vacuum, and stats queries
```

---

## 3. Local Simulation Engine Guide

To run a full demo of the Medallion pipeline and check schema drift / exactly-once processing offline, you can run the local simulation suite using standard Python (3.x) with `pandas` and SQLite.

### Step 3.1: Install Dependencies
Install Pandas:
```bash
pip install -r requirements.txt
```

### Step 3.2: Reset landing zone and database
Delete any old SQLite file and clean landing folders:
```bash
python local_simulator.py --clean
python src/data_generator.py --reset
```

### Step 3.3: Simulate Step 1 (Ingesting Base Data)
1. Drop the first file (`Fact_Sales_1.csv`):
   ```bash
   python src/data_generator.py --step 1
   ```
2. Execute pipeline:
   ```bash
   python local_simulator.py --run
   ```
*Observation:* Auto Loader ingests 4,410 raw records into Bronze, filters and writes 4,410 rows into Silver, and calculates initial Gold aggregates.

### Step 3.4: Verify Idempotency (Exactly-Once)
Run the pipeline again without dropping new files:
```bash
python local_simulator.py --run
```
*Observation:* Offset store reports 0 new files. Bronze and Silver records stay exactly at 4,410.

### Step 3.5: Simulate Step 2 (Incremental Batch & Merges)
1. Drop the second file (`Fact_Sales_2.csv`):
   ```bash
   python src/data_generator.py --step 2
   ```
2. Run pipeline:
   ```bash
   python local_simulator.py --run
   ```
*Observation:* Pipeline ingests the new file (40 rows). Bronze and Silver counts rise to 4,450. Gold values update immediately.

### Step 3.6: Simulate Step 3 (Schema Evolution)
1. Drop the third file (`2010-12-08.csv`):
   ```bash
   python src/data_generator.py --step 3
   ```
2. Run pipeline:
   ```bash
   python local_simulator.py --run
   ```
*Observation:* The pipeline detects the new format (which has no `transaction_id` and contains new columns like `InvoiceNo`, `StockCode`, etc.). SQLite schemas evolve dynamically without crashing, and Silver cleansing drops the records from the core sales table because they fail key validation.

---

## 4. Production Databricks Deployment Guide

Deploying to an active Azure Databricks workspace:

### 4.1 Notebook Execution
Import the files inside the `notebooks/` folder into your workspace under `/Shared/Project_Sentinel/notebooks/`.
Execution parameters are passed using Widgets:
- `environment`: `dev` or `prod`
- `base_path`: Target ADLS Gen2 path, e.g. `abfss://delta-forge@deltaforge.dfs.core.windows.net/`
- `database_name`: Metastore catalog target database (e.g. `legacy_hms_db`)

### 4.2 ADLS Gen2 Setup
Ensure your cluster or Managed Identity (ADF/Databricks) has the **Storage Blob Data Contributor** role on the storage account. Checkpoint, schema, and table paths will reside at:
- Raw Ingest Landing: `{base_path}/raw_landing/`
- Checkpoints: `{base_path}/checkpoints/`
- Managed Tables: `{base_path}/bronze/`, `{base_path}/silver/`, and `{base_path}/gold/`

---

## 5. Azure Data Factory (ADF) Orchestration

The pipeline is orchestrated in Azure Data Factory. The JSON deployment template is in [adf_pipeline.json](file:///c:/Users/radha/OneDrive/Documents/CT-project/project/adf/adf_pipeline.json).

### Recommended ADF Properties
- **Retry Count**: Set to 2-3 retries with a 60-second interval. It is completely safe to retry any failed execution since all checkpoints and upsert merges are idempotent.
- **Timeout**: Set to 2 hours to allow catch-up runs when processing a massive backlog.
- **Execution Schedule**: fixed cron schedule (hourly/daily) or event-driven trigger firing on blob-created events in `{base_path}/raw_landing/`.

---

## 6. Daily Delta Table Maintenance

Compaction and cleanup should be scheduled nightly to prevent performance degradation:

```sql
-- 1. Compact small files (optimizes read throughput)
OPTIMIZE legacy_hms_db.bronze_transactions;
OPTIMIZE legacy_hms_db.silver_transactions ZORDER BY (transaction_id);

-- 2. Purge stale transaction logs (removes historical files older than 7 days)
VACUUM legacy_hms_db.bronze_transactions RETAIN 168 HOURS;
VACUUM legacy_hms_db.silver_transactions RETAIN 168 HOURS;

-- 3. Update table statistics
ANALYZE TABLE legacy_hms_db.silver_transactions COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## 7. Failure Recovery Playbook

1. **Transient Network or Cluster Timeout**: ADF retry triggers recover the stream automatically using Structured Streaming offsets.
2. **Schema Drift**: If a new column is added, check `/checkpoints/bronze_schema`. If the evolution is valid, the stream restarts automatically.
3. **Poison File Blocks Ingestion**: Move the corrupted file out of `/raw_landing/` into a `/quarantine/` directory and re-trigger. Enabling `cloudFiles.rescuedDataColumn` collects bad rows in a JSON field (`_rescued_data`) without halting the pipeline.
4. **Data Corruption Recovery**: Run `DESCRIBE HISTORY legacy_hms_db.silver_transactions` to identify the corrupted batch. Use `RESTORE TABLE legacy_hms_db.silver_transactions TO VERSION AS OF <version_id>` to roll back to a known stable version.

---

## 8. Databricks Free Community Edition Setup

To execute the PySpark notebooks for free on the **Databricks Community Edition**:
1. Log in to your Databricks Community account.
2. Import the four files inside the `notebooks/` directory.
3. Upload the raw CSV files (`Fact_Sales_1.csv` etc.) from your `datasets/` folder through the Catalog interface (which saves them to `dbfs:/FileStore/tables/`).
4. In the `01_bronze_ingestion` notebook, create a temporary cell to set up the landing folder and copy files:
   ```python
   dbutils.fs.mkdirs("/FileStore/sentinel/raw_landing")
   dbutils.fs.cp("/FileStore/tables/Fact_Sales_1.csv", "/FileStore/sentinel/raw_landing/Fact_Sales_1.csv")
   ```
5. Configure widget values:
   * `base_path`: `/FileStore/sentinel`
   * `environment`: `dev`
6. Run notebooks interactively in sequence: **`01_bronze_ingestion`** $\rightarrow$ **`02_silver_cleansing`** $\rightarrow$ **`03_gold_metrics`**.
