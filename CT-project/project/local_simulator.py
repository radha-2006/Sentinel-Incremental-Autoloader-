import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from src.pipeline_utils import log_event, create_directory_structure

DB_FILE = "sentinel_local_warehouse.db"

def get_db_connection():
    return sqlite3.connect(DB_FILE)

def evolve_sqlite_schema(conn, table_name, df):
    """
    Checks if the DataFrame contains columns not present in the SQLite table,
    and runs ALTER TABLE ADD COLUMN to dynamically evolve the schema.
    Handles case-insensitivity to prevent duplicate column exceptions.
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    existing_cols = {row[1] for row in columns_info}
    existing_cols_lower = {row[1].lower(): row[1] for row in columns_info}
    
    if not existing_cols:
        return
        
    for col in list(df.columns):
        col_lower = col.lower()
        if col_lower in existing_cols_lower:
            actual_case = existing_cols_lower[col_lower]
            if actual_case != col:
                df.rename(columns={col: actual_case}, inplace=True)
                log_event("SCHEMA_EVOLUTION", f"Aligned column casing in DataFrame: {col} -> {actual_case}", "DEBUG")
        else:
            col_type = "TEXT"
            if pd.api.types.is_integer_dtype(df[col]):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[col]):
                col_type = "REAL"
            
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [{col}] {col_type}")
            log_event("SCHEMA_EVOLUTION", f"Evolved SQLite table {table_name}: added column {col} ({col_type})")
            existing_cols_lower[col_lower] = col
    conn.commit()


def setup_local_database():
    """
    Initializes the SQLite database tables for Bronze, Silver, and Gold layers.
    Also creates a file offset tracking log table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Checkpoint File Offset Store (Auto Loader RockDB simulation)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offset_store (
            file_name TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Pre-create Silver table structure
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver_transactions (
            transaction_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            transaction_time TIMESTAMP,
            ingestion_time TIMESTAMP,
            file_name TEXT,
            file_arrival_time TIMESTAMP,
            processed_time TIMESTAMP,
            processed_flag INTEGER,
            product_id TEXT,
            payment TEXT,
            credit_card TEXT,
            loyalty_card TEXT,
            cost REAL,
            quantity INTEGER
        )
    """)
    
    # Gold Table structure
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_kpi_metrics (
            processed_flag INTEGER PRIMARY KEY,
            total_records_processed INTEGER,
            latest_data_freshness TIMESTAMP,
            total_transaction_value REAL,
            avg_transaction_value REAL,
            anomalous_high_value_count INTEGER
        )
    """)
    
    conn.commit()
    conn.close()
    log_event("DATABASE_SETUP", "SQLite local warehouse tables configured successfully.")

def run_bronze_layer():
    """
    Simulates Databricks Auto Loader: reads raw csv files from landing zone,
    checks offset log to only process new files, enriches with metadata,
    and appends to the Bronze Delta table.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    landing_dir = os.path.join(base_dir, "raw_landing")
    
    if not os.path.exists(landing_dir) or len(os.listdir(landing_dir)) == 0:
        log_event("BRONZE_LAYER", "No files found in raw landing zone. Ingestion skipped.")
        return 0
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Read processed files list
    cursor.execute("SELECT file_name FROM offset_store")
    processed_files = set([row[0] for row in cursor.fetchall()])
    
    new_files = [f for f in os.listdir(landing_dir) if f.endswith('.csv') and f not in processed_files]
    
    if not new_files:
        log_event("BRONZE_LAYER", "No new files detected. Ingestion skipped.")
        conn.close()
        return 0
        
    ingested_rows = 0
    for filename in new_files:
        filepath = os.path.join(landing_dir, filename)
        arrival_time = datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
        
        # Load CSV using pandas
        try:
            df = pd.read_csv(filepath)
            log_event("BRONZE_LAYER", f"Reading new file: {filename} ({len(df)} rows)")
            
            # Clean up trailing comma columns (like Unnamed: 10)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            # Enrich with operational metadata (mimics Databricks _metadata)
            df['file_name'] = filepath
            df['file_arrival_time'] = arrival_time
            df['ingestion_time'] = datetime.utcnow().isoformat()
            
            # Simple Rescued Data logic (JSON representations of invalid/null rows)
            # Find rows where transaction_id is missing or NaN
            if 'transaction_id' in df.columns:
                invalid_mask = df['transaction_id'].isna()
            else:
                df['transaction_id'] = None
                invalid_mask = pd.Series([True] * len(df))
                
            if invalid_mask.any():
                df['_rescued_data'] = np.where(invalid_mask, df.to_json(orient='records', lines=True), None)
            else:
                df['_rescued_data'] = None
                
            # Evolve SQLite schema before appending to support dynamic schema changes (Auto Loader schemaMerge)
            evolve_sqlite_schema(conn, "bronze_transactions", df)
            
            # Append to Bronze table (simulates append mode writeStream)
            df.to_sql("bronze_transactions", conn, if_exists="append", index=False)

            
            # Update File Offset log
            cursor.execute("INSERT INTO offset_store (file_name) VALUES (?)", (filename,))
            ingested_rows += len(df)
            log_event("BRONZE_LAYER", f"Successfully ingested {filename} into bronze_transactions", "INFO", {"rows": len(df)})
        except Exception as e:
            log_event("BRONZE_LAYER", f"Failed to ingest file {filename}: {str(e)}", "ERROR")
            
    conn.commit()
    conn.close()
    return ingested_rows

def run_silver_layer():
    """
    Simulates Silver Structured Streaming pipeline:
    - Reads Bronze table incrementally (rows where ingestion_time > last processed)
    - Enforces Data Quality rules (removes null transaction_id and amount/price)
    - Deduplicates records within microbatch
    - Maps incoming CSV schemas (customer_id -> user_id, price -> amount)
    - Executes idempotent MERGE (UPSERT) into silver_transactions
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Read Bronze data that hasn't been merged to Silver yet
    # We trace this using processed_flag or by selecting transaction_ids that need processing.
    # In SQLite, we can select everything in bronze and merge it. Idempotent merge ensures duplicates are resolved.
    try:
        bronze_df = pd.read_sql_query("SELECT * FROM bronze_transactions", conn)
    except Exception as e:
        log_event("SILVER_LAYER", f"Bronze table does not exist or empty: {str(e)}")
        conn.close()
        return 0
        
    if bronze_df.empty:
        log_event("SILVER_LAYER", "No data in Bronze table. Skip.")
        conn.close()
        return 0
        
    cols = bronze_df.columns
    
    # 2. Schema alignment: map incoming columns if they exist
    if "customer_id" in cols and "user_id" not in cols:
        bronze_df.rename(columns={"customer_id": "user_id"}, inplace=True)
    if "price" in cols and "amount" not in cols:
        bronze_df.rename(columns={"price": "amount"}, inplace=True)
    if "transactional_date" in cols and "transaction_time" not in cols:
        bronze_df.rename(columns={"transactional_date": "transaction_time"}, inplace=True)
        
    # Ensure amount and user_id exist, otherwise add them as Null
    if "amount" not in bronze_df.columns:
        bronze_df["amount"] = None
    if "user_id" not in bronze_df.columns:
        bronze_df["user_id"] = None
    if "transaction_time" not in bronze_df.columns:
        bronze_df["transaction_time"] = None
        
    # 3. Data Cleansing
    # Null filtering: dropna(subset=["transaction_id", "amount"])
    before_clean = len(bronze_df)
    bronze_df.dropna(subset=["transaction_id", "amount"], inplace=True)
    null_dropped = before_clean - len(bronze_df)
    
    # Deduplication: dropDuplicates(["transaction_id"])
    before_dedup = len(bronze_df)
    bronze_df.drop_duplicates(subset=["transaction_id"], keep="last", inplace=True)
    duplicates_dropped = before_dedup - len(bronze_df)
    
    # Enrich processing properties
    bronze_df["processed_time"] = datetime.utcnow().isoformat()
    bronze_df["processed_flag"] = 1
    
    # 4. Upsert (MERGE INTO) into silver_transactions
    merged_count = 0
    updated_count = 0
    inserted_count = 0
    
    # Evolve silver_transactions table schema dynamically to support any new schema-evolution columns
    evolve_sqlite_schema(conn, "silver_transactions", bronze_df)
    
    # Query current columns in silver_transactions to build dynamic SQL queries
    cursor.execute("PRAGMA table_info(silver_transactions)")
    silver_cols = [row[1] for row in cursor.fetchall()]
    
    # Ensure all target columns exist in the incoming DataFrame (default to None)
    for col_name in silver_cols:
        if col_name not in bronze_df.columns:
            bronze_df[col_name] = None
            
    # Build dynamic UPDATE and INSERT SQL queries based on the evolved table structure
    update_cols = [c for c in silver_cols if c != "transaction_id"]
    update_clause = ", ".join([f"[{c}] = ?" for c in update_cols])
    update_sql = f"UPDATE silver_transactions SET {update_clause} WHERE transaction_id = ?"
    
    col_list = ", ".join([f"[{c}]" for c in silver_cols])
    placeholders = ", ".join(["?"] * len(silver_cols))
    insert_sql = f"INSERT INTO silver_transactions ({col_list}) VALUES ({placeholders})"
    
    # Perform SQL upsert (idempotent merge) for each record
    for _, row in bronze_df.iterrows():
        tx_id = int(row["transaction_id"])
        
        # Check if record exists
        cursor.execute("SELECT 1 FROM silver_transactions WHERE transaction_id = ?", (tx_id,))
        exists = cursor.fetchone()
        
        if exists:
            # Build parameter values for UPDATE: non-ID columns first, then ID at the end
            update_vals = [row[c] if pd.notna(row[c]) else None for c in update_cols]
            update_vals.append(tx_id)
            cursor.execute(update_sql, update_vals)
            updated_count += 1
        else:
            # Build parameter values for INSERT: all columns in table order
            insert_vals = [row[c] if pd.notna(row[c]) else None for c in silver_cols]
            cursor.execute(insert_sql, insert_vals)
            inserted_count += 1
            
        merged_count += 1
        
    conn.commit()
    conn.close()
    
    log_event("SILVER_LAYER", f"Silver processing completed. Total input: {before_clean}.", "INFO", {
        "null_dropped": null_dropped,
        "duplicates_dropped": duplicates_dropped,
        "updates": updated_count,
        "inserts": inserted_count,
        "total_silver_rows": merged_count
    })
    return merged_count

def run_gold_layer():
    """
    Simulates Gold layer complete aggregation:
    - Reads Silver table
    - Groups by processed_flag
    - Computes totals, average, freshness, and flags high-value anomalies
    - Overwrites gold_kpi_metrics
    - Fires simulated notification alerts if anomalies are detected
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Read Silver data
    try:
        silver_df = pd.read_sql_query("SELECT * FROM silver_transactions", conn)
    except Exception as e:
        log_event("GOLD_LAYER", f"Silver table does not exist or is empty: {str(e)}")
        conn.close()
        return
        
    if silver_df.empty:
        log_event("GOLD_LAYER", "No data in Silver table. KPI calculation skipped.")
        conn.close()
        return
        
    # Group by processed_flag and aggregate
    total_records = len(silver_df)
    latest_freshness = silver_df["processed_time"].max()
    total_value = silver_df["amount"].sum()
    avg_value = silver_df["amount"].mean()
    
    # Anomaly Detection: transactions > $19.00
    anomaly_threshold = 19.0
    anomalous_df = silver_df[silver_df["amount"] > anomaly_threshold]
    anomaly_count = len(anomalous_df)
    
    # Overwrite Gold table (Complete mode simulation)
    cursor.execute("DELETE FROM gold_kpi_metrics")
    cursor.execute("""
        INSERT INTO gold_kpi_metrics (
            processed_flag, total_records_processed, latest_data_freshness,
            total_transaction_value, avg_transaction_value, anomalous_high_value_count
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (1, total_records, latest_freshness, float(total_value), float(avg_value), anomaly_count))
    
    conn.commit()
    conn.close()
    
    log_event("GOLD_LAYER", "Gold KPI metrics refreshed successfully.", "INFO", {
        "total_records": total_records,
        "freshness": latest_freshness,
        "total_value": round(total_value, 2),
        "avg_value": round(avg_value, 2),
        "anomalous_count": anomaly_count
    })
    
    # Trigger alerting hook simulation
    if anomaly_count > 0:
        log_event("SECURITY_ALERT", f"ALERT: Detected {anomaly_count} high-value anomalies above ${anomaly_threshold}!", "WARNING", {
            "anomalies": anomalous_df[["transaction_id", "user_id", "amount", "payment"]].to_dict(orient="records")
        })

def check_results():
    """
    Displays current contents of the warehouse layers for manual validation.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n" + "="*50)
    print("           LOCAL DATA WAREHOUSE PREVIEW")
    print("="*50)
    
    # File Offsets
    cursor.execute("SELECT * FROM offset_store")
    offsets = cursor.fetchall()
    print(f"\n[Auto Loader State Store] Processed Files ({len(offsets)}):")
    for file, processed_at in offsets:
        print(f" - {file} (processed at: {processed_at})")
        
    # Bronze count
    try:
        cursor.execute("SELECT COUNT(*) FROM bronze_transactions")
        bronze_count = cursor.fetchone()[0]
        print(f"\n[Bronze Layer] Total Raw Records: {bronze_count}")
    except Exception:
        print("\n[Bronze Layer] Table is empty or does not exist.")
        
    # Silver preview
    try:
        cursor.execute("SELECT COUNT(*) FROM silver_transactions")
        silver_count = cursor.fetchone()[0]
        print(f"[Silver Layer] Total Cleaned/Merged Records: {silver_count}")
        
        print("\n[Silver Layer] Top 5 Transactions Sample:")
        cursor.execute("SELECT transaction_id, user_id, amount, transaction_time, payment, processed_time FROM silver_transactions LIMIT 5")
        rows = cursor.fetchall()
        for row in rows:
            print(f" - ID: {row[0]}, User: {row[1]}, Amt: ${row[2]}, Time: {row[3]}, Method: {row[4]}")
    except Exception as e:
        print(f"[Silver Layer] Preview error: {e}")
        
    # Gold metrics
    try:
        cursor.execute("SELECT * FROM gold_kpi_metrics")
        gold = cursor.fetchone()
        if gold:
            print(f"\n[Gold Layer] KPI Aggregate Statistics:")
            print(f" - Total Records Processed: {gold[1]}")
            print(f" - Latest Freshness Time:   {gold[2]}")
            print(f" - Total Transaction Value: ${round(gold[3], 2)}")
            print(f" - Average Transaction Size: ${round(gold[4], 2)}")
            print(f" - High-Value Anomalies:     {gold[5]}")
    except Exception as e:
         print(f"[Gold Layer] Preview error: {e}")
         
    print("="*50 + "\n")
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sentinel Local Medallion Pipeline")
    parser.add_argument("--run", action="store_true", help="Execute the Medallion ingestion pipeline (Bronze -> Silver -> Gold)")
    parser.add_argument("--status", action="store_true", help="Display warehouse layer summaries")
    parser.add_argument("--clean", action="store_true", help="Delete the local database warehouse file")
    
    args = parser.parse_args()
    
    if args.clean:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
            log_event("DATABASE_CLEAN", f"Deleted local database {DB_FILE}")
        else:
            print("No database file found to clean.")
    elif args.run:
        create_directory_structure(os.path.dirname(os.path.abspath(__file__)))
        setup_local_database()
        
        log_event("PIPELINE_RUN", "Starting Sentinel Local Pipeline execution...")
        ingested = run_bronze_layer()
        if ingested > 0 or True: # execute downstream always to pick up any changes
            run_silver_layer()
            run_gold_layer()
        log_event("PIPELINE_RUN", "Sentinel Local Pipeline execution complete.")
        check_results()
    elif args.status:
        check_results()
    else:
        parser.print_help()
