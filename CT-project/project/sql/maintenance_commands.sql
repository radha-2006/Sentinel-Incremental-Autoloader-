
-- 1. Delta File Compaction (Bin-packing small files into larger ones)
-- This solves the "small-file problem" by consolidating delta records.
OPTIMIZE legacy_hms_db.bronze_transactions;

-- 2. Z-Ordering compaction on primary keys
-- Co-locates column information based on transaction_id to optimize MERGE lookups.
OPTIMIZE legacy_hms_db.silver_transactions 
ZORDER BY (transaction_id);

-- 3. Delta Purging (Removing old unreferenced data files older than 7 days)
-- Caution: Do not set RETAIN below 168 hours to avoid read query failures.
VACUUM legacy_hms_db.bronze_transactions RETAIN 168 HOURS;
VACUUM legacy_hms_db.silver_transactions RETAIN 168 HOURS;

-- 4. Refresh Statistics
-- Informs the Spark Catalyst Optimizer of statistics for faster plan generation.
ANALYZE TABLE legacy_hms_db.silver_transactions 
COMPUTE STATISTICS FOR ALL COLUMNS;
