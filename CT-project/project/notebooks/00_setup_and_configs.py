
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("base_path", "abfss://delta-forge@deltaforge.dfs.core.windows.net/")
dbutils.widgets.text("database_name", "legacy_hms_db")


# Retrieve widget values
env = dbutils.widgets.get("environment")
base_path = dbutils.widgets.get("base_path").strip().rstrip("/")
database_name = dbutils.widgets.get("database_name")


raw_landing_path = f"{base_path}/raw_landing"
bronze_path = f"{base_path}/bronze/transactions"
silver_path = f"{base_path}/silver/transactions"
checkpoint_base = f"{base_path}/checkpoints"

# Define structured checkpoints
bronze_chkpt = f"{checkpoint_base}/bronze_chkpt"
bronze_schema = f"{checkpoint_base}/bronze_schema"
silver_chkpt = f"{checkpoint_base}/silver_chkpt"
gold_chkpt = f"{checkpoint_base}/gold_chkpt"

# Apply Spark configurations for optimization and schema safety
try:
    spark.conf.set("spark.sql.adaptive.enabled", "true")
except Exception as e:
    print(f"Warning: Could not set spark.sql.adaptive.enabled: {e}")

try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
except Exception as e:
    print(f"Warning: Could not set spark.databricks.delta.schema.autoMerge.enabled: {e}")

# Register and select target database in Hive Metastore
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

print(f"--- Sentinel Setup Complete ---")
print(f"Environment: {env}")
print(f"Base storage path: {base_path}")
print(f"Target Database: {database_name}")
print(f"Bronze raw landing: {raw_landing_path}")
