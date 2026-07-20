

from pyspark.sql.functions import current_timestamp, col


bronze_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", bronze_schema)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(raw_landing_path)
    .withColumn("file_name", col("_metadata.file_path")) 
    .withColumn("file_arrival_time", col("_metadata.file_modification_time"))
    .withColumn("ingestion_time", current_timestamp())
)


query = (bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_chkpt)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{database_name}.bronze_transactions")
)

query.awaitTermination()
print(f"Bronze ingestion streaming batch completed successfully.")
