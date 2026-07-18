

from pyspark.sql.functions import count, max, sum, avg, col, when


gold_metrics_df = (spark.readStream
    .table(f"{database_name}.silver_transactions")
    .groupBy("processed_flag")
    .agg(
        count("transaction_id").alias("total_records_processed"),
        max("processed_time").alias("latest_data_freshness"),
        sum("amount").alias("total_transaction_value"),
        avg("amount").alias("avg_transaction_value"),
        sum(when(col("amount") > 19.0, 1).otherwise(0)).alias("anomalous_high_value_count")
    )
)


gold_query = (gold_metrics_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", gold_chkpt)
    .trigger(availableNow=True)
    .toTable(f"{database_name}.gold_kpi_metrics")
)

gold_query.awaitTermination()
print("Gold KPI aggregation and streaming batch completed successfully.")
