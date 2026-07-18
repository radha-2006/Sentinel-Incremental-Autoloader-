
from pyspark.sql.functions import current_timestamp, lit, col

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.silver_transactions (
        transaction_id INT,
        user_id INT,
        amount FLOAT,
        transaction_time TIMESTAMP,
        ingestion_time TIMESTAMP,
        file_name STRING,
        file_arrival_time TIMESTAMP,
        processed_time TIMESTAMP,
        processed_flag INT,
        product_id STRING,
        payment STRING,
        credit_card STRING,
        loyalty_card STRING,
        cost FLOAT,
        quantity INT
    ) USING DELTA
""")

def process_silver_microbatch(microBatchDF, batchId):
    # Re-fetch active spark session from dataframe metadata
    spark_session = microBatchDF.sparkSession
    
    # Schema alignment: map incoming CSV columns to standard Silver properties
    cols = microBatchDF.columns
    df_transformed = microBatchDF
    
    if "customer_id" in cols and "user_id" not in cols:
        df_transformed = df_transformed.withColumnRenamed("customer_id", "user_id")
    if "price" in cols and "amount" not in cols:
        df_transformed = df_transformed.withColumnRenamed("price", "amount")
    if "transactional_date" in cols and "transaction_time" not in cols:
        df_transformed = df_transformed.withColumnRenamed("transactional_date", "transaction_time")
        
    # Enforce quality: drop rows with missing business keys
    # Deduplicate: collapse duplicate transaction keys within the microbatch
    cleaned_df = (df_transformed
        .dropna(subset=["transaction_id", "amount"])
        .dropDuplicates(["transaction_id"])
        .withColumn("processed_time", current_timestamp())
        .withColumn("processed_flag", lit(1))
    )
    
    # Register microbatch as a local temporary view
    cleaned_df.createOrReplaceTempView("silver_updates")
    
    # Execute direct SQL MERGE (more robust than older PySpark Java API wrappers)
    spark_session.sql(f"""
        MERGE INTO {database_name}.silver_transactions AS target
        USING silver_updates AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.user_id = source.user_id,
                target.amount = source.amount,
                target.transaction_time = source.transaction_time,
                target.ingestion_time = source.ingestion_time,
                target.file_name = source.file_name,
                target.file_arrival_time = source.file_arrival_time,
                target.processed_time = source.processed_time,
                target.processed_flag = source.processed_flag,
                target.product_id = source.product_id,
                target.payment = source.payment,
                target.credit_card = source.credit_card,
                target.loyalty_card = source.loyalty_card,
                target.cost = source.cost,
                target.quantity = source.quantity
        WHEN NOT MATCHED THEN 
            INSERT (
                transaction_id, user_id, amount, transaction_time, ingestion_time, 
                file_name, file_arrival_time, processed_time, processed_flag,
                product_id, payment, credit_card, loyalty_card, cost, quantity
            ) VALUES (
                source.transaction_id, source.user_id, source.amount, source.transaction_time, source.ingestion_time, 
                source.file_name, source.file_arrival_time, source.processed_time, source.processed_flag,
                source.product_id, source.payment, source.credit_card, source.loyalty_card, source.cost, source.quantity
            )
    """)


silver_query = (spark.readStream
    .table(f"{database_name}.bronze_transactions")
    .writeStream
    .foreachBatch(process_silver_microbatch)
    .option("checkpointLocation", silver_chkpt)
    .trigger(availableNow=True)
    .start()
)

silver_query.awaitTermination()
print("Silver cleansing and merge streaming batch completed successfully.")
