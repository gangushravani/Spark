from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OrdersPaymentsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10") \
    .config("spark.default.parallelism", "5") \
    .getOrCreate()

# Define the schema for orders and payments
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_date", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("amount", IntegerType(), True)
])

# Read orders from Kafka
orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-4j8dq.southeastasia.azure.confluent.cloud:9092") \
    .option("subscribe", "orders_topic_data") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='7TUSIFBKFOX4UR3P' password='Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("created_at", current_timestamp()) \
    .withWatermark("created_at", "30 minutes")\
    .repartition(5)
print("Orders stream read")

# Read payments from Kafka
payments_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-4j8dq.southeastasia.azure.confluent.cloud:9092") \
    .option("subscribe", "payments_topic_data") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='7TUSIFBKFOX4UR3P' password='Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), payment_schema).alias("data")) \
    .select("data.*") \
    .withColumn("created_at", current_timestamp()) \
    .withWatermark("created_at", "30 minutes")\
    .repartition(5)
print("Payments stream read")

# Join orders and payments on order_id with 30 minutes window
joined_df = orders_df.alias("orders").join(
    payments_df.alias("payments"),
    expr("""
        orders.order_id = payments.order_id AND
        orders.created_at <= payments.created_at + interval 30 minutes AND
        payments.created_at <= orders.created_at + interval 30 minutes
    """),
    "inner"
).select(
    col("orders.order_id"),
    col("orders.order_date"),
    col("orders.created_at").alias("order_created_at"),
    col("orders.customer_id"),
    col("orders.amount").alias("order_amount"),
    col("payments.payment_id"),
    col("payments.payment_date"),
    col("payments.created_at").alias("payment_created_at"),
    col("payments.amount").alias("payment_amount")
).repartition(5)

# Write the joined data to MongoDB
query = joined_df.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/mongo_db_checkpoint10") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://growdataskills:<password>@mongo-db-cluster-new.abc123.mongodb.net/?retryWrites=true&w=majority&appName=mongo-db-cluster-new") \
    .option('spark.mongodb.database', 'ecomm_mart') \
    .option('spark.mongodb.collection', 'orders_data_process_fact') \
    .option("truncate", False) \
    .start()
print("Write successfull")

query.awaitTermination()

# query = joined_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("checkpointLocation", "/tmp/mongo_db_checkpoint7") \
#     .start()

# query.awaitTermination()