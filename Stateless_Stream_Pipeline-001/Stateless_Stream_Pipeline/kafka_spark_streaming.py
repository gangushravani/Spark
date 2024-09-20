from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("KafkaReader") \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define your schema
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

print("Read Stream")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-4j8dq.southeastasia.azure.confluent.cloud:9092") \
    .option("subscribe", "user_data_topic") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f"org.apache.kafka.common.security.plain.PlainLoginModule required username='7TUSIFBKFOX4UR3P' password='Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/';") \
    .load()

# Convert the data and filter
data = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value").cast("string"), schema).alias("data")) \
  .select("data.*") \
  .filter(col("age") > 25)
print("Dataframe prepared")

checkpoint_dir = "/tmp/checkpoint-dir/dir_new1"

# Start streaming and print to console
query = data \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()
print("Write successfull")

query.awaitTermination()