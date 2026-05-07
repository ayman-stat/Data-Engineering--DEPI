import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# =========================================================
# 1. Load Environment Variables
# =========================================================

load_dotenv("../../.env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "live-ecommerce-orders")

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:depisecret@localhost:27017/"
)

print("🚀 Initializing PySpark Session...")

# =========================================================
# 2. Create Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("EcommerceLiveConsumer")

    # Kafka + MongoDB packages
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
        ])
    )

    # MongoDB connection
    .config("spark.mongodb.connection.uri", MONGO_URI)

    # Optional performance configs
    .config("spark.sql.shuffle.partitions", "2")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session Created Successfully")

# =========================================================
# 3. Define JSON Schema
# =========================================================

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("live_purchase_timestamp", StringType(), True)
])

print(f"🔄 Connecting to Kafka Topic: {TOPIC_NAME}")

# =========================================================
# 4. Read Stream From Kafka
# =========================================================

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

# =========================================================
# 5. Parse Kafka JSON
# =========================================================

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_payload")
    .withColumn(
        "data",
        from_json(col("json_payload"), order_schema)
    )
    .select("data.*")
)

print("💾 Writing Streaming Data To MongoDB...")

# =========================================================
# 6. Write Stream To MongoDB
# =========================================================

query = (
    parsed_df.writeStream
    .format("mongodb")

    # Important
    .option(
        "checkpointLocation",
        "D:/DEPI_Data/checkpoints/live_orders"
    )

    .option("database", "ecommerce_db")
    .option("collection", "live_orders")

    .outputMode("append")

    .start()
)

print("✅ Streaming Started Successfully")
print("📡 Listening for live Kafka messages...")

# =========================================================
# 7. Keep Stream Running
# =========================================================

query.awaitTermination()