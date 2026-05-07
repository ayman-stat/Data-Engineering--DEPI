import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, from_json, lit, sum as spark_sum, to_timestamp, window
from pyspark.sql.types import StringType, StructField, StructType


# =========================================================
# 1. Project Paths and Environment
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE_DIR = PROJECT_ROOT / "data_lake"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
DATA_DIR = PROJECT_ROOT / "data"

load_dotenv(PROJECT_ROOT / ".env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "live-ecommerce-orders")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", os.getenv("MONGO_DB_NAME", "ecommerce_db"))
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "live_orders")

BRONZE_PATH = DATA_LAKE_DIR / "bronze" / "live_orders"
SILVER_PATH = DATA_LAKE_DIR / "silver" / "live_orders"
GOLD_ORDERS_PER_MINUTE_PATH = DATA_LAKE_DIR / "gold" / "orders_per_minute"
GOLD_ORDER_STATUS_PATH = DATA_LAKE_DIR / "gold" / "order_status_counts"
GOLD_REVENUE_BY_STATE_PATH = DATA_LAKE_DIR / "gold" / "revenue_by_state"
GOLD_TOP_PRODUCTS_PATH = DATA_LAKE_DIR / "gold" / "top_products"

for layer_dir in [
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_ORDERS_PER_MINUTE_PATH,
    GOLD_ORDER_STATUS_PATH,
    GOLD_REVENUE_BY_STATE_PATH,
    GOLD_TOP_PRODUCTS_PATH,
    CHECKPOINT_DIR / "bronze_live_orders",
    CHECKPOINT_DIR / "silver_live_orders",
    CHECKPOINT_DIR / "gold_orders_per_minute",
    CHECKPOINT_DIR / "gold_order_status_counts",
    CHECKPOINT_DIR / "gold_revenue_by_state",
    CHECKPOINT_DIR / "gold_top_products",
    CHECKPOINT_DIR / "mongo_live_orders",
]:
    layer_dir.mkdir(parents=True, exist_ok=True)

print("Initializing PySpark session...")

# =========================================================
# 2. Create Spark Session
# =========================================================

spark = (
    SparkSession.builder
    .appName("EcommerceLiveConsumer")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
            ]
        ),
    )
    .config("spark.mongodb.connection.uri", MONGO_URI)
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("Spark session created successfully.")

# =========================================================
# 3. Load Static Dimension Data
# =========================================================

customers_path = DATA_DIR / "olist_customers_dataset.csv"
order_items_path = DATA_DIR / "olist_order_items_dataset.csv"
products_path = DATA_DIR / "olist_products_dataset.csv"
translation_path = DATA_DIR / "product_category_name_translation.csv"

static_dimensions_available = all(
    path.exists() for path in [customers_path, order_items_path, products_path]
)

if static_dimensions_available:
    customers_df = (
        spark.read
        .option("header", True)
        .csv(str(customers_path))
        .select("customer_id", "customer_city", "customer_state")
    )

    order_items_df = (
        spark.read
        .option("header", True)
        .csv(str(order_items_path))
        .select(
            "order_id",
            "product_id",
            "seller_id",
            col("price").cast("double").alias("price"),
            col("freight_value").cast("double").alias("freight_value"),
        )
    )

    products_df = (
        spark.read
        .option("header", True)
        .csv(str(products_path))
        .select("product_id", "product_category_name")
    )

    if translation_path.exists():
        translation_df = (
            spark.read
            .option("header", True)
            .csv(str(translation_path))
        )

        products_df = (
            products_df
            .join(translation_df, on="product_category_name", how="left")
            .withColumn(
                "product_category",
                coalesce(
                    col("product_category_name_english"),
                    col("product_category_name"),
                ),
            )
            .select("product_id", "product_category")
        )
    else:
        products_df = products_df.withColumn(
            "product_category",
            col("product_category_name"),
        ).select("product_id", "product_category")

    print("Static Olist dimensions loaded for enrichment.")
else:
    customers_df = None
    order_items_df = None
    products_df = None
    print("Static dimension CSV files not found. Enrichment Gold tables will be skipped.")

# =========================================================
# 4. Define JSON Schema
# =========================================================

order_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
        StructField("live_purchase_timestamp", StringType(), True),
    ]
)

print(f"Connecting to Kafka topic: {TOPIC_NAME}")

# =========================================================
# 5. Read Stream From Kafka
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
# 6. Bronze Layer - Raw Kafka Payload
# =========================================================

bronze_df = raw_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_payload"),
)

bronze_query = (
    bronze_df.writeStream
    .format("parquet")
    .option("path", str(BRONZE_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "bronze_live_orders"))
    .outputMode("append")
    .start()
)

print("Bronze layer started.")

# =========================================================
# 7. Parse Kafka JSON
# =========================================================

parsed_df = (
    bronze_df
    .withColumn("data", from_json(col("json_payload"), order_schema))
    .select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "data.*",
    )
)

# =========================================================
# 8. Silver Layer - Cleaned Orders
# =========================================================

clean_orders_df = (
    parsed_df
    .filter(col("order_id").isNotNull())
    .withColumn("purchase_ts", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("live_purchase_ts", to_timestamp(col("live_purchase_timestamp")))
    .withColumn("event_ts", coalesce(col("live_purchase_ts"), col("purchase_ts")))
    .withWatermark("event_ts", "10 minutes")
)

silver_df = (
    clean_orders_df
    .dropDuplicates(["order_id"])
)

silver_query = (
    silver_df.writeStream
    .format("parquet")
    .option("path", str(SILVER_PATH))
    .option("checkpointLocation", str(CHECKPOINT_DIR / "silver_live_orders"))
    .outputMode("append")
    .start()
)

print("Silver layer started.")

# =========================================================
# 9. Gold Layer - Orders Per Minute
# =========================================================

gold_orders_per_minute_df = (
    clean_orders_df
    .filter(col("event_ts").isNotNull())
    .groupBy(window(col("event_ts"), "1 minute"))
    .count()
    .select(
        col("window.start").alias("minute_start"),
        col("window.end").alias("minute_end"),
        col("count").alias("orders_count"),
    )
)


def write_gold_orders_per_minute(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    (
        batch_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(GOLD_ORDERS_PER_MINUTE_PATH))
    )

    print(f"Gold orders_per_minute snapshot updated from batch {batch_id}.")


gold_query = (
    gold_orders_per_minute_df.writeStream
    .foreachBatch(write_gold_orders_per_minute)
    .option("checkpointLocation", str(CHECKPOINT_DIR / "gold_orders_per_minute"))
    .outputMode("complete")
    .start()
)

print("Gold layer started.")

# =========================================================
# 10. Gold Layer - Order Status Counts
# =========================================================


def write_snapshot(batch_df, batch_id, output_path, table_name):
    if batch_df.isEmpty():
        return

    (
        batch_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Gold {table_name} snapshot updated from batch {batch_id}.")


order_status_counts_df = (
    clean_orders_df
    .filter(col("order_status").isNotNull())
    .groupBy("order_status")
    .count()
    .select(
        "order_status",
        col("count").alias("orders_count"),
    )
)

order_status_query = (
    order_status_counts_df.writeStream
    .foreachBatch(
        lambda batch_df, batch_id: write_snapshot(
            batch_df,
            batch_id,
            GOLD_ORDER_STATUS_PATH,
            "order_status_counts",
        )
    )
    .option("checkpointLocation", str(CHECKPOINT_DIR / "gold_order_status_counts"))
    .outputMode("complete")
    .start()
)

print("Gold order status table started.")

# =========================================================
# 11. Gold Layer - Enriched Business Aggregates
# =========================================================


if static_dimensions_available:
    enriched_order_items_df = (
        clean_orders_df
        .join(order_items_df, on="order_id", how="left")
        .join(customers_df, on="customer_id", how="left")
        .join(products_df, on="product_id", how="left")
        .withColumn(
            "line_revenue",
            coalesce(col("price"), lit(0.0)) + coalesce(col("freight_value"), lit(0.0)),
        )
    )

    revenue_by_state_df = (
        enriched_order_items_df
        .filter(col("customer_state").isNotNull())
        .groupBy("customer_state")
        .agg(
            spark_sum("price").alias("product_revenue"),
            spark_sum("freight_value").alias("freight_revenue"),
            spark_sum("line_revenue").alias("total_revenue"),
        )
    )

    top_products_df = (
        enriched_order_items_df
        .filter(col("product_category").isNotNull())
        .groupBy("product_category")
        .agg(
            spark_sum("price").alias("product_revenue"),
            spark_sum("line_revenue").alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
    )

    revenue_by_state_query = (
        revenue_by_state_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: write_snapshot(
                batch_df,
                batch_id,
                GOLD_REVENUE_BY_STATE_PATH,
                "revenue_by_state",
            )
        )
        .option("checkpointLocation", str(CHECKPOINT_DIR / "gold_revenue_by_state"))
        .outputMode("complete")
        .start()
    )

    top_products_query = (
        top_products_df.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: write_snapshot(
                batch_df,
                batch_id,
                GOLD_TOP_PRODUCTS_PATH,
                "top_products",
            )
        )
        .option("checkpointLocation", str(CHECKPOINT_DIR / "gold_top_products"))
        .outputMode("complete")
        .start()
    )

    print("Enriched Gold tables started.")

# =========================================================
# 12. MongoDB Sink - Cleaned Live Orders
# =========================================================

mongo_query = (
    silver_df.writeStream
    .format("mongodb")
    .option("checkpointLocation", str(CHECKPOINT_DIR / "mongo_live_orders"))
    .option("database", MONGO_DATABASE)
    .option("collection", MONGO_COLLECTION)
    .outputMode("append")
    .start()
)

print("MongoDB sink started.")
print("Streaming pipeline is listening for live Kafka messages...")

# =========================================================
# 13. Keep Streams Running
# =========================================================

spark.streams.awaitAnyTermination()
