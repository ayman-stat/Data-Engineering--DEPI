import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CSV_FILE_PATH = PROJECT_ROOT / "data" / "olist_orders_dataset.csv"

load_dotenv(PROJECT_ROOT / ".env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "live-ecommerce-orders")

print(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    print("Successfully connected to Kafka.")
except Exception as exc:
    print(f"Failed to connect to Kafka: {exc}")
    raise SystemExit(1)

print(f"Loading dataset from {CSV_FILE_PATH}...")
try:
    df = pd.read_csv(CSV_FILE_PATH)
    df = df.where(pd.notnull(df), None)
    print(f"Loaded {len(df)} orders.")
except FileNotFoundError:
    print(f"CSV file not found at {CSV_FILE_PATH}. Did you run the ingestion script?")
    raise SystemExit(1)

print(f"Starting live stream to topic '{TOPIC_NAME}'. Press Ctrl+C to stop.")
try:
    for _, row in df.iterrows():
        order_data = row.to_dict()
        order_data["live_purchase_timestamp"] = datetime.utcnow().isoformat()

        producer.send(TOPIC_NAME, value=order_data)
        print(
            f"Sent order: {order_data['order_id']} | "
            f"Time: {order_data['live_purchase_timestamp']}"
        )

        time.sleep(1)
except KeyboardInterrupt:
    print("\nStreaming stopped by user.")
finally:
    producer.flush()
    producer.close()
    print("Kafka producer closed cleanly.")
