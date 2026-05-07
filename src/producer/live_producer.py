import os
import json
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

# 1. Load environment variables from the .env file
# We use '../../.env' because we are running this from src/producer/
load_dotenv('../../.env')

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'live-ecommerce-orders')
CSV_FILE_PATH = '../../data/olist_orders_dataset.csv'

# 2. Connect to Kafka
print(f"🔄 Connecting to Kafka Broker at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Successfully connected to Kafka!")
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    exit()

# 3. Load the dataset
print(f"📥 Loading dataset from {CSV_FILE_PATH}...")
try:
    df = pd.read_csv(CSV_FILE_PATH)
    # Fill NaN values with None so JSON serialization works smoothly
    df = df.where(pd.notnull(df), None) 
    print(f"✅ Loaded {len(df)} orders.")
except FileNotFoundError:
    print(f"❌ CSV file not found at {CSV_FILE_PATH}! Did you run the ingestion script?")
    exit()

# 4. Simulate Live Streaming
print(f"🚀 Starting live stream to topic '{TOPIC_NAME}'... Press Ctrl+C to stop.")
try:
    for index, row in df.iterrows():
        # Convert row to dictionary
        order_data = row.to_dict()
        
        # Inject current timestamp to simulate live purchase
        order_data['live_purchase_timestamp'] = datetime.utcnow().isoformat()
        
        # Fire it into Kafka!
        producer.send(TOPIC_NAME, value=order_data)
        
        # Print to terminal so you can watch the magic
        print(f"📦 Sent Order: {order_data['order_id']} | Time: {order_data['live_purchase_timestamp']}")
        
        # Wait 1 second before sending the next one
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n🛑 Streaming stopped by user.")
finally:
    # Ensure all messages are sent before closing
    producer.flush()
    producer.close()
    print("🔌 Kafka producer closed cleanly.")