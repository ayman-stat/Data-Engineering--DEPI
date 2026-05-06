import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd
import os

# Define the local data directory
DATA_DIR = "../../data"
os.makedirs(DATA_DIR, exist_ok=True)

# The 9 files inside the Olist Brazilian E-Commerce dataset
file_names = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv"
]

dataframes = {}

print("🚀 Starting Data Ingestion via Kaggle API...")

for file_name in file_names:
    print(f"📥 Fetching {file_name}...")
    
    # Load the latest version using the Pandas Adapter
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "olistbr/brazilian-ecommerce",
        file_name
    )
    
    # Store in our dictionary (in case you want to manipulate them immediately)
    dataframes[file_name] = df
    
    # Save a local copy to the /data folder so your Kafka Producer and PySpark can use it
    save_path = os.path.join(DATA_DIR, file_name)
    df.to_csv(save_path, index=False)
    print(f"✅ Saved to {save_path} (Records: {len(df)})")

print("\n🎉 All 9 datasets successfully ingested and saved to the /data folder!")

# Show a quick preview of the main orders dataset
print("\nPreview of olist_orders_dataset.csv:")
print(dataframes["olist_orders_dataset.csv"].head())