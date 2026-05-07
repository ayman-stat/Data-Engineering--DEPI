import kagglehub
import os
import shutil

# Define the local data directory
DATA_DIR = "../../data"
os.makedirs(DATA_DIR, exist_ok=True)

print("🚀 Downloading dataset directly from Kaggle...")

# 1. Download the entire dataset folder at once (bypasses Pandas entirely!)
# This returns the path to the temporary folder where Kaggle downloaded the files
dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print(f"✅ Download complete. Extracting from: {dataset_path}")

# The 9 files we need
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

print("\n📂 Moving files into your /data folder...")

# 2. Copy the raw files natively (zero data corruption or parsing errors)
for file_name in file_names:
    source = os.path.join(dataset_path, file_name)
    destination = os.path.join(DATA_DIR, file_name)
    
    try:
        shutil.copy2(source, destination)
        print(f"✅ Successfully copied {file_name}")
    except FileNotFoundError:
        print(f"⚠️ Warning: Could not find {file_name} in the downloaded dataset.")

print("\n🎉 All datasets successfully ingested into the /data folder!")