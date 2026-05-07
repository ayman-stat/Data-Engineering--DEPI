import os
import shutil
from pathlib import Path

import kagglehub


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
os.makedirs(DATA_DIR, exist_ok=True)

print("Downloading dataset directly from Kaggle...")

dataset_path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
print(f"Download complete. Extracting from: {dataset_path}")

file_names = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

print("\nMoving files into your /data folder...")

for file_name in file_names:
    source = Path(dataset_path) / file_name
    destination = DATA_DIR / file_name

    try:
        shutil.copy2(source, destination)
        print(f"Successfully copied {file_name}")
    except FileNotFoundError:
        print(f"Warning: could not find {file_name} in the downloaded dataset.")

print("\nAll datasets successfully ingested into the /data folder.")
