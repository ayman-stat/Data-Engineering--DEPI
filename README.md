# Real-Time E-Commerce Data Engineering Pipeline

This project is a complete local data engineering pipeline for real-time e-commerce analytics using the Olist Brazilian E-Commerce dataset. It simulates live customer orders, streams them through Kafka, processes them with PySpark Structured Streaming, stores data in a Bronze/Silver/Gold lakehouse layout, enriches business data for analytics, writes cleaned records to MongoDB, and adds an ML forecasting layer for future order demand.

The project is designed for a DEPI graduation/demo setting: it is practical enough to run on a laptop, but structured like a professional data platform.

## Project Purpose

The goal is to help business users and mentors answer:

- How many orders are arriving live per minute?
- What is the current order status distribution?
- Which states generate the most revenue?
- Which product categories perform best?
- Can we forecast future daily order volume?

Business story:

> We built a real-time e-commerce analytics platform that simulates live purchases, processes them instantly, enriches them with customer and product context, stores them in analytics-ready lakehouse layers, and produces both operational dashboards and predictive insights.

## Architecture

```text
Kaggle Olist CSV Files
        |
        v
Python Kafka Producer
        |
        v
Kafka Topic: live-ecommerce-orders
        |
        v
PySpark Structured Streaming Consumer
        |
        +--> Bronze Layer: raw Kafka payloads
        |
        +--> Silver Layer: cleaned and typed live orders
        |
        +--> Gold Layer: BI-ready aggregations
        |       - orders per minute
        |       - order status counts
        |       - revenue by state
        |       - top product categories
        |
        +--> MongoDB: cleaned live order sink
        |
        v
Power BI Dashboard + ML Forecasting Layer
```

## Tech Stack

| Component | Tool |
| --- | --- |
| Dataset | Kaggle Olist Brazilian E-Commerce |
| Streaming broker | Apache Kafka |
| Stream processing | PySpark Structured Streaming |
| Data lake | Local Parquet lakehouse |
| NoSQL sink | MongoDB |
| Container runtime | Docker Compose |
| BI target | Power BI |
| ML layer | pandas + scikit-learn |

## Repository Hierarchy

```text
.
|-- docker/
|   `-- docker-compose.yml          # Kafka and MongoDB services
|
|-- src/
|   |-- producer/
|   |   |-- ingest_data.py          # Downloads/copies Kaggle Olist CSVs
|   |   `-- live_producer.py        # Sends simulated live orders to Kafka
|   |
|   |-- consumer/
|   |   `-- spark_consumer.py       # Streaming Bronze/Silver/Gold + MongoDB sink
|   |
|   `-- ml/
|       `-- forecast_daily_orders.py # Daily order forecasting model
|
|-- data/                           # Local raw CSV files, ignored by git
|-- data_lake/
|   |-- bronze/                     # Raw Kafka records as parquet
|   |-- silver/                     # Cleaned order records as parquet
|   `-- gold/                       # BI-ready parquet tables
|
|-- checkpoints/                    # Spark streaming checkpoints, ignored by git
|-- ml_outputs/                     # Forecast CSV/JSON outputs, ignored by git
|-- notebooks/                      # Optional notebooks for exploration/demo
|-- requirements.txt
|-- .gitignore
`-- README.md
```

## Data Lake Layers

### Bronze Layer

Location:

```text
data_lake/bronze/live_orders
```

Purpose:

- Store raw Kafka message payloads.
- Preserve topic, partition, offset, timestamp, key, and JSON payload.
- Enable replay, debugging, and auditability.

### Silver Layer

Location:

```text
data_lake/silver/live_orders
```

Purpose:

- Parse JSON into structured columns.
- Convert purchase timestamps.
- Remove invalid rows with null `order_id`.
- Deduplicate orders for clean downstream storage.

### Gold Layer

Locations:

```text
data_lake/gold/orders_per_minute
data_lake/gold/order_status_counts
data_lake/gold/revenue_by_state
data_lake/gold/top_products
```

Purpose:

- Provide small BI-ready parquet tables.
- Support Power BI dashboards directly.
- Answer operational and business questions without requiring Power BI to process raw streaming data.

## Gold Tables

| Table | Purpose |
| --- | --- |
| `orders_per_minute` | Live order throughput monitoring |
| `order_status_counts` | Operational status distribution |
| `revenue_by_state` | Geographic revenue analysis |
| `top_products` | Product category performance |

## ML Layer

Script:

```text
src/ml/forecast_daily_orders.py
```

The ML layer trains a daily order forecasting model using historical Olist orders. It creates time-based features, lag features, rolling averages, and revenue signals, then trains a `RandomForestRegressor` to forecast daily order volume.

Generated outputs:

```text
ml_outputs/daily_orders_forecast.csv
ml_outputs/daily_orders_metrics.json
```

These files are ignored by git because they are reproducible runtime outputs.

Run:

```powershell
python src\ml\forecast_daily_orders.py
```

Use in Power BI:

- Import `ml_outputs/daily_orders_forecast.csv`.
- Plot `date` vs `orders_count` and `predicted_orders`.
- Filter by `record_type` to separate backtest and future forecast records.

## Setup

Create and activate a virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create `.env` in the repository root:

```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=live-ecommerce-orders
MONGO_URI=mongodb://localhost:27017/
MONGO_DB_NAME=ecommerce_db
MONGO_COLLECTION=live_orders
```

## Run the Full Pipeline

Start Kafka and MongoDB:

```powershell
docker compose -f docker\docker-compose.yml up -d
```

Download the Olist dataset:

```powershell
python src\producer\ingest_data.py
```

Start the Spark streaming consumer:

```powershell
python src\consumer\spark_consumer.py
```

In another terminal, start the live producer:

```powershell
python src\producer\live_producer.py
```

For a controlled demo, send only a limited number of orders:

```powershell
python src\producer\live_producer.py --limit 100 --delay-seconds 0.25
```

Run the ML forecast:

```powershell
python src\ml\forecast_daily_orders.py
```

## Demo Scripts

Use this script when you want the numbers to refresh for Power BI:

```powershell
.\scripts\start_demo_pipeline.ps1 -Limit 100 -DelaySeconds 0.25
```

What it does:

- Starts Kafka and MongoDB with Docker Compose.
- Downloads the dataset if it is missing.
- Starts the Spark streaming consumer in the background.
- Sends a controlled batch of live orders to Kafka.
- Waits for Spark micro-batches to update Gold parquet outputs.
- Refreshes the ML forecast outputs.

For a clean presentation run, reset generated lake/checkpoint outputs first:

```powershell
.\scripts\start_demo_pipeline.ps1 -CleanRun -Limit 100 -DelaySeconds 0.25
```

Stop the background Spark consumer after the demo:

```powershell
.\scripts\stop_demo_pipeline.ps1
```

Stop Spark and Docker services:

```powershell
.\scripts\stop_demo_pipeline.ps1 -StopDocker
```

Refresh only the ML forecast:

```powershell
.\scripts\refresh_ml.ps1
```

## Verification Commands

Check Docker services:

```powershell
docker compose -f docker\docker-compose.yml ps
```

Check data lake files:

```powershell
dir data_lake\bronze\live_orders
dir data_lake\silver\live_orders
dir data_lake\gold\orders_per_minute
dir data_lake\gold\order_status_counts
dir data_lake\gold\revenue_by_state
dir data_lake\gold\top_products
```

Check MongoDB count:

```powershell
docker exec mongodb mongosh --quiet --eval "db.getSiblingDB('ecommerce_db').live_orders.countDocuments()"
```

Check ML outputs:

```powershell
dir ml_outputs
type ml_outputs\daily_orders_metrics.json
```

## Power BI Dashboard Plan

The Power BI report file is stored here:

```text
powerbi/ecommerce_realtime_dashboard.pbix
```

Clean Power Query M code is documented here:

```text
powerbi/power_query_m_code.md
```

After running the demo pipeline script, open Power BI and click:

```text
Home -> Refresh
```

Recommended pages:

1. Live Operations
   - orders per minute line chart
   - order status bar chart
   - total live orders card

2. Revenue Geography
   - revenue by state map or filled map
   - total revenue card
   - top states table

3. Product Performance
   - top product categories by revenue
   - product category table

4. Forecasting
   - historical daily orders vs predicted orders
   - future 14-day forecast
   - model MAE/RMSE cards from `daily_orders_metrics.json`

## Demo Script

1. Start Docker Compose and show Kafka/MongoDB are running.
2. Start the Spark consumer and explain Bronze/Silver/Gold.
3. Start the Kafka producer and show orders being sent live.
4. Open the data lake folders and show parquet files appearing.
5. Show MongoDB document count increasing.
6. Open Power BI dashboard connected to Gold tables.
7. Run the ML script and show forecast outputs.

## Current Verified Smoke Test

The pipeline was smoke-tested locally with 20 Kafka records:

```text
Bronze: 20 rows
Silver: 20 rows
Gold orders_per_minute: 1 row, 20 orders
Gold order_status_counts: delivered=19, invoiced=1
Gold revenue_by_state: 9 rows
Gold top_products: 13 rows
MongoDB live_orders count: 571 documents
```

## Why This Project Is Professional

- Uses real streaming architecture with Kafka.
- Separates raw, cleaned, and business-ready data.
- Keeps replay/debug capability through Bronze storage.
- Provides dashboard-ready Gold outputs.
- Includes NoSQL storage with MongoDB.
- Adds predictive analytics through an ML layer.
- Runs locally with free tools and clear commands.

## Future Enhancements

- Add Airflow orchestration for batch ingestion and ML refresh.
- Add data quality checks for nulls, duplicate orders, and schema drift.
- Containerize the Spark consumer.
- Add Power BI screenshots to the repository.
- Add Delta Lake when the environment supports it reliably.
