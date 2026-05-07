import json
from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "ml_outputs"

ORDERS_PATH = DATA_DIR / "olist_orders_dataset.csv"
ORDER_ITEMS_PATH = DATA_DIR / "olist_order_items_dataset.csv"

FORECAST_PATH = OUTPUT_DIR / "daily_orders_forecast.csv"
METRICS_PATH = OUTPUT_DIR / "daily_orders_metrics.json"


def load_daily_orders() -> pd.DataFrame:
    if not ORDERS_PATH.exists():
        raise FileNotFoundError(
            f"Missing {ORDERS_PATH}. Run src/producer/ingest_data.py first."
        )

    orders = pd.read_csv(
        ORDERS_PATH,
        usecols=["order_id", "order_status", "order_purchase_timestamp"],
    )
    orders["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"],
        errors="coerce",
    )
    orders = orders.dropna(subset=["order_purchase_timestamp"])
    orders["order_date"] = orders["order_purchase_timestamp"].dt.date

    daily = (
        orders
        .groupby("order_date")
        .agg(
            orders_count=("order_id", "nunique"),
            delivered_orders=(
                "order_status",
                lambda values: (values == "delivered").sum(),
            ),
        )
        .reset_index()
    )

    if ORDER_ITEMS_PATH.exists():
        order_items = pd.read_csv(
            ORDER_ITEMS_PATH,
            usecols=["order_id", "price", "freight_value"],
        )
        order_items["line_revenue"] = (
            order_items["price"].fillna(0) + order_items["freight_value"].fillna(0)
        )
        order_revenue = order_items.groupby("order_id", as_index=False)["line_revenue"].sum()
        orders_with_revenue = orders[["order_id", "order_date"]].merge(
            order_revenue,
            on="order_id",
            how="left",
        )
        daily_revenue = (
            orders_with_revenue
            .groupby("order_date", as_index=False)["line_revenue"]
            .sum()
            .rename(columns={"line_revenue": "total_revenue"})
        )
        daily = daily.merge(daily_revenue, on="order_date", how="left")
    else:
        daily["total_revenue"] = 0.0

    daily["order_date"] = pd.to_datetime(daily["order_date"])
    daily = daily.sort_values("order_date").reset_index(drop=True)
    return daily


def add_time_features(daily: pd.DataFrame) -> pd.DataFrame:
    featured = daily.copy()
    featured["day_index"] = (featured["order_date"] - featured["order_date"].min()).dt.days
    featured["day_of_week"] = featured["order_date"].dt.dayofweek
    featured["day_of_month"] = featured["order_date"].dt.day
    featured["month"] = featured["order_date"].dt.month
    featured["is_weekend"] = featured["day_of_week"].isin([5, 6]).astype(int)
    featured["orders_lag_1"] = featured["orders_count"].shift(1)
    featured["orders_lag_7"] = featured["orders_count"].shift(7)
    featured["orders_rolling_7"] = featured["orders_count"].shift(1).rolling(7).mean()
    featured["revenue_lag_1"] = featured["total_revenue"].shift(1)
    featured = featured.dropna().reset_index(drop=True)
    return featured


def train_forecast_model(featured: pd.DataFrame):
    feature_columns = [
        "day_index",
        "day_of_week",
        "day_of_month",
        "month",
        "is_weekend",
        "orders_lag_1",
        "orders_lag_7",
        "orders_rolling_7",
        "revenue_lag_1",
    ]

    split_index = int(len(featured) * 0.8)
    train_df = featured.iloc[:split_index]
    test_df = featured.iloc[split_index:]

    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        min_samples_leaf=2,
        n_jobs=-1,
    )
    model.fit(train_df[feature_columns], train_df["orders_count"])

    predictions = model.predict(test_df[feature_columns])
    metrics = {
        "training_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "mae": round(float(mean_absolute_error(test_df["orders_count"], predictions)), 3),
        "rmse": round(
            float(mean_squared_error(test_df["orders_count"], predictions) ** 0.5),
            3,
        ),
        "r2": round(float(r2_score(test_df["orders_count"], predictions)), 3),
    }

    test_output = test_df[["order_date", "orders_count", "total_revenue"]].copy()
    test_output["predicted_orders"] = predictions.round(0).astype(int)
    test_output["absolute_error"] = (
        test_output["orders_count"] - test_output["predicted_orders"]
    ).abs()

    return model, feature_columns, metrics, test_output


def forecast_next_days(
    model,
    feature_columns,
    daily: pd.DataFrame,
    horizon_days: int = 14,
) -> pd.DataFrame:
    history = daily.copy().sort_values("order_date").reset_index(drop=True)
    forecasts = []

    for _ in range(horizon_days):
        next_date = history["order_date"].max() + pd.Timedelta(days=1)
        next_row = {
            "order_date": next_date,
            "orders_count": 0,
            "delivered_orders": 0,
            "total_revenue": history["total_revenue"].tail(7).mean(),
        }
        history = pd.concat([history, pd.DataFrame([next_row])], ignore_index=True)
        features = add_time_features(history).tail(1)
        predicted_orders = int(round(model.predict(features[feature_columns])[0]))
        history.loc[history.index[-1], "orders_count"] = max(predicted_orders, 0)
        forecasts.append(
            {
                "forecast_date": next_date.date().isoformat(),
                "predicted_orders": max(predicted_orders, 0),
            }
        )

    return pd.DataFrame(forecasts)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    daily = load_daily_orders()
    featured = add_time_features(daily)

    if len(featured) < 30:
        raise ValueError("Not enough daily history to train a useful forecasting model.")

    model, feature_columns, metrics, backtest = train_forecast_model(featured)
    future_forecast = forecast_next_days(model, feature_columns, daily)

    output_records = []

    for record in backtest.to_dict("records"):
        output_records.append(
            {
                "record_type": "backtest",
                "date": record["order_date"].date().isoformat(),
                "orders_count": int(record["orders_count"]),
                "predicted_orders": int(record["predicted_orders"]),
                "total_revenue": round(float(record["total_revenue"]), 2),
                "absolute_error": int(record["absolute_error"]),
            }
        )

    for record in future_forecast.to_dict("records"):
        output_records.append(
            {
                "record_type": "forecast",
                "date": record["forecast_date"],
                "orders_count": "",
                "predicted_orders": int(record["predicted_orders"]),
                "total_revenue": "",
                "absolute_error": "",
            }
        )

    output = pd.DataFrame(output_records)

    output.to_csv(FORECAST_PATH, index=False)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("ML forecasting layer completed.")
    print(f"Forecast output: {FORECAST_PATH}")
    print(f"Metrics output: {METRICS_PATH}")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
