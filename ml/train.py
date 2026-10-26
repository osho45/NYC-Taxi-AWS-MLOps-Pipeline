from __future__ import annotations

import os

import awswrangler as wr
import joblib
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import json
from datetime import datetime, timezone


DATABASE = "nyc_taxi_ml_gold"
TABLE = "hourly_ml_observations"

FEATURES = [
    "pickup_location_id",
    "hour",
    "day_of_week",
    "is_weekend",
    "lag_1h",
    "lag_24h",
    "lag_168h",
    "rolling_mean_3h",
    "rolling_mean_24h",
]

TARGET = "ride_count"


def load_data() -> pd.DataFrame:
    print("Reading Gold ML observations from Athena...")

    df = wr.athena.read_sql_query(
        sql=f"""
            SELECT
                pickup_location_id,
                pickup_hour_ts,
                ride_count
            FROM {DATABASE}.{TABLE}
            ORDER BY pickup_location_id, pickup_hour_ts
        """,
        database=DATABASE,
        ctas_approach=False,
    )

    return df


def create_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["pickup_hour_ts"] = pd.to_datetime(df["pickup_hour_ts"])

    df = df.sort_values(
        ["pickup_location_id", "pickup_hour_ts"]
    ).reset_index(drop=True)

    # Time features
    df["hour"] = df["pickup_hour_ts"].dt.hour
    df["day_of_week"] = df["pickup_hour_ts"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # Historical demand
    grouped = df.groupby("pickup_location_id")["ride_count"]

    df["lag_1h"] = grouped.shift(1)
    df["lag_24h"] = grouped.shift(24)
    df["lag_168h"] = grouped.shift(168)

    shifted = grouped.shift(1)

    df["rolling_mean_3h"] = (
        shifted
        .groupby(df["pickup_location_id"])
        .rolling(3)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["rolling_mean_24h"] = (
        shifted
        .groupby(df["pickup_location_id"])
        .rolling(24)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df = df.dropna(subset=FEATURES).reset_index(drop=True)

    return df


def main() -> None:
    df = load_data()

    print(f"Raw rows: {len(df):,}")

    df = create_features(df)

    print(f"ML-ready rows: {len(df):,}")

    # -----------------------------------------
    # Time-based train/test split
    # -----------------------------------------

    train_df = df[
        df["pickup_hour_ts"] < "2025-03-01"
    ].copy()

    test_df = df[
        df["pickup_hour_ts"] >= "2025-03-01"
    ].copy()

    print(f"Training rows: {len(train_df):,}")
    print(f"Testing rows:  {len(test_df):,}")

    X_train = train_df[FEATURES]
    y_train = train_df[TARGET]

    X_test = test_df[FEATURES]
    y_test = test_df[TARGET]

    # -----------------------------------------
    # Simple baseline
    # Yesterday's same-hour demand
    # -----------------------------------------

    baseline_predictions = test_df["lag_24h"]

    baseline_mae = mean_absolute_error(
        y_test,
        baseline_predictions,
    )

    print(f"\nBaseline MAE: {baseline_mae:.3f}")

    # -----------------------------------------
    # Random Forest
    # -----------------------------------------

    print("\nTraining Random Forest...")

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    )

    model.fit(X_train, y_train)

    predictions = model.predict(X_test)

    # Taxi demand cannot be negative.
    predictions = np.maximum(predictions, 0)

    mae = mean_absolute_error(y_test, predictions)

    rmse = np.sqrt(
        mean_squared_error(y_test, predictions)
    )

    print("\nModel evaluation")
    print("----------------")
    print(f"Random Forest MAE:  {mae:.3f}")
    print(f"Random Forest RMSE: {rmse:.3f}")
    print(f"Baseline MAE:       {baseline_mae:.3f}")

    improvement = (
        (baseline_mae - mae) / baseline_mae
    ) * 100

    print(
        f"MAE improvement over baseline: "
        f"{improvement:.2f}%"
    )

    # -----------------------------------------
    # Feature importance
    # -----------------------------------------

    importance = pd.DataFrame(
        {
            "feature": FEATURES,
            "importance": model.feature_importances_,
        }
    ).sort_values(
        "importance",
        ascending=False,
    )

    print("\nFeature importance:")
    print(importance.to_string(index=False))

    # -----------------------------------------
    # Save model artifacts locally
    # -----------------------------------------

    model_dir = os.path.join(
        os.path.dirname(__file__),
        "artifacts",
    )

    os.makedirs(model_dir, exist_ok=True)

    model_path = os.path.join(
        model_dir,
        "taxi_demand_rf.joblib",
    )

    metrics_path = os.path.join(
        model_dir,
        "metrics.json",
    )

    metadata_path = os.path.join(
        model_dir,
        "model_metadata.json",
    )

    joblib.dump(model, model_path)

    metrics = {
        "baseline_mae": float(baseline_mae),
        "model_mae": float(mae),
        "model_rmse": float(rmse),
        "mae_improvement_percent": float(improvement),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
    }

    with open(metrics_path, "w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2)

    metadata = {
        "model_type": "RandomForestRegressor",
        "target": TARGET,
        "features": FEATURES,
        "train_end": "2025-02-28",
        "test_start": "2025-03-01",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    with open(metadata_path, "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    print("\nArtifacts saved:")
    print(model_path)
    print(metrics_path)
    print(metadata_path)


if __name__ == "__main__":
    main()