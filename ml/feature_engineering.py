from __future__ import annotations

import os

import awswrangler as wr
import pandas as pd


DATABASE = "nyc_taxi_ml_gold"
TABLE = "hourly_ml_observations"


def load_gold_data() -> pd.DataFrame:
    """Read Gold ML observations from Athena."""

    region = os.getenv("AWS_REGION", "us-east-1")

    print(f"Reading Gold data from AWS region: {region}")

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
    """Create time, lag, and rolling demand features."""

    df = df.copy()

    df["pickup_hour_ts"] = pd.to_datetime(df["pickup_hour_ts"])

    df = df.sort_values(
        ["pickup_location_id", "pickup_hour_ts"]
    ).reset_index(drop=True)

    # -----------------------------
    # Time features
    # -----------------------------
    df["hour"] = df["pickup_hour_ts"].dt.hour
    df["day_of_week"] = df["pickup_hour_ts"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    # -----------------------------
    # Historical demand features
    # -----------------------------
    grouped = df.groupby("pickup_location_id")["ride_count"]

    df["lag_1h"] = grouped.shift(1)
    df["lag_24h"] = grouped.shift(24)
    df["lag_168h"] = grouped.shift(168)

    # Rolling averages must use only previous observations.
    shifted_demand = grouped.shift(1)

    df["rolling_mean_3h"] = (
        shifted_demand
        .groupby(df["pickup_location_id"])
        .rolling(3)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["rolling_mean_24h"] = (
        shifted_demand
        .groupby(df["pickup_location_id"])
        .rolling(24)
        .mean()
        .reset_index(level=0, drop=True)
    )

    feature_columns = [
        "lag_1h",
        "lag_24h",
        "lag_168h",
        "rolling_mean_3h",
        "rolling_mean_24h",
    ]

    df = df.dropna(subset=feature_columns).reset_index(drop=True)

    return df


def main() -> None:

    df = load_gold_data()

    print(f"Gold rows loaded: {len(df):,}")

    feature_df = create_features(df)

    print("\nFeature columns:")
    print(feature_df.columns.tolist())

    print("\nSample:")
    print(feature_df.head(10))

    print("\nMissing values:")
    print(feature_df.isna().sum())


if __name__ == "__main__":
    main()