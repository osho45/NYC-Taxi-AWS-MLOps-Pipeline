from __future__ import annotations

import os
import tempfile

import boto3
import joblib
import pandas as pd


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


def load_model():
    bucket = os.environ["ARTIFACTS_BUCKET"]
    key = "ml/models/taxi_demand_rf.joblib"

    s3 = boto3.client("s3")

    with tempfile.NamedTemporaryFile(suffix=".joblib") as temp_file:
        s3.download_file(bucket, key, temp_file.name)
        model = joblib.load(temp_file.name)

    return model


def predict(model, features: dict) -> float:
    row = pd.DataFrame(
        [[features[name] for name in FEATURES]],
        columns=FEATURES,
    )

    prediction = model.predict(row)[0]

    return max(float(prediction), 0.0)


def main():
    model = load_model()

    sample = {
        "pickup_location_id": 161,
        "hour": 17,
        "day_of_week": 1,
        "is_weekend": 0,
        "lag_1h": 85,
        "lag_24h": 92,
        "lag_168h": 88,
        "rolling_mean_3h": 82.3,
        "rolling_mean_24h": 64.7,
    }

    result = predict(model, sample)

    print(f"Predicted taxi demand: {result:.2f} rides")


if __name__ == "__main__":
    main()