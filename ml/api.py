from __future__ import annotations

import os
import tempfile

import boto3
import joblib
import pandas as pd

from fastapi import FastAPI
from pydantic import BaseModel


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


app = FastAPI(title="NYC Taxi Demand API")


class PredictionRequest(BaseModel):
    pickup_location_id: int
    hour: int
    day_of_week: int
    is_weekend: int
    lag_1h: float
    lag_24h: float
    lag_168h: float
    rolling_mean_3h: float
    rolling_mean_24h: float


def load_model():
    bucket = os.environ["ARTIFACTS_BUCKET"]
    key = "ml/models/taxi_demand_rf.joblib"

    s3 = boto3.client("s3")

    with tempfile.NamedTemporaryFile(suffix=".joblib") as temp_file:
        s3.download_file(bucket, key, temp_file.name)
        return joblib.load(temp_file.name)


model = load_model()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(request: PredictionRequest):
    values = request.model_dump()

    row = pd.DataFrame(
        [[values[name] for name in FEATURES]],
        columns=FEATURES,
    )

    prediction = model.predict(row)[0]
    prediction = max(float(prediction), 0.0)

    return {
        "predicted_rides": round(prediction, 2)
    }