from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone

import boto3
import joblib
import pandas as pd
from boto3.dynamodb.conditions import Key


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

MODEL_KEY = "ml/models/taxi_demand_rf.joblib"

_model = None

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def load_model():
    global _model

    if _model is not None:
        return _model

    bucket = os.environ["ARTIFACTS_BUCKET"]

    with tempfile.NamedTemporaryFile(suffix=".joblib") as temp_file:
        s3.download_file(bucket, MODEL_KEY, temp_file.name)
        _model = joblib.load(temp_file.name)

    return _model


def get_historical_features(
    pickup_location_id: int,
    prediction_time: datetime,
) -> dict:

    table_name = os.environ["DEMAND_HISTORY_TABLE"]
    table = dynamodb.Table(table_name)

    start_time = (
            prediction_time - timedelta(hours=168)
        ).replace(tzinfo=None)

    end_time = (
            prediction_time - timedelta(hours=1)
        ).replace(tzinfo=None)

    response = table.query(
        KeyConditionExpression=(
            Key("pickup_location_id").eq(pickup_location_id)
            & Key("pickup_hour_ts").between(
                start_time.isoformat(),
                end_time.isoformat(),
            )
        )
    )

    items = response["Items"]

    history = {
        item["pickup_hour_ts"]: float(item["ride_count"])
        for item in items
    }

    def value_at(hours_back: int) -> float:
        timestamp = (
            prediction_time - timedelta(hours=hours_back)
        ).replace(tzinfo=None).isoformat()

        if timestamp not in history:
            raise ValueError(
                f"Missing historical demand for "
                f"location={pickup_location_id}, "
                f"time={timestamp}"
            )

        return history[timestamp]

    lag_1h = value_at(1)
    lag_24h = value_at(24)
    lag_168h = value_at(168)

    last_3h = [
        value_at(i)
        for i in range(1, 4)
    ]

    last_24h = [
        value_at(i)
        for i in range(1, 25)
    ]

    return {
        "lag_1h": lag_1h,
        "lag_24h": lag_24h,
        "lag_168h": lag_168h,
        "rolling_mean_3h": sum(last_3h) / 3,
        "rolling_mean_24h": sum(last_24h) / 24,
    }


def get_available_prediction_range():
    table_name = os.environ["DEMAND_HISTORY_TABLE"]
    table = dynamodb.Table(table_name)

    response = table.get_item(
        Key={
            "pickup_location_id": 0,
            "pickup_hour_ts": "METADATA",
        }
    )

    metadata = response.get("Item")

    if not metadata:
        raise ValueError(
            "Prediction range metadata is not available yet."
        )

    return {
        "earliest_prediction_time":
            metadata["earliest_prediction_time"],
        "latest_prediction_time":
            metadata["latest_prediction_time"],
    }


def lambda_handler(event, context):
    try:
        route_key = event.get("routeKey")

        if route_key == "GET /range":
            prediction_range = get_available_prediction_range()

            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(prediction_range),
            }

        body = event.get("body", event)

        if isinstance(body, str):
            body = json.loads(body)

        pickup_location_id = int(
            body["pickup_location_id"]
        )

        prediction_time = datetime.fromisoformat(
            body["prediction_time"].replace("Z", "+00:00")
        )

        if prediction_time.tzinfo is None:
            prediction_time = prediction_time.replace(
                tzinfo=timezone.utc
            )

        history = get_historical_features(
            pickup_location_id,
            prediction_time,
        )

        values = {
            "pickup_location_id": pickup_location_id,
            "hour": prediction_time.hour,
            "day_of_week": prediction_time.weekday(),
            "is_weekend": int(
                prediction_time.weekday() >= 5
            ),
            **history,
        }

        row = pd.DataFrame(
            [[values[name] for name in FEATURES]],
            columns=FEATURES,
            dtype=float,
        )

        model = load_model()

        prediction = max(
            float(model.predict(row)[0]),
            0.0,
        )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(
                {
                    "pickup_location_id": pickup_location_id,
                    "prediction_time": prediction_time.isoformat(),
                    "predicted_rides": round(prediction, 2),
                }
            ),
        }

    except Exception as exc:
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps(
                {
                    "error": str(exc)
                }
            ),
        }
