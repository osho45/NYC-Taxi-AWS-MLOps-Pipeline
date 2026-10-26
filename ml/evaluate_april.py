import json
import os
from pathlib import Path

import awswrangler as wr
import joblib
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error

from ml.train import create_features, FEATURES, TARGET

ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = ROOT / "ml" / "artifacts" / "taxi_demand_rf.joblib"

print("Loading Gold ML observations from Athena...")

# Include March for April's historical lag features.
df = wr.athena.read_sql_query(
    sql="""
        SELECT pickup_location_id, pickup_hour_ts, ride_count
        FROM nyc_taxi_ml_gold.hourly_ml_observations
        WHERE target_year = 2025
          AND target_month IN (3, 4)
    """,
    database="nyc_taxi_ml_gold",
    ctas_approach=False,
    s3_output=os.environ["ATHENA_RESULTS_S3_URI"],
)

df = create_features(df)

april = df[
    (df["pickup_hour_ts"] >= "2025-04-01")
    & (df["pickup_hour_ts"] < "2025-05-01")
].copy()

if april.empty:
    raise RuntimeError("No April evaluation rows found.")

print(f"April evaluation rows: {len(april):,}")

model = joblib.load(MODEL_PATH)

predictions = np.maximum(model.predict(april[FEATURES]), 0)

baseline_mae = mean_absolute_error(april[TARGET], april["lag_24h"])
model_mae = mean_absolute_error(april[TARGET], predictions)
model_rmse = np.sqrt(mean_squared_error(april[TARGET], predictions))
improvement = (
    100 * (baseline_mae - model_mae) / baseline_mae
    if baseline_mae > 0 else 0
)

metrics = {
    "month": "2025-04",
    "evaluation_rows": len(april),
    "baseline_mae": round(float(baseline_mae), 4),
    "model_mae": round(float(model_mae), 4),
    "model_rmse": round(float(model_rmse), 4),
    "mae_improvement_percent": round(float(improvement), 2),
}

print(json.dumps(metrics, indent=2))
