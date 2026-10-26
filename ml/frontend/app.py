import os
from datetime import date, time, datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import streamlit as st


API_URL = os.getenv("API_URL")
BASE_DIR = Path(__file__).resolve().parent


zone_df = pd.read_csv(
    BASE_DIR / "taxi_zone_lookup.csv"
)

zone_df["display_name"] = (
    zone_df["Borough"]
    + " - "
    + zone_df["Zone"]
)


st.set_page_config(
    page_title="NYC Taxi Demand Predictor",
    page_icon="🚕",
    layout="centered",
)


st.title("🚕 NYC Taxi Demand Predictor")

st.write(
    "Select a pickup location and prediction time "
    "to estimate the expected number of taxi rides."
)


# Searchable dropdown
selected_zone = st.selectbox(
    "Pickup Location",
    options=zone_df["display_name"].tolist(),
    index=int(zone_df.index[
        zone_df["LocationID"] == 161
    ][0]),
    placeholder="Search or select a pickup location",
)


pickup_location_id = int(
    zone_df.loc[
        zone_df["display_name"] == selected_zone,
        "LocationID",
    ].iloc[0]
)


prediction_date = st.date_input(
    "Prediction Date",
    value=date(2025, 3, 20),
)


prediction_time = st.time_input(
    "Prediction Time",
    value=time(17, 0),
)


if st.button("Predict Demand"):

    if not API_URL:
        st.error(
            "API_URL environment variable is not configured."
        )
        st.stop()

    dt = datetime.combine(
        prediction_date,
        prediction_time,
        tzinfo=timezone.utc,
    )

    payload = {
        "pickup_location_id": pickup_location_id,
        "prediction_time": dt.isoformat().replace(
            "+00:00",
            "Z",
        ),
    }

    try:

        with st.spinner("Calculating demand..."):

            response = requests.post(
                f"{API_URL}/predict",
                json=payload,
                timeout=35,
            )

        if response.status_code == 200:

            result = response.json()

            predicted_rides = result[
                "predicted_rides"
            ]

            st.success(
                "Prediction completed"
            )

            st.metric(
                label="Predicted Taxi Demand",
                value=f"{predicted_rides:.0f} rides",
            )

            st.caption(
                f"Exact model prediction: "
                f"{predicted_rides:.2f}"
            )

        else:

            try:
                error = response.json()

            except ValueError:
                error = response.text

            st.error(
                f"Prediction failed: {error}"
            )

    except requests.exceptions.Timeout:

        st.error(
            "The prediction service took too long "
            "to respond. Please try again."
        )

    except requests.exceptions.RequestException as exc:

        st.error(
            f"Unable to reach prediction service: {exc}"
        )
