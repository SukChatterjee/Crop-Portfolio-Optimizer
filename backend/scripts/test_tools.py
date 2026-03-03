#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve()
BACKEND_DIR = CURRENT.parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from agent_tools.compute import compute_forecasts
from agent_tools.costs import fetch_cost_per_acre
from agent_tools.nass import fetch_ohio_crop_stats
from agent_tools.noaa import fetch_weather_features
import pandas as pd


def main() -> None:
    farm_profile = {
        "location": {
            "lat": 40.4173,
            "lng": -82.9071,
            "address": "Ohio",
            "county": "Delaware",
            "state": "OH",
        },
        "acres": 220,
        "has_irrigation": True,
        "soil_type": "Loam",
        "selected_crops": ["Corn", "Wheat", "Soybeans", "Tomatoes"],
        "risk_preference": "moderate",
        "goal": "balanced",
    }

    crops = farm_profile["selected_crops"]
    lat = farm_profile["location"]["lat"]
    lng = farm_profile["location"]["lng"]

    nass_df = fetch_ohio_crop_stats(crops, last_n_years=3)
    weather = fetch_weather_features(lat, lng, last_n_years=3)
    now_year = pd.Timestamp.utcnow().year
    years = list(range(now_year - 2, now_year + 1))
    price_df = pd.DataFrame(
        [{"year": y, "crop": c, "avg_price": 3.5} for c in crops for y in years]
    )
    costs = fetch_cost_per_acre(crops)
    
    results = compute_forecasts(farm_profile, nass_df, price_df, costs, weather)

    print("Weather summary:")
    print(weather.get("summary", "N/A"))
    print("\nForecasts:")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
