#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve()
BACKEND_DIR = CURRENT.parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from agent.graph import build_graph


def main() -> None:
    app = build_graph()
    init_state = {
        "farm_profile": {
            "location": {
                "lat": 40.4173,
                "lng": -82.9071,
                "address": "Ohio, USA",
                "county": "Delaware",
                "state": "OH",
            },
            "acres": 220,
            "has_irrigation": True,
            "soil_type": "Loam",
            "selected_crops": ["Corn", "Wheat", "Soybeans", "Tomatoes", "Lettuce"],
            "risk_preference": "moderate",
            "goal": "balanced",
        },
        "datasets_summary": {},
        "crop_results": [],
        "weather_summary": "",
        "market_outlook": "",
        "errors": [],
    }
    result = app.invoke(init_state)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

