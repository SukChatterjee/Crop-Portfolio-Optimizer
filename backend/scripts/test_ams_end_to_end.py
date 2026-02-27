#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve()
BACKEND_DIR = CURRENT.parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from agent.planners.ams_planner import plan_ams_for_crop
from agent_tools.ams import get_prices_for_crop_with_plan


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crops", required=True, help="Comma-separated crop names")
    parser.add_argument("--state", default="OHIO")
    parser.add_argument("--lookback-days", type=int, default=1095)
    args = parser.parse_args()

    crops = [c.strip() for c in args.crops.split(",") if c.strip()]
    out = {}
    for crop in crops:
        plan = plan_ams_for_crop(
            crop_name=crop,
            state_name=args.state,
            lookback_days=args.lookback_days,
        )
        result = get_prices_for_crop_with_plan(
            crop_name=crop,
            state_name=args.state,
            lookback_days=args.lookback_days,
            planner_json=plan,
        )
        out[crop] = {
            "plan": plan,
            "result": result,
        }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

