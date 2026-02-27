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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--crop", required=True)
    parser.add_argument("--state", default="OHIO")
    parser.add_argument("--lookback-days", type=int, default=1095)
    args = parser.parse_args()

    plan = plan_ams_for_crop(
        crop_name=args.crop,
        state_name=args.state,
        lookback_days=args.lookback_days,
    )
    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    main()

