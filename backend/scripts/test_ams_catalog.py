#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

CURRENT = Path(__file__).resolve()
BACKEND_DIR = CURRENT.parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from agent_tools.ams import fetch_reports_catalog


def main() -> None:
    rows = fetch_reports_catalog(force_refresh=False)
    print(f"catalog_count={len(rows)}")
    print("sample_5:")
    for row in rows[:5]:
        print(
            {
                "slug_id": row.get("slug_id"),
                "slug_name": row.get("slug_name"),
                "report_title": row.get("report_title"),
                "published_date": row.get("published_date"),
            }
        )


if __name__ == "__main__":
    main()

