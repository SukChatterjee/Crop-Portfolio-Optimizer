from __future__ import annotations

import hashlib
import os
from typing import Dict, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .cache import cached_json, load_parquet, parquet_cache_path, save_parquet


DEFAULT_COSTS = {
    "corn": 780.0,
    "wheat": 520.0,
    "soybeans": 540.0,
    "rice": 980.0,
    "cotton": 760.0,
    "tomatoes": 1900.0,
    "potatoes": 1450.0,
    "onions": 1320.0,
    "apples": 2100.0,
    "lettuce": 1680.0,
}


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
def _request_json(url: str, params: Dict[str, str]) -> Dict:
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def fetch_cost_per_acre(selected_crops: List[str], force_refresh: bool = False) -> Dict[str, float]:
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    cache_key = {"crops": sorted(crops)}
    parquet_path = parquet_cache_path("costs", cache_key)
    cached = load_parquet(parquet_path)
    if not force_refresh and cached is not None and not cached.empty:
        print("[agent][tool] costs source=processed-cache", flush=True)
        return {r["crop"]: float(r["cost_per_acre"]) for _, r in cached.iterrows()}

    costs = {}
    api_url = os.environ.get("COSTS_API_URL", "").strip()
    if api_url:
        try:
            payload = cached_json(
                namespace="costs",
                key={"url": api_url, "crops": sorted(crops)},
                fetcher=lambda: _request_json(api_url, {"crops": ",".join(crops)}),
                ttl_hours=24,
                force_refresh=force_refresh,
            )
            data = payload.get("data", []) if isinstance(payload, dict) else []
            for row in data:
                crop = str(row.get("crop", "")).strip()
                if not crop:
                    continue
                try:
                    costs[crop] = float(row.get("cost_per_acre"))
                except (TypeError, ValueError):
                    continue
            print("[agent][tool] costs source=api", flush=True)
        except Exception as exc:
            print(f"[agent][tool] costs source=fallback reason=api_error error={exc}", flush=True)
            pass
    else:
        print("[agent][tool] costs source=fallback reason=missing_api_url", flush=True)

    for crop in crops:
        if crop not in costs:
            known = DEFAULT_COSTS.get(crop.lower())
            if known is not None:
                costs[crop] = known
            else:
                costs[crop] = round(_seeded_float(f"cost:{crop}", 550.0, 2200.0), 2)

    save_parquet(
        parquet_path,
        pd.DataFrame(
            [{"crop": crop, "cost_per_acre": value} for crop, value in costs.items()]
        ),
    )
    return costs
