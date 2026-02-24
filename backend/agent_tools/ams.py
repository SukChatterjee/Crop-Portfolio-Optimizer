from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .cache import cached_json, load_parquet, parquet_cache_path, save_parquet


CROP_TO_SLUG_ID = {
    "corn": "corn",
    "wheat": "wheat",
    "soybeans": "soybeans",
    "rice": "rice",
    "cotton": "cotton",
    "tomatoes": "tomatoes",
    "potatoes": "potatoes",
    "onions": "onions",
    "apples": "apples",
    "lettuce": "lettuce",
}


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
def _request_json(url: str, headers: Dict[str, str], params: Dict[str, str]) -> Dict:
    response = requests.get(url, headers=headers, params=params, timeout=25)
    response.raise_for_status()
    return response.json()


def _fallback_price_series(crop: str, years: List[int]) -> List[Dict]:
    rows = []
    for year in years:
        price = _seeded_float(f"price:{crop}:{year}", 0.18, 14.0)
        rows.append({"year": int(year), "crop": crop, "avg_price": round(price, 4)})
    return rows


def _parse_template_response(payload: Dict, crop: str, years: List[int]) -> pd.DataFrame:
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    parsed = []
    for item in rows:
        try:
            year = int(item.get("year"))
            price = float(item.get("price"))
        except (TypeError, ValueError):
            continue
        if year in years:
            parsed.append({"year": year, "crop": crop, "avg_price": price})
    return pd.DataFrame(parsed)


def fetch_price_series(
    selected_crops: List[str],
    last_n_years: int = 3,
    force_refresh: bool = False,
) -> pd.DataFrame:
    now_year = datetime.utcnow().year
    years = list(range(now_year - last_n_years + 1, now_year + 1))
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    cache_key = {"crops": sorted(crops), "years": years}
    parquet_path = parquet_cache_path("ams", cache_key)
    cached = load_parquet(parquet_path)
    if not force_refresh and cached is not None and not cached.empty:
        print("[agent][tool] ams source=processed-cache", flush=True)
        return cached

    template = os.environ.get("AMS_PRICE_URL_TEMPLATE", "").strip()
    api_key = os.environ.get("AMS_API_KEY", "").strip()
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    frames = []
    for crop in crops:
        slug = CROP_TO_SLUG_ID.get(crop.lower(), crop.lower().replace(" ", "-"))
        if template:
            try:
                url = template.format(slug=slug)
                key = {"crop": crop, "slug": slug, "years": years, "template": template}
                payload = cached_json(
                    namespace="ams",
                    key=key,
                    fetcher=lambda u=url: _request_json(u, headers=headers, params={}),
                    ttl_hours=12,
                    force_refresh=force_refresh,
                )
                df = _parse_template_response(payload, crop, years)
                if not df.empty:
                    frames.append(df)
                    print(f"[agent][tool] ams crop={crop} source=api", flush=True)
                    continue
            except Exception as exc:
                print(f"[agent][tool] ams crop={crop} source=api error={exc}", flush=True)
                pass
        else:
            print(f"[agent][tool] ams crop={crop} source=fallback reason=missing_template", flush=True)
        frames.append(pd.DataFrame(_fallback_price_series(crop, years)))
        if template:
            print(f"[agent][tool] ams crop={crop} source=fallback reason=api_unavailable", flush=True)

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["year", "crop", "avg_price"])
    save_parquet(parquet_path, result)
    return result
