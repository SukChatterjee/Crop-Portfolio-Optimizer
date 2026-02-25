from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
try:
    from tenacity import retry, stop_after_attempt, wait_exponential
except ImportError:
    def retry(*args, **kwargs):
        def _decorator(func):
            return func
        return _decorator

    def stop_after_attempt(*args, **kwargs):
        return None

    def wait_exponential(*args, **kwargs):
        return None

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
def _request_json(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    auth: Optional[Tuple[str, str]] = None,
) -> Dict:
    response = requests.get(url, headers=headers, params=params, auth=auth, timeout=25)
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


def _walk_items(node: Any):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk_items(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_items(item)


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    allowed = set("0123456789.-")
    cleaned = "".join(ch for ch in text if ch in allowed)
    if not cleaned or cleaned in {"-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _extract_year(item: Dict[str, Any]) -> Optional[int]:
    for key in ("report_begin_date", "report_date", "date", "reported_date"):
        raw = item.get(key)
        if not raw:
            continue
        text = str(raw).strip()
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(text[:10], fmt).year
            except ValueError:
                continue
        for token in text.split():
            if len(token) == 4 and token.isdigit():
                y = int(token)
                if 1900 <= y <= 2100:
                    return y
    return None


def _extract_price(item: Dict[str, Any]) -> Optional[float]:
    preferred_keys = [
        "weighted_average",
        "weighted_avg",
        "avg_price",
        "price",
        "low_price",
        "high_price",
    ]
    for key in preferred_keys:
        if key in item:
            v = _parse_float(item.get(key))
            if v is not None and v > 0:
                return v
    for key, raw in item.items():
        k = str(key).lower()
        if "price" in k or "average" in k:
            v = _parse_float(raw)
            if v is not None and v > 0:
                return v
    return None


def _parse_mars_response(payload: Dict[str, Any], crop: str, years: List[int]) -> pd.DataFrame:
    annual: Dict[int, List[float]] = {y: [] for y in years}
    for item in _walk_items(payload):
        if not isinstance(item, dict):
            continue
        year = _extract_year(item)
        if year not in annual:
            continue
        commodity = str(item.get("commodity", "")).strip().lower()
        if commodity and crop.lower() not in commodity and commodity not in {"all", "all commodities"}:
            continue
        price = _extract_price(item)
        if price is None:
            continue
        annual[year].append(float(price))

    rows = []
    for year in years:
        vals = annual.get(year, [])
        if vals:
            rows.append({"year": int(year), "crop": crop, "avg_price": round(float(sum(vals) / len(vals)), 4)})
    return pd.DataFrame(rows)


def _merge_mars_endpoint(base_detail_endpoint: str, slug_id: Any) -> str:
    base = base_detail_endpoint.rstrip("/")
    tail = f"/reports/{slug_id}/Report Detail"
    if "/reports/" in base:
        prefix = base.split("/reports/")[0]
        return f"{prefix}{tail}"
    return f"{base}{tail}"


def _extract_slug_ids(payload: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for item in _walk_items(payload):
        if not isinstance(item, dict):
            continue
        for key in ("slug_id", "report_id", "slugId"):
            if key in item and item.get(key) is not None:
                text = str(item.get(key)).strip()
                if text:
                    ids.append(text)
    dedup: List[str] = []
    seen = set()
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            dedup.append(sid)
    return dedup


def _fetch_mars_payload(
    endpoint: str,
    q: str,
    mars_key: str,
    force_refresh: bool,
    cache_key: Dict[str, Any],
) -> Dict[str, Any]:
    return cached_json(
        namespace="ams",
        key=cache_key,
        fetcher=lambda: _request_json(
            endpoint,
            headers={},
            params={"q": q},
            auth=(mars_key, ""),
        ),
        ttl_hours=12,
        force_refresh=force_refresh,
    )


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
    mars_key = os.environ.get("MARS_API_USERNAME", "").strip() or api_key
    mars_endpoint = os.environ.get(
        "AMS_MARS_REPORT_ENDPOINT",
        "https://marsapi.ams.usda.gov/services/v1.2/reports/3046/Report Detail",
    ).strip()
    mars_reports_endpoint = os.environ.get(
        "AMS_MARS_REPORTS_ENDPOINT",
        "https://marsapi.ams.usda.gov/services/v1.2/reports",
    ).strip()
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    start_str = f"01/01/{min(years)}"
    end_str = f"12/31/{max(years)}"

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
        elif mars_key:
            try:
                q = f"commodity={crop};report_begin_date={start_str}:{end_str}"
                key = {"crop": crop, "years": years, "endpoint": mars_endpoint, "q": q}
                payload = _fetch_mars_payload(
                    mars_endpoint,
                    q,
                    mars_key,
                    force_refresh,
                    cache_key=key,
                )
                df = _parse_mars_response(payload, crop, years)
                if not df.empty:
                    frames.append(df)
                    print(f"[agent][tool] ams crop={crop} source=mars_api", flush=True)
                    continue
                # Auto-discover other report ids for commodity when default report has no rows.
                discovery_q = f"commodity={crop}"
                discovery_key = {"crop": crop, "endpoint": mars_reports_endpoint, "q": discovery_q, "kind": "discover"}
                discovered = _fetch_mars_payload(
                    mars_reports_endpoint,
                    discovery_q,
                    mars_key,
                    force_refresh,
                    cache_key=discovery_key,
                )
                slug_ids = _extract_slug_ids(discovered)[:6]
                matched = False
                for sid in slug_ids:
                    candidate_endpoint = _merge_mars_endpoint(mars_endpoint, sid)
                    candidate_q = f"commodity={crop};report_begin_date={start_str}:{end_str}"
                    candidate_key = {
                        "crop": crop,
                        "years": years,
                        "endpoint": candidate_endpoint,
                        "q": candidate_q,
                        "slug_id": sid,
                    }
                    candidate_payload = _fetch_mars_payload(
                        candidate_endpoint,
                        candidate_q,
                        mars_key,
                        force_refresh,
                        cache_key=candidate_key,
                    )
                    candidate_df = _parse_mars_response(candidate_payload, crop, years)
                    if not candidate_df.empty:
                        frames.append(candidate_df)
                        print(
                            f"[agent][tool] ams crop={crop} source=mars_api_discovered slug_id={sid}",
                            flush=True,
                        )
                        matched = True
                        break
                if matched:
                    continue
                print(f"[agent][tool] ams crop={crop} source=mars_api_empty", flush=True)
            except Exception as exc:
                print(f"[agent][tool] ams crop={crop} source=mars_api error={exc}", flush=True)
        else:
            print(f"[agent][tool] ams crop={crop} source=fallback reason=missing_template", flush=True)
        frames.append(pd.DataFrame(_fallback_price_series(crop, years)))
        if template or mars_key:
            print(f"[agent][tool] ams crop={crop} source=fallback reason=api_unavailable", flush=True)
        else:
            print(f"[agent][tool] ams crop={crop} source=fallback reason=missing_api_config", flush=True)

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["year", "crop", "avg_price"])
    save_parquet(parquet_path, result)
    return result
