from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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


def _is_plausible_cost(crop: str, value: float) -> bool:
    if not pd.notna(value):
        return False
    v = float(value)
    # Generic per-acre sanity bounds.
    if v < 50 or v > 20000:
        return False
    base = float(DEFAULT_COSTS.get(str(crop).lower(), 1200.0))
    lower = max(50.0, 0.35 * base)
    upper = min(20000.0, 5.0 * base)
    return lower <= v <= upper


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


def _to_float(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    try:
        return float(text)
    except (TypeError, ValueError):
        return float("nan")


def _with_year(url: str, year: int) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["year"] = str(year)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _parse_query(url: str) -> Dict[str, str]:
    parts = urlsplit(url)
    return {k: v for k, v in parse_qsl(parts.query, keep_blank_values=True)}


def _replace_path(url: str, new_path: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


def _with_params(url: str, updates: Dict[str, str]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    for k, v in updates.items():
        if v is None:
            continue
        query[k] = str(v)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _contains_any(text: str, needles: List[str]) -> bool:
    t = text.lower()
    return any(n in t for n in needles)


def _crop_terms(crop: str) -> List[str]:
    c = crop.lower().strip()
    if not c:
        return []
    terms = [c]
    if c.endswith("s") and len(c) > 3:
        terms.append(c[:-1])
    if c.endswith("es") and len(c) > 3:
        terms.append(c[:-2])
    return list(dict.fromkeys(terms))


def _ers_row_matches_crop(row: Dict[str, Any], crop: str) -> bool:
    terms = _crop_terms(crop)
    hay = " ".join(
        str(row.get(k, "") or "")
        for k in ("farmType", "category", "categoryValue", "category2", "category2Value", "variableName", "variableDesc")
    ).lower()
    return any(t in hay for t in terms)


def _pick_cost_from_rows(rows: List[Dict[str, Any]], crop: str) -> Optional[float]:
    """Select best per-acre cost estimate from ARMS survey rows."""
    scored: List[tuple] = []
    crop_l = crop.lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        blob = " ".join(str(v) for v in row.values() if v is not None).lower()
        if crop_l not in blob and crop_l.rstrip("s") not in blob:
            continue
        value_candidates = [
            row.get("cost_per_acre"),
            row.get("estimate"),
            row.get("value"),
            row.get("val"),
            row.get("amount"),
        ]
        value = float("nan")
        for candidate in value_candidates:
            f = _to_float(candidate)
            if pd.notna(f):
                value = f
                break
        if not pd.notna(value) or value <= 0:
            continue
        # Prefer rows explicitly about per-acre costs/expenses.
        score = 0
        if _contains_any(blob, ["per acre", "per_acre", "/acre"]):
            score += 4
        if _contains_any(blob, ["cost", "expense", "operating", "total"]):
            score += 3
        if _contains_any(blob, ["return", "revenue", "income"]):
            score -= 3
        scored.append((score, float(value)))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [v for s, v in scored[:5] if s == scored[0][0]]
    return float(sum(top) / len(top)) if top else None


def _extract_cost_rows(payload: Dict, crops: List[str]) -> Dict[str, float]:
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    out: Dict[str, float] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        crop_raw = (
            row.get("crop")
            or row.get("commodity")
            or row.get("commodity_desc")
            or row.get("item")
            or row.get("product")
            or ""
        )
        crop_name = str(crop_raw).strip()
        if not crop_name:
            continue
        value_raw = row.get("cost_per_acre")
        if value_raw is None:
            for k, v in row.items():
                key = str(k).lower()
                if "per_acre" in key and ("cost" in key or "expense" in key):
                    value_raw = v
                    break
        value = _to_float(value_raw)
        if not pd.notna(value) or value <= 0:
            continue
        for crop in crops:
            c = crop.lower()
            n = crop_name.lower()
            if c in n or n in c:
                if _is_plausible_cost(crop, float(value)):
                    out[crop] = float(value)
                break

    # ERS ARMS survey schema fallback:
    # rows often carry crop in category/categoryValue and value in estimate.
    if len(out) < len(crops):
        divisor = float(os.environ.get("ERS_PER_FARM_TO_PER_ACRE_DIVISOR", "1000"))
        for crop in crops:
            if crop in out:
                continue
            best: Optional[tuple] = None  # (score, value)
            for row in rows:
                if not isinstance(row, dict) or not _ers_row_matches_crop(row, crop):
                    continue
                name = str(row.get("variableName", "")).lower()
                desc = str(row.get("variableDesc", "")).lower()
                unit = str(row.get("variableUnit", "")).lower()
                blob = f"{name} {desc} {unit}"
                if not _contains_any(blob, ["expense", "cost", "operating"]):
                    continue
                if _contains_any(blob, ["income", "revenue", "return"]):
                    continue
                raw = row.get("estimate")
                value = _to_float(raw)
                if not pd.notna(value) or value <= 0:
                    continue
                # Prefer total cost metrics.
                score = 0
                if "total cash expenses" in blob:
                    score += 5
                if "total expenses" in blob:
                    score += 4
                if "variable expenses" in blob:
                    score += 3
                if "fixed expenses" in blob:
                    score += 2
                if "per acre" in blob or "/acre" in blob:
                    score += 4
                if "dollars per farm" in blob:
                    # Convert per-farm estimate into a conservative per-acre proxy.
                    value = float(value) / max(divisor, 1.0)
                if best is None or score > best[0]:
                    best = (score, float(value))
            if best and best[1] > 0 and _is_plausible_cost(crop, best[1]):
                out[crop] = round(best[1], 2)
    return out


def _fetch_ers_survey_for_crop(base_url: str, crop: str, year: int, force_refresh: bool) -> Dict[str, Any]:
    # If user provided /arms/report endpoint, switch to /arms/surveydata to fetch actual values.
    survey_url = base_url
    if "/arms/report" in survey_url:
        survey_url = _replace_path(survey_url, "/data/arms/surveydata")
    # Narrow request by commodity where possible.
    survey_url = _with_params(
        survey_url,
        {
            "year": str(year),
            "category": "commodity",
            "category_value": crop,
        },
    )
    key = {"url": survey_url, "crop": crop, "year": year, "kind": "surveydata"}
    payload = cached_json(
        namespace="costs",
        key=key,
        fetcher=lambda u=survey_url: _request_json(u, {}),
        ttl_hours=24,
        force_refresh=force_refresh,
    )
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    if isinstance(rows, list) and rows:
        return payload
    # Fallback to broader survey pull for the year/report and filter client-side by crop.
    broad_url = _with_params(survey_url, {"category": "", "category_value": ""})
    broad_key = {"url": broad_url, "crop": crop, "year": year, "kind": "surveydata_broad"}
    return cached_json(
        namespace="costs",
        key=broad_key,
        fetcher=lambda u=broad_url: _request_json(u, {}),
        ttl_hours=24,
        force_refresh=force_refresh,
    )


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
            last_n_years = int(os.environ.get("COSTS_LAST_N_YEARS", "3"))
            now_year = datetime.utcnow().year
            base_year_raw = os.environ.get("COSTS_BASE_YEAR", "").strip()
            base_year = int(base_year_raw) if base_year_raw.isdigit() else (now_year - 1)
            years = [base_year - i for i in range(max(1, last_n_years))]
            years.sort()

            by_crop_series: Dict[str, List[float]] = {crop: [] for crop in crops}
            year_hits: Dict[int, int] = {y: 0 for y in years}
            query = _parse_query(api_url)
            for year in years:
                year_url = _with_year(api_url, year)
                # Primary request with user's URL.
                payload = cached_json(
                    namespace="costs",
                    key={"url": year_url, "crops": sorted(crops), "year": year, "kind": "primary"},
                    fetcher=lambda u=year_url: _request_json(u, {}),
                    ttl_hours=24,
                    force_refresh=force_refresh,
                )
                year_costs = _extract_cost_rows(payload, crops)

                # If primary payload is metadata-only (common for /arms/report), fallback to surveydata per crop.
                if not year_costs and "/api.ers.usda.gov/" in year_url and "/arms/" in year_url:
                    for crop in crops:
                        try:
                            survey_payload = _fetch_ers_survey_for_crop(api_url, crop, year, force_refresh=force_refresh)
                            survey_rows = survey_payload.get("data", []) if isinstance(survey_payload, dict) else []
                            if isinstance(survey_rows, list):
                                picked = _pick_cost_from_rows(survey_rows, crop)
                                if picked is not None and picked > 0:
                                    year_costs[crop] = round(float(picked), 2)
                        except Exception:
                            continue

                # Final fallback: if this is ERS URL and report missing, inject report term from env/query.
                if not year_costs and "/api.ers.usda.gov/" in year_url and "/arms/surveydata" in year_url:
                    report_name = os.environ.get("ERS_ARMS_REPORT", query.get("report", "income statement")).strip()
                    repaired_url = _with_params(year_url, {"report": report_name})
                    repaired_payload = cached_json(
                        namespace="costs",
                        key={"url": repaired_url, "crops": sorted(crops), "year": year, "kind": "repaired"},
                        fetcher=lambda u=repaired_url: _request_json(u, {}),
                        ttl_hours=24,
                        force_refresh=force_refresh,
                    )
                    year_costs = _extract_cost_rows(repaired_payload, crops)

                for crop, value in year_costs.items():
                    by_crop_series[crop].append(value)
                year_hits[year] = len(year_costs)

            for crop in crops:
                vals = by_crop_series.get(crop, [])
                if vals:
                    avg = round(float(sum(vals) / len(vals)), 2)
                    if _is_plausible_cost(crop, avg):
                        costs[crop] = avg

            if costs:
                print(f"[agent][tool] costs source=api years={years} year_hits={year_hits}", flush=True)
            else:
                print(f"[agent][tool] costs source=api_empty years={years} year_hits={year_hits}", flush=True)
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
        elif not _is_plausible_cost(crop, costs[crop]):
            known = DEFAULT_COSTS.get(crop.lower())
            costs[crop] = known if known is not None else round(_seeded_float(f"cost:{crop}", 550.0, 2200.0), 2)

    save_parquet(
        parquet_path,
        pd.DataFrame(
            [{"crop": crop, "cost_per_acre": value} for crop, value in costs.items()]
        ),
    )
    return costs
