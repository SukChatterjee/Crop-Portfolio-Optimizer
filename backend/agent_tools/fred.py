from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from .cache import cached_json

FRED_SERIES_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_LOOKBACK_YEARS = 5


def _request_json(params: Dict[str, str]) -> Dict[str, Any]:
    response = requests.get(FRED_SERIES_OBSERVATIONS_URL, params=params, timeout=25)
    response.raise_for_status()
    return response.json()


def _coerce_float(value: Any) -> Optional[float]:
    try:
        text = str(value).strip()
        if not text or text == ".":
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _normalize_queries(plan: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    queries = (plan or {}).get("query_candidates") if isinstance(plan, dict) else None
    if not isinstance(queries, list):
        return []

    out: List[Dict[str, str]] = []
    seen = set()
    for item in queries:
        if not isinstance(item, dict):
            continue
        series_id = str(item.get("series_id") or "").strip().upper()
        if not series_id or series_id in seen:
            continue
        seen.add(series_id)
        out.append(
            {
                "series_id": series_id,
                "title": str(item.get("title") or "").strip(),
                "purpose": str(item.get("purpose") or "").strip(),
                "units": str(item.get("units") or "lin").strip() or "lin",
                "frequency": str(item.get("frequency") or "").strip(),
                "aggregation_method": str(item.get("aggregation_method") or "avg").strip() or "avg",
                "observation_start": str(item.get("observation_start") or "").strip(),
                "observation_end": str(item.get("observation_end") or "").strip(),
            }
        )
    return out


def _default_observation_start(plan: Optional[Dict[str, Any]]) -> str:
    try:
        lookback_years = int((plan or {}).get("lookback_years") or DEFAULT_LOOKBACK_YEARS)
    except (TypeError, ValueError):
        lookback_years = DEFAULT_LOOKBACK_YEARS
    lookback_years = max(1, min(lookback_years, 15))
    today = datetime.utcnow().date()
    return today.replace(year=today.year - lookback_years).isoformat()


def _build_summary(series_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not series_rows:
        return {"series_count": 0, "features": {}}

    features: Dict[str, Any] = {}
    for row in series_rows:
        series_id = str(row.get("series_id") or "").strip().upper()
        latest = row.get("latest_value")
        change_pct = row.get("latest_change_pct")
        if latest is not None:
            features[f"{series_id}_latest"] = latest
        if change_pct is not None:
            features[f"{series_id}_change_pct"] = change_pct

    return {
        "series_count": len(series_rows),
        "features": features,
        "series": [
            {
                "series_id": row.get("series_id"),
                "title": row.get("title"),
                "purpose": row.get("purpose"),
                "latest_value": row.get("latest_value"),
                "latest_change_pct": row.get("latest_change_pct"),
                "observation_count": row.get("observation_count"),
                "observation_start": row.get("observation_start"),
                "observation_end": row.get("observation_end"),
            }
            for row in series_rows
        ],
    }


def fetch_fred_series(
    plan: Optional[Dict[str, Any]],
    force_refresh: bool = False,
) -> Dict[str, Any]:
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    queries = _normalize_queries(plan)
    if not queries:
        print("[agent][tool] fred source=skipped reason=empty_plan", flush=True)
        return {"series": [], "summary": {"series_count": 0, "features": {}}, "status": "no_data"}
    if not api_key:
        print("[agent][tool] fred source=skipped reason=missing_api_key", flush=True)
        return {"series": [], "summary": {"series_count": 0, "features": {}}, "status": "no_data"}

    default_start = _default_observation_start(plan)
    out: List[Dict[str, Any]] = []
    for query in queries:
        params = {
            "api_key": api_key,
            "file_type": "json",
            "series_id": query["series_id"],
            "units": query["units"],
            "sort_order": "asc",
            "observation_start": query["observation_start"] or default_start,
        }
        if query.get("observation_end"):
            params["observation_end"] = query["observation_end"]
        if query.get("frequency"):
            params["frequency"] = query["frequency"]
        if query.get("aggregation_method"):
            params["aggregation_method"] = query["aggregation_method"]

        cache_key = {"series_id": query["series_id"], "params": params}
        try:
            payload = cached_json(
                namespace="fred",
                key=cache_key,
                fetcher=lambda p=params: _request_json(p),
                ttl_hours=12,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            print(f"[agent][tool] fred series_id={query['series_id']} source=error error={exc}", flush=True)
            continue

        observations = payload.get("observations", []) if isinstance(payload, dict) else []
        parsed_observations = []
        for obs in observations:
            if not isinstance(obs, dict):
                continue
            value = _coerce_float(obs.get("value"))
            if value is None:
                continue
            parsed_observations.append(
                {
                    "date": str(obs.get("date") or "").strip(),
                    "value": value,
                }
            )
        if not parsed_observations:
            print(f"[agent][tool] fred series_id={query['series_id']} source=no_data", flush=True)
            continue

        latest = parsed_observations[-1]["value"]
        previous = parsed_observations[-2]["value"] if len(parsed_observations) >= 2 else None
        change = (latest - previous) if previous is not None else None
        change_pct = ((change / previous) * 100.0) if previous not in {None, 0} and change is not None else None
        row = {
            "series_id": query["series_id"],
            "title": query["title"] or query["series_id"],
            "purpose": query["purpose"],
            "units": query["units"],
            "frequency": query["frequency"],
            "aggregation_method": query["aggregation_method"],
            "observation_start": parsed_observations[0]["date"],
            "observation_end": parsed_observations[-1]["date"],
            "observation_count": len(parsed_observations),
            "latest_value": round(float(latest), 4),
            "latest_change": round(float(change), 4) if change is not None else None,
            "latest_change_pct": round(float(change_pct), 4) if change_pct is not None else None,
            "observations": parsed_observations[-24:],
        }
        print(
            f"[agent][tool] fred series_id={query['series_id']} source=ok observations={len(parsed_observations)} latest={row['latest_value']}",
            flush=True,
        )
        out.append(row)

    status = "ok" if out else "no_data"
    return {"series": out, "summary": _build_summary(out), "status": status}
