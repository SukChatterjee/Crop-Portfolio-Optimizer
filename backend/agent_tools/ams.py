from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from .auth import build_ams_auth
from .cache import get_cache_dir

AMS_BASE = "https://marsapi.ams.usda.gov/services/v1.2"
CATALOG_TTL_DAYS = 7
MAX_SLUG_IDS_PER_CROP = 10


def _ams_cache_dir() -> Path:
    d = get_cache_dir() / "ams"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _is_fresh(path: Path, ttl_days: int) -> bool:
    if not path.exists():
        return False
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return modified >= datetime.now(timezone.utc) - timedelta(days=ttl_days)


def _load_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def _ams_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    auth_cfg = build_ams_auth()
    print(
        f"[agent][api-call] namespace=ams endpoint={url} params_keys={list((params or {}).keys())}",
        flush=True,
    )
    resp = requests.get(
        url,
        params=params or {},
        headers=auth_cfg.headers,
        auth=auth_cfg.auth,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _iter_dicts(node: Any):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _iter_dicts(v)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_dicts(item)


def _parse_date(raw: Any) -> Optional[str]:
    if not raw:
        return None
    text = str(raw).strip()
    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%m/%d/%Y")
            return dt.date().isoformat()
        except ValueError:
            return None
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    text = str(v).strip().replace(",", "")
    if not text:
        return None
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_range_numbers(text: str) -> Tuple[Optional[float], Optional[float]]:
    nums = re.findall(r"-?\d+(?:\.\d+)?", text or "")
    values: List[float] = []
    for n in nums:
        try:
            values.append(float(n))
        except ValueError:
            continue
    if len(values) >= 2:
        return values[0], values[1]
    return None, None


def _row_text(row: Dict[str, Any]) -> str:
    parts = []
    for k in (
        "slug_name",
        "report_title",
        "market_type",
        "market_location_name",
        "market_location_state",
        "state",
        "city",
        "commodity",
        "commodity_desc",
        "report_narrative",
    ):
        if row.get(k) is not None:
            parts.append(str(row.get(k)))
    return " ".join(parts).lower()


def _crop_aliases(crop_name: str) -> List[str]:
    c = str(crop_name or "").strip().lower()
    aliases = {c} if c else set()
    if c == "tomatoes":
        aliases.update({"tomato"})
    elif c == "tomato":
        aliases.update({"tomatoes"})
    elif c == "cotton":
        aliases.update({"cottonseed"})
    return [a for a in aliases if a]


def _contains_any_token(text: str, tokens: List[str]) -> bool:
    t = str(text or "").lower()
    if not t or not tokens:
        return False
    return any(tok in t for tok in tokens if tok)


def _normalize_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return [x for x in payload["results"] if isinstance(x, dict)]
        return [x for x in _iter_dicts(payload) if isinstance(x, dict)]
    return []


def _build_summary_from_series(series: List[Dict[str, Any]]) -> Dict[str, Any]:
    prices = [float(r["price_avg"]) for r in series if r.get("price_avg") is not None]
    if not prices:
        return {"p10": None, "p50": None, "p90": None, "unit_mode": None}
    s = pd.Series(prices, dtype=float)
    units = [str(r.get("unit", "")).strip() for r in series if str(r.get("unit", "")).strip()]
    unit_mode = Counter(units).most_common(1)[0][0] if units else None
    return {
        "p10": round(float(s.quantile(0.10)), 4),
        "p50": round(float(s.quantile(0.50)), 4),
        "p90": round(float(s.quantile(0.90)), 4),
        "unit_mode": unit_mode,
    }


def fetch_reports_catalog(force_refresh: bool = False) -> List[Dict[str, Any]]:
    cache_path = _ams_cache_dir() / "reports_catalog.json"
    if not force_refresh and _is_fresh(cache_path, CATALOG_TTL_DAYS):
        cached = _load_json(cache_path)
        if isinstance(cached, list):
            print("[agent][cache-hit] namespace=ams kind=reports_catalog", flush=True)
            return [x for x in cached if isinstance(x, dict)]

    payload = _ams_get(f"{AMS_BASE}/reports")
    rows = _normalize_rows(payload)
    _save_json(cache_path, rows)
    return rows


def fetch_report_details(slug_id: int, lookback_days: int) -> Dict[str, Any]:
    if not isinstance(slug_id, int):
        raise ValueError("slug_id must be int")
    if lookback_days > 1200:
        raise ValueError("lookback_days must be <= 1200")
    if lookback_days <= 0:
        lookback_days = 1095

    cache_path = _ams_cache_dir() / f"details_{slug_id}_{lookback_days}.json"
    cached = _load_json(cache_path)
    if cached is not None:
        print(f"[agent][cache-hit] namespace=ams kind=details slug_id={slug_id}", flush=True)
        if isinstance(cached, dict):
            return cached
        return {"results": _normalize_rows(cached)}

    payload = _ams_get(
        f"{AMS_BASE}/reports/{slug_id}/Details",
        params={"lastDays": int(lookback_days)},
    )
    if not isinstance(payload, dict):
        payload = {"results": _normalize_rows(payload)}
    _save_json(cache_path, payload)
    return payload


def parse_details_to_price_series(
    details_json: Dict[str, Any],
    crop_name: str,
    slug_id: int,
    parse_hints: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    crop = str(crop_name or "").strip()
    crop_l = crop.lower()
    crop_aliases = _crop_aliases(crop)
    hints = parse_hints or {}
    unit_regex = str(hints.get("acceptable_units_regex") or r"(\$\s*/\s*BU|\$/BU|BUSHEL|\$\s*/\s*CWT|\$/CWT|CWT)")
    fields_priority = hints.get("price_fields_priority")
    if not isinstance(fields_priority, list):
        fields_priority = ["weighted_avg", "weighted_average", "price", "low_price", "high_price", "range"]

    rows = _normalize_rows(details_json)
    out: List[Dict[str, Any]] = []
    for r in rows:
        text = _row_text(r)
        if crop_l and not _contains_any_token(text, crop_aliases):
            # soft filter: if commodity-like fields are absent, keep row.
            commodity = str(r.get("commodity") or r.get("commodity_desc") or "").strip().lower()
            if commodity and not _contains_any_token(commodity, crop_aliases):
                continue

        date = (
            _parse_date(r.get("report_date"))
            or _parse_date(r.get("report_begin_date"))
            or _parse_date(r.get("published_Date"))
            or _parse_date(r.get("published_date"))
        )
        if not date:
            continue

        unit = str(r.get("unit") or r.get("units") or r.get("uom") or "").strip()
        if not unit:
            # infer from narrative/report text
            narrative = " ".join([str(r.get("report_narrative") or ""), str(r.get("report_title") or "")])
            m = re.search(unit_regex, narrative, flags=re.IGNORECASE)
            if m:
                unit = m.group(0)

        price_avg = None
        price_low = None
        price_high = None

        # preferred numeric fields
        for key in fields_priority:
            if key in {"low_price", "high_price", "range"}:
                continue
            if key in r:
                price_avg = _to_float(r.get(key))
                if price_avg is not None:
                    break

        if price_avg is None:
            low_keys = ["low_price", "low", "min_price", "price_low"]
            high_keys = ["high_price", "high", "max_price", "price_high"]
            for k in low_keys:
                if k in r:
                    price_low = _to_float(r.get(k))
                    if price_low is not None:
                        break
            for k in high_keys:
                if k in r:
                    price_high = _to_float(r.get(k))
                    if price_high is not None:
                        break
            if price_low is not None and price_high is not None:
                price_avg = round((price_low + price_high) / 2.0, 6)

        if price_avg is None:
            range_text = str(r.get("range") or r.get("price_range") or r.get("report_narrative") or "")
            lo, hi = _extract_range_numbers(range_text)
            if lo is not None and hi is not None:
                price_low = lo
                price_high = hi
                price_avg = round((lo + hi) / 2.0, 6)

        if price_avg is None:
            continue

        out.append(
            {
                "date": date,
                "crop": crop,
                "slug_id": int(slug_id),
                "market": str(
                    r.get("market_location_name")
                    or r.get("market_type")
                    or r.get("city")
                    or ""
                ).strip()
                or None,
                "unit": unit or None,
                "price_low": price_low,
                "price_high": price_high,
                "price_avg": float(price_avg),
            }
        )

    # Deduplicate by date + market + slug
    dedup: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
    for row in out:
        key = (row["date"], str(row.get("market") or ""), int(row["slug_id"]))
        if key not in dedup:
            dedup[key] = row
    ordered = sorted(dedup.values(), key=lambda x: x["date"])
    return ordered


def _catalog_row_text(row: Dict[str, Any]) -> str:
    parts = [
        str(row.get("slug_name") or ""),
        str(row.get("report_title") or ""),
        str(row.get("markets") or ""),
        str(row.get("market_types") or ""),
        str(row.get("offices") or ""),
    ]
    return " ".join(parts).lower()


def _published_ts(row: Dict[str, Any]) -> float:
    text = str(row.get("published_date") or "").strip()
    if not text:
        return 0.0
    parsed = _parse_date(text)
    if not parsed:
        return 0.0
    try:
        return datetime.strptime(parsed, "%Y-%m-%d").timestamp()
    except ValueError:
        return 0.0


def _apply_catalog_filters(catalog: List[Dict[str, Any]], filters: Dict[str, Any], max_candidates: int) -> List[Dict[str, Any]]:
    contains_any = [str(x).strip().lower() for x in (filters.get("contains_any") or []) if str(x).strip()]
    exclude_any = [str(x).strip().lower() for x in (filters.get("exclude_any") or []) if str(x).strip()]

    out: List[Tuple[int, float, Dict[str, Any]]] = []
    for row in catalog:
        text = _catalog_row_text(row)
        if contains_any and not any(t in text for t in contains_any):
            continue
        if exclude_any and any(t in text for t in exclude_any):
            continue
        score = sum(1 for t in contains_any if t in text)
        out.append((score, _published_ts(row), row))

    out.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [x[2] for x in out[:max_candidates]]


def _fallback_filters_for_crop(crop_name: str) -> Dict[str, Any]:
    c = str(crop_name or "").strip().lower()
    if c in {"tomatoes", "tomato"}:
        return {"contains_any": ["tomato", "shipping point", "fob"], "exclude_any": []}
    if c == "cotton":
        return {"contains_any": ["cotton", "spot", "market"], "exclude_any": []}
    return {"contains_any": [c], "exclude_any": []}


def get_prices_for_crop_with_plan(
    crop_name: str,
    state_name: str,
    lookback_days: int,
    planner_json: Dict[str, Any],
) -> Dict[str, Any]:
    crop = str(crop_name or "").strip()
    if not crop:
        return {"status": "no_data", "crop": crop_name, "reason": "missing_crop_name"}
    if lookback_days > 1200:
        lookback_days = 1200

    plan = planner_json.get("plan") if isinstance(planner_json, dict) else None
    if isinstance(plan, dict) and plan.get("status") == "no_data":
        return {
            "status": "no_data",
            "crop": crop,
            "chosen_slug_id": None,
            "candidates_tested": [],
            "series": [],
            "summary": {"p10": None, "p50": None, "p90": None, "unit_mode": None},
            "reason": str(plan.get("reason") or "planner_no_data"),
        }

    plan = plan or {}
    catalog_query = plan.get("catalog_query") if isinstance(plan.get("catalog_query"), dict) else {}
    filters = catalog_query.get("filters") if isinstance(catalog_query.get("filters"), dict) else {}
    max_candidates = int(catalog_query.get("max_candidates") or 30)
    max_candidates = min(100, max(1, max_candidates))
    parse_hints = planner_json.get("parse_hints") if isinstance(planner_json.get("parse_hints"), dict) else {}
    unit_regex = str(parse_hints.get("acceptable_units_regex") or "")

    try:
        catalog = fetch_reports_catalog(force_refresh=False)
    except Exception as exc:
        return {
            "status": "no_data",
            "crop": crop,
            "chosen_slug_id": None,
            "candidates_tested": [],
            "series": [],
            "summary": {"p10": None, "p50": None, "p90": None, "unit_mode": None},
            "reason": f"catalog_error:{exc}",
        }

    candidates = _apply_catalog_filters(catalog, filters, max_candidates=max_candidates)
    candidate_slug_ids: List[int] = []
    for row in candidates:
        raw_sid = row.get("slug_id") or row.get("report_id") or row.get("slugId")
        try:
            sid = int(raw_sid)
        except (TypeError, ValueError):
            continue
        if sid not in candidate_slug_ids:
            candidate_slug_ids.append(sid)
        if len(candidate_slug_ids) >= MAX_SLUG_IDS_PER_CROP:
            break

    if not candidate_slug_ids:
        return {
            "status": "no_data",
            "crop": crop,
            "chosen_slug_id": None,
            "candidates_tested": [],
            "series": [],
            "summary": {"p10": None, "p50": None, "p90": None, "unit_mode": None},
            "reason": "no_candidate_slug_ids",
        }

    # Single-run dedupe for detail calls.
    detail_cache: Dict[int, Dict[str, Any]] = {}
    tested: List[Dict[str, Any]] = []
    best_sid: Optional[int] = None
    best_series: List[Dict[str, Any]] = []
    best_score = (-1, -1)  # (parsed_count, unit_match)

    unit_pattern = re.compile(unit_regex, flags=re.IGNORECASE) if unit_regex else None

    for sid in candidate_slug_ids[:MAX_SLUG_IDS_PER_CROP]:
        if sid in detail_cache:
            details = detail_cache[sid]
        else:
            try:
                details = fetch_report_details(sid, lookback_days=lookback_days)
            except Exception:
                tested.append({"slug_id": sid, "parsed_count": 0, "unit_mode": None})
                continue
            detail_cache[sid] = details

        series = parse_details_to_price_series(details, crop, sid, parse_hints=parse_hints)
        parsed_count = len(series)
        units = [str(r.get("unit", "")).strip() for r in series if str(r.get("unit", "")).strip()]
        unit_mode = Counter(units).most_common(1)[0][0] if units else None
        unit_match = 1 if (unit_mode and unit_pattern and unit_pattern.search(unit_mode)) else 0

        tested.append({"slug_id": sid, "parsed_count": parsed_count, "unit_mode": unit_mode})
        score = (parsed_count, unit_match)
        if score > best_score:
            best_score = score
            best_sid = sid
            best_series = series

    if best_sid is None or not best_series:
        # Fallback path: planner filters can overfit to state words and miss valid
        # slugs (especially for tomatoes). Retry with relaxed crop-centric filters.
        retry_filters = _fallback_filters_for_crop(crop)
        retry_candidates = _apply_catalog_filters(catalog, retry_filters, max_candidates=max_candidates)
        retry_slug_ids: List[int] = []
        for row in retry_candidates:
            raw_sid = row.get("slug_id") or row.get("report_id") or row.get("slugId")
            try:
                sid = int(raw_sid)
            except (TypeError, ValueError):
                continue
            if sid not in retry_slug_ids:
                retry_slug_ids.append(sid)
            if len(retry_slug_ids) >= MAX_SLUG_IDS_PER_CROP:
                break

        for sid in retry_slug_ids:
            if sid in detail_cache:
                details = detail_cache[sid]
            else:
                try:
                    details = fetch_report_details(sid, lookback_days=lookback_days)
                except Exception:
                    continue
                detail_cache[sid] = details
            series = parse_details_to_price_series(details, crop, sid, parse_hints=parse_hints)
            parsed_count = len(series)
            units = [str(r.get("unit", "")).strip() for r in series if str(r.get("unit", "")).strip()]
            unit_mode = Counter(units).most_common(1)[0][0] if units else None
            unit_match = 1 if (unit_mode and unit_pattern and unit_pattern.search(unit_mode)) else 0
            tested.append({"slug_id": sid, "parsed_count": parsed_count, "unit_mode": unit_mode, "fallback": True})
            score = (parsed_count, unit_match)
            if score > best_score:
                best_score = score
                best_sid = sid
                best_series = series

    if best_sid is None or not best_series:
        return {
            "status": "no_data",
            "crop": crop,
            "chosen_slug_id": None,
            "candidates_tested": tested,
            "series": [],
            "summary": {"p10": None, "p50": None, "p90": None, "unit_mode": None},
            "reason": "no_parsed_rows",
        }

    summary = _build_summary_from_series(best_series)
    return {
        "status": "ok",
        "crop": crop,
        "chosen_slug_id": int(best_sid),
        "candidates_tested": tested,
        "series": best_series,
        "summary": summary,
    }
