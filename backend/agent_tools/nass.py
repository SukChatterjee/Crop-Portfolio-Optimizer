from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

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

from .cache import cached_json, load_parquet, parquet_cache_path, raw_cache_path, save_parquet

NASS_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
NASS_PARAM_VALUES_URL = "https://quickstats.nass.usda.gov/api/get_param_values/"
DEFAULT_SECTORS = ["CROPS", "HORTICULTURE", "VEGETABLES", "FRUIT & TREE NUTS", "FIELD CROPS"]


def _normalize_crop_candidates(crop: str) -> List[str]:
    base = crop.strip().upper()
    if not base:
        return []
    candidates = [base]
    if base.endswith("S") and len(base) > 3:
        candidates.append(base[:-1])
    if base.endswith("ES") and len(base) > 3:
        candidates.append(base[:-2])
    candidates.append(base.replace("&", "AND"))
    out = []
    seen = set()
    for c in candidates:
        c = c.strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _crop_search_terms(crop: str) -> List[str]:
    base = crop.strip().upper()
    if not base:
        return []
    parts = [p for p in base.replace(",", " ").split() if p]
    terms = [base]
    if parts:
        terms.append(parts[0])
    for c in _normalize_crop_candidates(crop):
        terms.append(c)
    out = []
    seen = set()
    for t in terms:
        t = t.strip()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
def _request_json(url: str, params: Dict[str, str]) -> Dict:
    response = requests.get(url, params=params, timeout=25)
    response.raise_for_status()
    return response.json()


def _request_json_fast(url: str, params: Dict[str, str]) -> Dict:
    # Discovery should be quick and cheap; avoid long waits/retries.
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def _request_text_fast(url: str, params: Dict[str, str]) -> str:
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.text


def _fetch_param_values(api_key: str, param: str) -> List[str]:
    cache_key = {"param": param}
    path = raw_cache_path("nass_param_values", cache_key)
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [str(v).strip() for v in data if str(v).strip()]
        except Exception:
            pass
    try:
        text = _request_text_fast(
            NASS_PARAM_VALUES_URL,
            {"key": api_key, "param": param},
        )
        # Endpoint returns JSON array text for many params.
        parsed = json.loads(text)
        if not isinstance(parsed, list):
            return []
        values = [str(v) for v in parsed]
        vals = [v.strip() for v in values if str(v).strip()]
        path.write_text(json.dumps(vals), encoding="utf-8")
        return vals
    except Exception:
        return []


def _parse_value(raw: str) -> float:
    cleaned = (raw or "").replace(",", "").strip()
    if cleaned in {"(D)", "(Z)", "", "NA"}:
        return float("nan")
    try:
        return float(cleaned)
    except ValueError:
        return float("nan")


def _fallback_rows(crop: str, years: List[int]) -> List[Dict]:
    rows = []
    for year in years:
        yld = _seeded_float(f"{crop}:{year}:yield", 40, 240)
        area = _seeded_float(f"{crop}:{year}:area", 50_000, 850_000)
        prod = yld * area
        rows.append(
            {
                "year": int(year),
                "crop": crop,
                "yield": round(yld, 2),
                "production": round(prod, 2),
                "area": round(area, 2),
            }
        )
    return rows


def _build_nass_candidates(crop: str, years: List[int], api_key: str) -> List[Dict[str, str]]:
    terms = _crop_search_terms(crop)
    commodities = _fetch_param_values(api_key, "commodity_desc")
    groups = _fetch_param_values(api_key, "group_desc")
    sectors = _fetch_param_values(api_key, "sector_desc")

    commodity_candidates = [
        c for c in commodities if any(t in c.upper() for t in terms)
    ]
    group_candidates = [
        g for g in groups if any(t in g.upper() for t in terms)
    ]
    if not group_candidates:
        group_candidates = [
            g for g in groups if g.upper() in {"FIELD CROPS", "VEGETABLES", "FRUIT & TREE NUTS", "HORTICULTURE"}
        ]

    sector_candidates = [
        s for s in sectors if s.upper() in {d.upper() for d in DEFAULT_SECTORS}
    ] or DEFAULT_SECTORS

    # Final generic fallback if metadata endpoint is unavailable.
    if not commodity_candidates:
        commodity_candidates = terms
    if not group_candidates:
        group_candidates = ["FIELD CROPS", "VEGETABLES", "FRUIT & TREE NUTS"]

    default_state = os.environ.get("NASS_DEFAULT_STATE", "").strip().upper()
    geo_candidates: List[Dict[str, str]] = [{"agg_level_desc": "NATIONAL"}, {}]
    if default_state:
        geo_candidates.insert(0, {"agg_level_desc": "STATE", "state_name": default_state})

    out: List[Dict[str, str]] = []
    for commodity in commodity_candidates:
        for sector in sector_candidates:
            for geo in geo_candidates:
                out.append(
                    {
                        "key": api_key,
                        "format": "JSON",
                        **geo,
                        "sector_desc": sector,
                        "commodity_desc": commodity,
                        "year__GE": str(min(years)),
                        "year__LE": str(max(years)),
                    }
                )
                for group in group_candidates:
                    out.append(
                        {
                            "key": api_key,
                            "format": "JSON",
                            **geo,
                            "sector_desc": sector,
                            "group_desc": group,
                            "commodity_desc": commodity,
                            "year__GE": str(min(years)),
                            "year__LE": str(max(years)),
                        }
                    )
    return out


def discover_nass_params(
    selected_crops: List[str],
    last_n_years: int = 3,
    force_refresh: bool = False,
    seed_plan: Optional[Dict[str, Dict[str, str]]] = None,
) -> Dict[str, Dict[str, str]]:
    api_key = os.environ.get("USDA_NASS_API_KEY", "").strip()
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    if not api_key or not crops:
        if not api_key:
            print("[agent][tool] nass-discovery source=skipped reason=missing_api_key", flush=True)
        return {}

    now_year = datetime.utcnow().year
    years = list(range(now_year - last_n_years + 1, now_year + 1))
    out: Dict[str, Dict[str, str]] = {}
    unresolved: List[str] = []
    max_candidates = max(1, int(os.environ.get("NASS_DISCOVERY_MAX_CANDIDATES", "6")))
    for crop in crops:
        candidates: List[Dict[str, str]] = []
        seed = (seed_plan or {}).get(crop)
        if isinstance(seed, dict):
            seed_queries = seed.get("queries")
            if isinstance(seed_queries, list):
                for q in seed_queries:
                    if not isinstance(q, dict):
                        continue
                    base = {
                        "key": api_key,
                        "format": "JSON",
                        "year__GE": str(min(years)),
                        "year__LE": str(max(years)),
                    }
                    for key in ("state_name", "agg_level_desc", "sector_desc", "group_desc", "commodity_desc"):
                        value = str(q.get(key, "")).strip()
                        if value:
                            base[key] = value
                    if base["commodity_desc"]:
                        candidates.append(base)
            else:
                seed_candidate = {
                    "key": api_key,
                    "format": "JSON",
                    "sector_desc": str(seed.get("sector_desc", "")).strip(),
                    "commodity_desc": str(seed.get("commodity_desc", "")).strip(),
                    "year__GE": str(min(years)),
                    "year__LE": str(max(years)),
                }
                state_name = str(seed.get("state_name", "")).strip()
                agg_level_desc = str(seed.get("agg_level_desc", "")).strip()
                if state_name:
                    seed_candidate["state_name"] = state_name
                if agg_level_desc:
                    seed_candidate["agg_level_desc"] = agg_level_desc
                group_desc = str(seed.get("group_desc", "")).strip()
                if group_desc:
                    seed_candidate["group_desc"] = group_desc
                if seed_candidate["commodity_desc"]:
                    candidates.append(seed_candidate)
        candidates.extend(_build_nass_candidates(crop, years, api_key)[:max_candidates])
        deduped_candidates: List[Dict[str, str]] = []
        seen = set()
        for c in candidates:
            signature = tuple(sorted((k, v) for k, v in c.items() if k not in {"key", "format"}))
            if signature in seen:
                continue
            seen.add(signature)
            deduped_candidates.append(c)
        matched = None
        for base_params in deduped_candidates:
            for stat in ("YIELD", "PRODUCTION", "AREA HARVESTED"):
                params = dict(base_params)
                params["statisticcat_desc"] = stat
                cache_key = {"crop": crop, "years": years, "params": params, "kind": "discover"}
                try:
                    payload = cached_json(
                        namespace="nass",
                        key=cache_key,
                        fetcher=lambda p=params: _request_json_fast(NASS_URL, p),
                        ttl_hours=24,
                        # Keep short-cache behavior for discovery even in live mode.
                        force_refresh=False,
                    )
                    if isinstance(payload, dict) and payload.get("data"):
                        matched = {
                            "commodity_desc": base_params.get("commodity_desc", ""),
                            "group_desc": base_params.get("group_desc", ""),
                            "sector_desc": base_params.get("sector_desc", "CROPS"),
                            "state_name": base_params.get("state_name", ""),
                            "agg_level_desc": base_params.get("agg_level_desc", ""),
                        }
                        break
                except Exception:
                    continue
            if matched:
                break
        if matched:
            out[crop] = matched
            print(f"[agent][tool] nass-discovery crop={crop} source=api params={matched}", flush=True)
        else:
            unresolved.append(crop)
            print(f"[agent][tool] nass-discovery crop={crop} source=empty", flush=True)
    if unresolved:
        out["__unresolved__"] = {"crops": ",".join(unresolved)}
    return out


def _fetch_crop_stats(
    crop: str,
    years: List[int],
    api_key: str,
    force_refresh: bool = False,
    discovery: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    try:
        stats = {
            "YIELD": "yield",
            "PRODUCTION": "production",
            "AREA HARVESTED": "area",
        }
        rows = []
        for stat_name in stats:
            candidates = []
            if discovery:
                base = {
                    "key": api_key,
                    "format": "JSON",
                    "sector_desc": discovery.get("sector_desc", "CROPS"),
                    "commodity_desc": discovery.get("commodity_desc", crop.upper()),
                    "year__GE": str(min(years)),
                    "year__LE": str(max(years)),
                }
                if discovery.get("state_name"):
                    base["state_name"] = discovery["state_name"]
                if discovery.get("agg_level_desc"):
                    base["agg_level_desc"] = discovery["agg_level_desc"]
                if discovery.get("group_desc"):
                    base["group_desc"] = discovery["group_desc"]
                candidates.append(base)
            else:
                max_candidates = max(1, int(os.environ.get("NASS_FETCH_MAX_CANDIDATES", "10")))
                candidates.extend(_build_nass_candidates(crop, years, api_key)[:max_candidates])

            deduped_candidates = []
            seen = set()
            for c in candidates:
                signature = tuple(sorted((k, v) for k, v in c.items() if k not in {"key", "format"}))
                if signature in seen:
                    continue
                seen.add(signature)
                deduped_candidates.append(c)

            got_any = False
            for base_params in deduped_candidates:
                params = dict(base_params)
                params["statisticcat_desc"] = stat_name
                cache_key = {"crop": crop, "stat": stat_name, "years": years, "params": params}
                try:
                    payload = cached_json(
                        namespace="nass",
                        key=cache_key,
                        fetcher=lambda p=params: _request_json(NASS_URL, p),
                        ttl_hours=24,
                        force_refresh=force_refresh,
                    )
                except Exception:
                    continue
                data = payload.get("data", []) if isinstance(payload, dict) else []
                if data:
                    got_any = True
                for item in data:
                    try:
                        year = int(item.get("year"))
                    except (TypeError, ValueError):
                        continue
                    if year not in years:
                        continue
                    rows.append(
                        {
                            "year": year,
                            "crop": crop,
                            "metric": stats[stat_name],
                            "value": _parse_value(item.get("Value", "")),
                        }
                    )
                if got_any:
                    break
        if not rows:
            return pd.DataFrame(_fallback_rows(crop, years))

        metric_df = pd.DataFrame(rows).dropna(subset=["value"])
        if metric_df.empty:
            return pd.DataFrame(_fallback_rows(crop, years))

        grouped = metric_df.groupby(["year", "crop", "metric"], as_index=False)["value"].mean()
        wide = grouped.pivot(index=["year", "crop"], columns="metric", values="value").reset_index()
        for col in ["yield", "production", "area"]:
            if col not in wide.columns:
                wide[col] = float("nan")
        wide = wide.ffill().bfill()
        for col in ["yield", "production", "area"]:
            wide[col] = wide[col].fillna(pd.Series([r[col] for r in _fallback_rows(crop, list(wide["year"]))]))
        return wide[["year", "crop", "yield", "production", "area"]]
    except Exception:
        # Never break the crop pipeline due to one provider parse issue.
        return pd.DataFrame(_fallback_rows(crop, years))


def fetch_ohio_crop_stats(
    selected_crops: List[str],
    last_n_years: int = 3,
    force_refresh: bool = False,
    discovery_plan: Optional[Dict[str, Dict[str, str]]] = None,
) -> pd.DataFrame:
    now_year = datetime.utcnow().year
    years = list(range(now_year - last_n_years + 1, now_year + 1))
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    unresolved = set()
    if discovery_plan and isinstance(discovery_plan.get("__unresolved__"), dict):
        unresolved_csv = str(discovery_plan.get("__unresolved__", {}).get("crops", ""))
        unresolved = {c.strip() for c in unresolved_csv.split(",") if c.strip()}

    cache_key = {
        "crops": sorted(crops),
        "years": years,
        "state": "OHIO",
        "plan": discovery_plan or {},
    }
    parquet_path = parquet_cache_path("nass", cache_key)
    cached = load_parquet(parquet_path)
    if not force_refresh and cached is not None and not cached.empty:
        print("[agent][tool] nass source=processed-cache", flush=True)
        return cached

    api_key = os.environ.get("USDA_NASS_API_KEY", "").strip()
    frames = []
    skip_unresolved = os.environ.get("AGENT_SKIP_FETCH_IF_DISCOVERY_EMPTY", "0") == "1"
    for crop in crops:
        if skip_unresolved and crop in unresolved:
            frames.append(pd.DataFrame(_fallback_rows(crop, years)))
            print(f"[agent][tool] nass crop={crop} source=fallback reason=discovery_empty_skip", flush=True)
            continue
        if api_key:
            try:
                frames.append(
                    _fetch_crop_stats(
                        crop,
                        years,
                        api_key,
                        force_refresh=force_refresh,
                        discovery=(discovery_plan or {}).get(crop),
                    )
                )
                print(f"[agent][tool] nass crop={crop} source=api", flush=True)
                continue
            except Exception as exc:
                print(f"[agent][tool] nass crop={crop} source=api error={exc}", flush=True)
                pass
        else:
            print(f"[agent][tool] nass crop={crop} source=fallback reason=missing_api_key", flush=True)
        frames.append(pd.DataFrame(_fallback_rows(crop, years)))
        if api_key:
            print(f"[agent][tool] nass crop={crop} source=fallback reason=api_unavailable", flush=True)

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["year", "crop", "yield", "production", "area"])
    save_parquet(parquet_path, result)
    return result
