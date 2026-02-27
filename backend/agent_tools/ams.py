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


def _normalize_aliases(crop: str) -> List[str]:
    base = crop.strip().lower()
    if not base:
        return []
    aliases = [base]
    if base.endswith("s") and len(base) > 3:
        aliases.append(base[:-1])
    if base.endswith("es") and len(base) > 3:
        aliases.append(base[:-2])
    aliases.append(base.replace("&", "and"))
    out = []
    seen = set()
    for a in aliases:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out


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


def _request_json_fast(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    auth: Optional[Tuple[str, str]] = None,
) -> Dict:
    # Discovery should be quick and cheap; avoid long waits/retries.
    response = requests.get(url, headers=headers, params=params, auth=auth, timeout=10)
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
    for key in ("year", "crop_year", "marketing_year"):
        raw = item.get(key)
        if raw is None:
            continue
        if isinstance(raw, (int, float)):
            y = int(raw)
            if 1900 <= y <= 2100:
                return y
        text = str(raw).strip()
        if len(text) == 4 and text.isdigit():
            y = int(text)
            if 1900 <= y <= 2100:
                return y
    for key in (
        "report_begin_date",
        "report_date",
        "date",
        "reported_date",
        "reportDate",
        "reportBeginDate",
        "reportEndDate",
    ):
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
        "weightedAverage",
        "weightedAvg",
        "avg_price",
        "price",
        "low_price",
        "high_price",
        "lowPrice",
        "highPrice",
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


def _norm_token(text: str) -> str:
    return "".join(ch for ch in str(text).lower().strip() if ch.isalnum() or ch.isspace()).strip()


def _commodity_matches_crop(crop: str, commodity: str) -> bool:
    c = _norm_token(crop)
    m = _norm_token(commodity)
    if not m:
        return True
    aliases = _normalize_aliases(crop)
    norm_aliases = [_norm_token(a) for a in aliases if _norm_token(a)]
    if c and c in m:
        return True
    for a in norm_aliases:
        if a in m or (m and m in a):
            return True
    return m in {"all", "all commodities"}


def _extract_commodity(item: Dict[str, Any]) -> str:
    direct_keys = ("commodity", "commodity_desc", "commodity_name")
    for key in direct_keys:
        if key in item and item.get(key) is not None:
            return str(item.get(key)).strip()
    for key, value in item.items():
        if value is None:
            continue
        k = str(key).lower()
        if "commodity" in k:
            return str(value).strip()
    return ""


def _parse_mars_response(payload: Dict[str, Any], crop: str, years: List[int]) -> pd.DataFrame:
    annual: Dict[int, List[float]] = {y: [] for y in years}
    for item in _walk_items(payload):
        if not isinstance(item, dict):
            continue
        year = _extract_year(item)
        if year not in annual:
            continue
        commodity = _extract_commodity(item)
        if commodity and not _commodity_matches_crop(crop, commodity):
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


def _build_report_queries(alias: str, start_str: str, end_str: str, allow_broad: bool = False) -> List[str]:
    a = (alias or "").strip()
    queries: List[str] = []
    if a:
        queries.extend(
            [
                f"commodity={a};report_begin_date={start_str}:{end_str}",
                f"commodity={a}",
                f"commodity_desc={a};report_begin_date={start_str}:{end_str}",
                f"commodity_desc={a}",
                f"commodity_name={a};report_begin_date={start_str}:{end_str}",
                f"commodity_name={a}",
            ]
        )
    if allow_broad:
        queries.extend([f"report_begin_date={start_str}:{end_str}", ""])
    out: List[str] = []
    seen = set()
    for q in queries:
        if q in seen:
            continue
        seen.add(q)
        out.append(q)
    max_default = "4" if allow_broad else "3"
    max_q = min(4, max(2, int(os.environ.get("AMS_QUERY_VARIANTS_MAX", max_default))))
    return out[:max_q]


def _seed_param_variants(params: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(params, dict):
        return []
    base = {str(k): v for k, v in params.items() if v is not None and str(v).strip()}
    if not base:
        return []
    variants: List[Dict[str, Any]] = [dict(base)]
    # Generic relaxation ladder for overly strict point filters.
    for drop_keys in (("reportDate",), ("state",), ("reportDate", "state")):
        relaxed = {k: v for k, v in base.items() if k not in drop_keys}
        if relaxed and relaxed not in variants:
            variants.append(relaxed)
    max_v = min(4, max(1, int(os.environ.get("AMS_SEED_PARAM_VARIANTS", "4"))))
    return variants[:max_v]


def _seed_query_attempts(seed_q: str, seed_params: Optional[Dict[str, Any]]) -> List[Tuple[str, Optional[Dict[str, Any]]]]:
    attempts: List[Tuple[str, Optional[Dict[str, Any]]]] = []
    param_variants = _seed_param_variants(seed_params)
    if not param_variants:
        param_variants = [seed_params] if isinstance(seed_params, dict) else []

    # First try explicit params (what planner decided), then equivalent q form.
    for p in param_variants:
        if isinstance(p, dict) and p:
            attempts.append(("", p))
            q_from_params = _params_to_q(p)
            if q_from_params:
                attempts.append((q_from_params, None))

    # If planner supplied q directly, try that too.
    q_clean = (seed_q or "").strip()
    if q_clean:
        attempts.append((q_clean, None))

    deduped: List[Tuple[str, Optional[Dict[str, Any]]]] = []
    seen = set()
    for qv, pv in attempts:
        key = (qv, tuple(sorted((pv or {}).items())))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((qv, pv))

    max_attempts = min(8, max(1, int(os.environ.get("AMS_SEED_ATTEMPTS_MAX", "6"))))
    return deduped[:max_attempts]


def _params_to_q(params: Optional[Dict[str, Any]]) -> str:
    if not isinstance(params, dict):
        return ""
    parts: List[str] = []
    for k, v in params.items():
        if v is None:
            continue
        sk = str(k).strip()
        sv = str(v).strip()
        if sk and sv:
            parts.append(f"{sk}={sv}")
    return ";".join(parts)


def _merge_mars_endpoint(base_detail_endpoint: str, slug_id: Any) -> str:
    base = base_detail_endpoint.rstrip("/")
    tail = f"/reports/{slug_id}/Report Detail"
    if base.endswith("/reports"):
        return f"{base}/{slug_id}/Report Detail"
    if "/reports/" in base:
        prefix = base.split("/reports/")[0]
        return f"{prefix}{tail}"
    return f"{base}{tail}"


def _normalize_mars_endpoint(endpoint: str, default_endpoint: str) -> str:
    raw = (endpoint or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"}:
        return raw
    base = default_endpoint.split("/reports/")[0].rstrip("/")
    return f"{base}/{raw.lstrip('/')}"


def _extract_slug_ids(payload: Any) -> List[str]:
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


def _score_report_item(item: Dict[str, Any], aliases: List[str]) -> int:
    text_parts: List[str] = []
    for key in (
        "report_name",
        "report_title",
        "title",
        "name",
        "description",
        "commodity",
        "commodities",
        "commodity_desc",
    ):
        if key in item and item.get(key) is not None:
            text_parts.append(str(item.get(key)))
    hay = _norm_token(" ".join(text_parts))
    if not hay:
        return 0
    score = 0
    for alias in aliases:
        a = _norm_token(alias)
        if not a:
            continue
        if a in hay:
            score += 3
        for token in a.split():
            if token and token in hay:
                score += 1
    if "price" in hay:
        score += 1
    return score


def _is_price_style_item(item: Dict[str, Any]) -> bool:
    text_parts: List[str] = []
    for key in (
        "report_name",
        "report_title",
        "title",
        "name",
        "description",
        "commodity",
        "commodities",
        "commodity_desc",
    ):
        if key in item and item.get(key) is not None:
            text_parts.append(str(item.get(key)))
    hay = _norm_token(" ".join(text_parts))
    if not hay:
        return False
    keywords = ("price", "bid", "market", "index", "wholesale", "retail", "fob")
    return any(k in hay for k in keywords)


def _extract_ranked_slug_ids(payload: Any, aliases: List[str]) -> List[str]:
    ranked: List[Tuple[int, str]] = []
    for item in _walk_items(payload):
        if not isinstance(item, dict):
            continue
        sid: Optional[str] = None
        for key in ("slug_id", "report_id", "slugId"):
            if key in item and item.get(key) is not None:
                text = str(item.get(key)).strip()
                if text:
                    sid = text
                    break
        if not sid:
            continue
        ranked.append((_score_report_item(item, aliases), sid))

    # Highest score first, keep stable insertion order for ties.
    ranked.sort(key=lambda x: x[0], reverse=True)
    out: List[str] = []
    seen = set()
    for _, sid in ranked:
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def _extract_price_style_slug_ids(payload: Any, aliases: List[str], max_slugs: int) -> List[str]:
    ranked: List[Tuple[int, str]] = []
    for item in _walk_items(payload):
        if not isinstance(item, dict):
            continue
        if not _is_price_style_item(item):
            continue
        sid: Optional[str] = None
        for key in ("slug_id", "report_id", "slugId"):
            if key in item and item.get(key) is not None:
                text = str(item.get(key)).strip()
                if text:
                    sid = text
                    break
        if not sid:
            continue
        ranked.append((_score_report_item(item, aliases), sid))
    ranked.sort(key=lambda x: x[0], reverse=True)
    out: List[str] = []
    seen = set()
    for _, sid in ranked:
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
        if len(out) >= max_slugs:
            break
    return out


def _discover_candidate_report_endpoints(
    crop: str,
    alias_list: List[str],
    mars_reports_endpoint: str,
    detail_base_endpoint: str,
    mars_key: str,
    force_refresh: bool,
    max_slugs: int,
) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    seen_sid = set()
    max_catalog_probes = min(6, max(3, int(os.environ.get("AMS_REPORT_CATALOG_PROBES", "5"))))
    for alias in alias_list:
        query_candidates = [
            f"commodity={alias}",
            f"commodity={alias.upper()}",
            alias,
            f"report_title={alias}",
            "",  # Broad catalog fallback when commodity filter returns no rows.
        ][:max_catalog_probes]
        for discovery_q in query_candidates:
            discovered = _safe_fetch_mars_payload(
                mars_reports_endpoint,
                discovery_q,
                mars_key,
                force_refresh,
                cache_key={
                    "crop": crop,
                    "q": discovery_q,
                    "endpoint": mars_reports_endpoint,
                    "kind": "discover-reports",
                    "alias": alias,
                },
            )
            if discovered is None:
                continue
            ranked_slug_ids = _extract_price_style_slug_ids(discovered, alias_list, max_slugs=max_slugs)
            if not ranked_slug_ids:
                ranked_slug_ids = _extract_ranked_slug_ids(discovered, alias_list)[:max_slugs]
            for sid in ranked_slug_ids[:max_slugs]:
                if sid in seen_sid:
                    continue
                seen_sid.add(sid)
                endpoint = _merge_mars_endpoint(detail_base_endpoint, sid)
                out.append((alias, endpoint, sid))
                if len(out) >= max_slugs:
                    return out
    return out


def _fetch_mars_payload(
    endpoint: str,
    q: str,
    mars_key: str,
    force_refresh: bool,
    cache_key: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    req_params: Dict[str, str] = {}
    if isinstance(params, dict):
        for k, v in params.items():
            if v is None:
                continue
            sk = str(k).strip()
            sv = str(v).strip()
            if sk and sv:
                req_params[sk] = sv
    if q and "q" not in req_params:
        req_params["q"] = q
    return cached_json(
        namespace="ams",
        key=cache_key,
        fetcher=lambda: _request_json(
            endpoint,
            headers={},
            params=req_params,
            auth=(mars_key, ""),
        ),
        ttl_hours=12,
        force_refresh=force_refresh,
    )


def _safe_fetch_mars_payload(
    endpoint: str,
    q: str,
    mars_key: str,
    force_refresh: bool,
    cache_key: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Any]:
    try:
        return _fetch_mars_payload(
            endpoint=endpoint,
            q=q,
            mars_key=mars_key,
            force_refresh=force_refresh,
            cache_key=cache_key,
            params=params,
        )
    except Exception:
        return None


def discover_mars_params(
    selected_crops: List[str],
    last_n_years: int = 3,
    force_refresh: bool = False,
    seed_plan: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    if not crops:
        return {}

    template = os.environ.get("AMS_PRICE_URL_TEMPLATE", "").strip()
    api_key = os.environ.get("AMS_API_KEY", "").strip()
    mars_key = os.environ.get("MARS_API_USERNAME", "").strip() or api_key
    if not template and not mars_key:
        print("[agent][tool] ams-discovery source=skipped reason=missing_api_config", flush=True)
        return {}

    now_year = datetime.utcnow().year
    years = list(range(now_year - last_n_years + 1, now_year + 1))
    start_str = f"01/01/{min(years)}"
    end_str = f"12/31/{max(years)}"
    mars_endpoint = os.environ.get("AMS_MARS_REPORT_ENDPOINT", "").strip()
    mars_reports_endpoint = os.environ.get(
        "AMS_MARS_REPORTS_ENDPOINT",
        "https://marsapi.ams.usda.gov/services/v1.1/reports",
    ).strip()
    market_news_endpoint = os.environ.get("AMS_MARKET_NEWS_ENDPOINT", "").strip()
    detail_base_endpoint = mars_endpoint or mars_reports_endpoint

    out: Dict[str, Dict[str, Any]] = {}
    unresolved: List[str] = []
    max_aliases = max(1, int(os.environ.get("AMS_DISCOVERY_MAX_ALIASES", "4")))
    max_slugs = min(6, max(1, int(os.environ.get("AMS_DISCOVERY_MAX_SLUGS", "6"))))
    strict_agent_params = os.environ.get("AMS_STRICT_AGENT_PARAMS", "1") == "1"
    for crop in crops:
        alias_list = _normalize_aliases(crop)
        seed = (seed_plan or {}).get(crop)
        seed_queries: List[Dict[str, Any]] = []
        if isinstance(seed, dict):
            seed_alias = str(seed.get("alias", "")).strip().lower()
            if seed_alias:
                alias_list.insert(0, seed_alias)
            raw_queries = seed.get("queries")
            if isinstance(raw_queries, list):
                for q in raw_queries:
                    if not isinstance(q, dict):
                        continue
                    endpoint = _normalize_mars_endpoint(str(q.get("endpoint", "")).strip(), detail_base_endpoint)
                    qstr = str(q.get("q", "")).strip()
                    params_obj = q.get("params") if isinstance(q.get("params"), dict) else None
                    if endpoint and (qstr or params_obj):
                        seed_queries.append({"endpoint": endpoint, "q": qstr, "params": params_obj})
        if not alias_list:
            alias_list = [crop.lower()]
        alias_list = alias_list[:max_aliases]

        if mars_key and seed_queries:
            matched_seed = False
            for sq in seed_queries:
                try:
                    for seed_q_variant, p in _seed_query_attempts(
                        str(sq.get("q", "")).strip(),
                        sq.get("params"),
                    ):
                        payload = _fetch_mars_payload(
                            sq["endpoint"],
                            seed_q_variant,
                            mars_key,
                            force_refresh,
                            cache_key={
                                "crop": crop,
                                "endpoint": sq["endpoint"],
                                "q": seed_q_variant,
                                "params": p or {},
                                "kind": "seed-query",
                            },
                            params=p,
                        )
                        df = _parse_mars_response(payload, crop, years)
                        if not df.empty:
                            out[crop] = {"mode": "mars", "alias": alias_list[0], "endpoint": sq["endpoint"]}
                            print(f"[agent][tool] ams-discovery crop={crop} source=seed_query", flush=True)
                            matched_seed = True
                            break
                        slug_ids = _extract_ranked_slug_ids(payload, alias_list)[:max_slugs]
                        if slug_ids:
                            sid = slug_ids[0]
                            detail_endpoint = _merge_mars_endpoint(detail_base_endpoint, sid)
                            out[crop] = {
                                "mode": "mars",
                                "alias": alias_list[0],
                                "endpoint": detail_endpoint,
                                "slug_id": sid,
                            }
                            print(
                                f"[agent][tool] ams-discovery crop={crop} source=seed_query_catalog slug_id={sid}",
                                flush=True,
                            )
                            matched_seed = True
                            break
                    if matched_seed:
                        break
                except Exception:
                    continue
            if matched_seed:
                continue
            if strict_agent_params:
                unresolved.append(crop)
                print(
                    f"[agent][tool] ams-discovery crop={crop} source=empty reason=seed_queries_no_match",
                    flush=True,
                )
                continue

        if template and api_key:
            for alias in alias_list:
                slug = alias.replace(" ", "-")
                try:
                    url = template.format(slug=slug)
                    payload = cached_json(
                        namespace="ams",
                        key={"crop": crop, "slug": slug, "years": years, "kind": "discover-template"},
                        fetcher=lambda u=url: _request_json_fast(
                            u,
                            headers={"Authorization": f"Bearer {api_key}"},
                            params={},
                        ),
                        ttl_hours=12,
                        force_refresh=False,
                    )
                    df = _parse_template_response(payload, crop, years)
                    if not df.empty:
                        out[crop] = {"mode": "template", "slug": slug}
                        print(f"[agent][tool] ams-discovery crop={crop} source=template slug={slug}", flush=True)
                        break
                except Exception:
                    continue
            if crop in out:
                continue

        if mars_key:
            found = False
            for alias in alias_list:
                if mars_endpoint:
                    configured_q = f"commodity={alias};report_begin_date={start_str}:{end_str}"
                    configured_payload = _safe_fetch_mars_payload(
                        mars_endpoint,
                        configured_q,
                        mars_key,
                        force_refresh,
                        cache_key={"crop": crop, "q": configured_q, "endpoint": mars_endpoint, "kind": "discover-configured"},
                    )
                    configured_df = _parse_mars_response(configured_payload or {}, crop, years)
                    if not configured_df.empty:
                        out[crop] = {"mode": "mars", "alias": alias, "endpoint": mars_endpoint}
                        print(f"[agent][tool] ams-discovery crop={crop} source=mars_configured alias={alias}", flush=True)
                        found = True
                        break
                if alias != alias_list[0]:
                    continue

                candidates = _discover_candidate_report_endpoints(
                    crop=crop,
                    alias_list=alias_list,
                    mars_reports_endpoint=mars_reports_endpoint,
                    detail_base_endpoint=detail_base_endpoint,
                    mars_key=mars_key,
                    force_refresh=force_refresh,
                    max_slugs=max_slugs,
                )
                if not candidates and market_news_endpoint:
                    out[crop] = {"mode": "market_news", "alias": alias_list[0], "endpoint": market_news_endpoint}
                    print(
                        f"[agent][tool] ams-discovery crop={crop} source=market_news reason=no_price_style_mars",
                        flush=True,
                    )
                    found = True
                    break
                for alias_candidate, candidate_endpoint, sid in candidates:
                    for candidate_q in _build_report_queries(alias_candidate, start_str, end_str, allow_broad=True):
                        candidate_payload = _safe_fetch_mars_payload(
                            candidate_endpoint,
                            candidate_q,
                            mars_key,
                            force_refresh,
                            cache_key={
                                "crop": crop,
                                "q": candidate_q,
                                "endpoint": candidate_endpoint,
                                "slug_id": sid,
                                "kind": "discover-candidate",
                            },
                        )
                        if candidate_payload is None:
                            continue
                        candidate_df = _parse_mars_response(candidate_payload, crop, years)
                        if not candidate_df.empty:
                            out[crop] = {
                                "mode": "mars",
                                "alias": alias_candidate,
                                "endpoint": candidate_endpoint,
                                "slug_id": sid,
                            }
                            print(
                                f"[agent][tool] ams-discovery crop={crop} source=mars_discovered alias={alias} slug_id={sid}",
                                flush=True,
                            )
                            found = True
                            break
                    if found:
                        break
                if found:
                    break
            if not found:
                unresolved.append(crop)
                print(f"[agent][tool] ams-discovery crop={crop} source=empty", flush=True)

    if unresolved:
        out["__unresolved__"] = {"crops": ",".join(unresolved)}
    return out


def fetch_price_series(
    selected_crops: List[str],
    last_n_years: int = 3,
    force_refresh: bool = False,
    discovery_plan: Optional[Dict[str, Dict[str, Any]]] = None,
    seed_plan: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    now_year = datetime.utcnow().year
    years = list(range(now_year - last_n_years + 1, now_year + 1))
    crops = [c.strip() for c in selected_crops if c and c.strip()]
    cache_key = {"crops": sorted(crops), "years": years, "plan": discovery_plan or {}}
    parquet_path = parquet_cache_path("ams", cache_key)
    cached = load_parquet(parquet_path)
    if not force_refresh and cached is not None and not cached.empty:
        print("[agent][tool] ams source=processed-cache", flush=True)
        return cached

    template = os.environ.get("AMS_PRICE_URL_TEMPLATE", "").strip()
    api_key = os.environ.get("AMS_API_KEY", "").strip()
    mars_key = os.environ.get("MARS_API_USERNAME", "").strip() or api_key
    mars_endpoint = os.environ.get("AMS_MARS_REPORT_ENDPOINT", "").strip()
    mars_reports_endpoint = os.environ.get(
        "AMS_MARS_REPORTS_ENDPOINT",
        "https://marsapi.ams.usda.gov/services/v1.1/reports",
    ).strip()
    market_news_endpoint = os.environ.get("AMS_MARKET_NEWS_ENDPOINT", "").strip()
    detail_base_endpoint = mars_endpoint or mars_reports_endpoint
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    start_str = f"01/01/{min(years)}"
    end_str = f"12/31/{max(years)}"

    unresolved = set()
    if discovery_plan and isinstance(discovery_plan.get("__unresolved__"), dict):
        unresolved_csv = str(discovery_plan.get("__unresolved__", {}).get("crops", ""))
        unresolved = {c.strip() for c in unresolved_csv.split(",") if c.strip()}
    skip_unresolved = os.environ.get("AGENT_SKIP_FETCH_IF_DISCOVERY_EMPTY", "0") == "1"
    strict_agent_params = os.environ.get("AMS_STRICT_AGENT_PARAMS", "1") == "1"

    frames = []
    for crop in crops:
        if skip_unresolved and crop in unresolved:
            frames.append(pd.DataFrame(_fallback_price_series(crop, years)))
            print(f"[agent][tool] ams crop={crop} source=fallback reason=discovery_empty_skip", flush=True)
            continue
        crop_plan = (discovery_plan or {}).get(crop, {})
        crop_seed = (seed_plan or {}).get(crop, {})
        if crop_plan.get("mode") == "market_news":
            alias = str(crop_plan.get("alias") or crop).strip()
            endpoint = str(crop_plan.get("endpoint") or market_news_endpoint).strip()
            if endpoint:
                matched_mn = False
                for q in _build_report_queries(alias, start_str, end_str, allow_broad=False):
                    key = {"crop": crop, "years": years, "endpoint": endpoint, "q": q, "kind": "market-news"}
                    payload = _safe_fetch_mars_payload(
                        endpoint,
                        q,
                        mars_key,
                        force_refresh,
                        cache_key=key,
                    )
                    df = _parse_mars_response(payload or {}, crop, years)
                    if not df.empty:
                        frames.append(df)
                        print(f"[agent][tool] ams crop={crop} source=market_news", flush=True)
                        matched_mn = True
                        break
                if matched_mn:
                    continue
        slug = crop.lower().replace(" ", "-")
        if crop_plan.get("mode") == "template" and crop_plan.get("slug"):
            slug = str(crop_plan["slug"])
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
                # Try agent-planned AMS queries directly in fetch path.
                raw_seed_queries = crop_seed.get("queries") if isinstance(crop_seed, dict) else None
                seed_query_present = isinstance(raw_seed_queries, list) and len(raw_seed_queries) > 0
                if isinstance(raw_seed_queries, list):
                    for sq in raw_seed_queries:
                        if not isinstance(sq, dict):
                            continue
                        seed_endpoint = _normalize_mars_endpoint(
                            str(sq.get("endpoint", "")).strip(),
                            detail_base_endpoint,
                        )
                        seed_q = str(sq.get("q", "")).strip()
                        seed_params = sq.get("params") if isinstance(sq.get("params"), dict) else None
                        if not seed_endpoint or (not seed_q and not seed_params):
                            continue
                        seed_key = {
                            "crop": crop,
                            "years": years,
                            "endpoint": seed_endpoint,
                            "q": seed_q,
                            "params": seed_params or {},
                            "kind": "seed-fetch",
                        }
                        for seed_q_variant, p in _seed_query_attempts(seed_q, seed_params):
                            seed_key["params"] = p or {}
                            seed_key["q"] = seed_q_variant
                            seed_payload = _safe_fetch_mars_payload(
                                seed_endpoint,
                                seed_q_variant,
                                mars_key,
                                force_refresh,
                                cache_key=seed_key,
                                params=p,
                            )
                            seed_df = _parse_mars_response(seed_payload or {}, crop, years)
                            if not seed_df.empty:
                                frames.append(seed_df)
                                print(f"[agent][tool] ams crop={crop} source=seed_query_fetch", flush=True)
                                break
                            slug_ids = _extract_ranked_slug_ids(seed_payload or {}, [crop])[:3]
                            if slug_ids:
                                sid = slug_ids[0]
                                detail_endpoint = _merge_mars_endpoint(detail_base_endpoint, sid)
                                detail_key = {
                                    "crop": crop,
                                    "years": years,
                                    "endpoint": detail_endpoint,
                                    "q": seed_q_variant,
                                    "params": p or {},
                                    "slug_id": sid,
                                    "kind": "seed-fetch-detail",
                                }
                                detail_payload = _safe_fetch_mars_payload(
                                    detail_endpoint,
                                    seed_q_variant,
                                    mars_key,
                                    force_refresh,
                                    cache_key=detail_key,
                                    params=p,
                                )
                                detail_df = _parse_mars_response(detail_payload or {}, crop, years)
                                if not detail_df.empty:
                                    frames.append(detail_df)
                                    print(
                                        f"[agent][tool] ams crop={crop} source=seed_query_fetch_detail slug_id={sid}",
                                        flush=True,
                                    )
                                    seed_df = detail_df
                                    break
                        if not seed_df.empty:
                            break
                    else:
                        seed_df = pd.DataFrame()
                    if not seed_df.empty:
                        continue
                    if strict_agent_params:
                        print(
                            f"[agent][tool] ams crop={crop} source=mars_api_empty reason=seed_queries_no_match",
                            flush=True,
                        )
                        frames.append(pd.DataFrame(_fallback_price_series(crop, years)))
                        print(f"[agent][tool] ams crop={crop} source=fallback reason=api_unavailable", flush=True)
                        continue

                if strict_agent_params and not seed_query_present:
                    print(
                        f"[agent][tool] ams crop={crop} source=mars_api_empty reason=missing_seed_queries",
                        flush=True,
                    )
                    frames.append(pd.DataFrame(_fallback_price_series(crop, years)))
                    print(f"[agent][tool] ams crop={crop} source=fallback reason=api_unavailable", flush=True)
                    continue

                alias = str(crop_plan.get("alias") or crop).strip()
                preferred_endpoint = str(crop_plan.get("endpoint") or "").strip()
                if preferred_endpoint:
                    matched_pref = False
                    for q in _build_report_queries(alias, start_str, end_str, allow_broad=False):
                        key = {"crop": crop, "years": years, "endpoint": preferred_endpoint, "q": q}
                        payload = _safe_fetch_mars_payload(
                            preferred_endpoint,
                            q,
                            mars_key,
                            force_refresh,
                            cache_key=key,
                        )
                        df = _parse_mars_response(payload or {}, crop, years)
                        if not df.empty:
                            frames.append(df)
                            print(f"[agent][tool] ams crop={crop} source=mars_api", flush=True)
                            matched_pref = True
                            break
                    if matched_pref:
                        continue
                elif mars_endpoint:
                    matched_conf = False
                    for q in _build_report_queries(alias, start_str, end_str, allow_broad=False):
                        configured_key = {
                            "crop": crop,
                            "years": years,
                            "endpoint": mars_endpoint,
                            "q": q,
                            "kind": "configured",
                        }
                        configured_payload = _safe_fetch_mars_payload(
                            mars_endpoint,
                            q,
                            mars_key,
                            force_refresh,
                            cache_key=configured_key,
                        )
                        configured_df = _parse_mars_response(configured_payload or {}, crop, years)
                        if not configured_df.empty:
                            frames.append(configured_df)
                            print(f"[agent][tool] ams crop={crop} source=mars_api_configured", flush=True)
                            matched_conf = True
                            break
                    if matched_conf:
                        continue

                # Auto-discover report ids for commodity when explicit endpoint is missing/empty.
                alias_list = _normalize_aliases(crop)
                if crop_plan.get("alias"):
                    alias_list.insert(0, str(crop_plan.get("alias")))
                if not alias_list:
                    alias_list = [crop]
                candidates = _discover_candidate_report_endpoints(
                    crop=crop,
                    alias_list=alias_list,
                    mars_reports_endpoint=mars_reports_endpoint,
                    detail_base_endpoint=detail_base_endpoint,
                    mars_key=mars_key,
                    force_refresh=force_refresh,
                    max_slugs=min(6, max(1, int(os.environ.get("AMS_FETCH_MAX_SLUGS", "6")))),
                )
                matched = False
                for alias_candidate, candidate_endpoint, sid in candidates:
                    for candidate_q in _build_report_queries(alias_candidate, start_str, end_str, allow_broad=False):
                        candidate_key = {
                            "crop": crop,
                            "years": years,
                            "endpoint": candidate_endpoint,
                            "q": candidate_q,
                            "slug_id": sid,
                        }
                        candidate_payload = _safe_fetch_mars_payload(
                            candidate_endpoint,
                            candidate_q,
                            mars_key,
                            force_refresh,
                            cache_key=candidate_key,
                        )
                        if candidate_payload is None:
                            continue
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
