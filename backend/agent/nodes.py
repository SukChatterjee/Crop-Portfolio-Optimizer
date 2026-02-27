from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
from openai import OpenAI

from agent_tools.ams import discover_mars_params, fetch_price_series
from agent_tools.compute import compute_forecasts
from agent_tools.costs import fetch_cost_per_acre
from agent_tools.nass import discover_nass_params, fetch_ohio_crop_stats
from agent_tools.noaa import fetch_weather_features
from agent_tools.soil import fetch_soil_features

from .state import AgentState


VALID_RISK = {"conservative", "moderate", "aggressive"}
VALID_GOAL = {"maximize_profit", "balanced", "minimize_risk"}


def _ensure_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return []


def _default_weather_summary(weather_stats: Dict[str, Any]) -> str:
    if weather_stats.get("summary"):
        return str(weather_stats["summary"])
    features = weather_stats.get("features", {})
    return (
        "Weather baseline indicates avg precipitation "
        f"{features.get('avg_prcp_mm', 'N/A')} mm/day with temperature range "
        f"{features.get('avg_tmin_c', 'N/A')}C to {features.get('avg_tmax_c', 'N/A')}C."
    )


def _default_market_outlook(market_stats: Dict[str, Any]) -> str:
    crops = market_stats.get("crops", [])
    if not crops:
        return "Market outlook unavailable; using historical averages."
    best = max(crops, key=lambda c: c.get("latest_avg_price", 0.0))
    return (
        "Recent average pricing suggests mixed commodity momentum. "
        f"{best.get('crop')} currently shows the strongest observed average price."
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        pass

    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(payload[start : end + 1])
        except json.JSONDecodeError:
            return {}
    return {}


def _openai_generate_json(api_key: str, model: str, prompt: str, purpose: str) -> Dict[str, Any]:
    chosen = (model or "").strip()
    if not chosen:
        return {}
    try:
        print(f"[agent][api-call] provider=openai model={chosen} purpose={purpose}", flush=True)
        client = OpenAI(api_key=api_key, max_retries=int(os.environ.get("OPENAI_MAX_RETRIES", "2")))
        response = client.chat.completions.create(
            model=chosen,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        text = response.choices[0].message.content or "{}"
        return _extract_json_object(text)
    except Exception as exc:
        print(f"[agent][tool] openai source=error purpose={purpose} model={chosen} error={exc}", flush=True)
        return {}


def _build_market_stats(price_df) -> Dict[str, Any]:
    if price_df is None or price_df.empty:
        return {"crops": [], "series_points": 0}
    rows = []
    for crop, group in price_df.groupby("crop"):
        g = group.sort_values("year")
        latest = float(g["avg_price"].iloc[-1])
        first = float(g["avg_price"].iloc[0])
        trend = (latest - first) / max(abs(first), 0.0001)
        rows.append(
            {
                "crop": crop,
                "latest_avg_price": round(latest, 4),
                "price_trend": round(trend, 4),
                "years": [int(y) for y in g["year"].tolist()],
            }
        )
    return {"crops": rows, "series_points": int(len(price_df))}


def _normalize_plan_keys(plan: Dict[str, Any], crops: List[str]) -> Dict[str, Any]:
    if not isinstance(plan, dict):
        return {}
    out: Dict[str, Any] = {}
    by_lower = {c.lower(): c for c in crops}
    for k, v in plan.items():
        key = str(k).strip()
        if not key:
            continue
        canonical = by_lower.get(key.lower(), key)
        out[canonical] = v
    return out


def _llm_plan_params(selected_crops: List[str], farm_profile: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("[agent][tool] param-planner source=skipped reason=missing_openai_api_key", flush=True)
        return {"nass": {}, "ams": {}}

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    ams_endpoint_hint = os.environ.get(
        "AMS_MARS_REPORTS_ENDPOINT",
        "https://marsapi.ams.usda.gov/services/v1.1/reports",
    ).strip()
    system_msg = (
        "You are an API query planner. Return strict JSON only. "
        "Given user crop names, propose candidate query parameters for USDA NASS and AMS MARS APIs. "
        "Output schema: "
        "{\"nass\":{\"<crop>\":{\"queries\":["
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"YIELD\"},"
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"PRODUCTION\"},"
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"AREA HARVESTED\"}"
        "]}},"
        "\"ams\":{\"<crop>\":{\"queries\":[{\"endpoint\":\"...\",\"q\":\"...\",\"params\":{\"key\":\"value\"}}]}}}. "
        "Prefer actual commodity strings used by API responses. Return at least one query per crop if possible. "
        "Do not assume a fixed state unless user context implies it; include national candidates too. "
        "Use fully-qualified https endpoints for AMS. "
        "For AMS params, choose the API style supported by endpoint; prefer explicit params object over q-style. "
        "For example use endpoint "
        + ams_endpoint_hint
        + " with params like "
        "{\"commodity\":\"Tomato\",\"state\":\"Illinois\",\"reportDate\":\"2024-01-01\"}. "
        "Use only endpoint+params that are likely to return rows for the crop; avoid generic empty probes. "
        "Use empty strings when unknown. No markdown."
    )
    payload = {
        "crops": selected_crops,
        "farm_profile": {
            "location": farm_profile.get("location"),
            "state": farm_profile.get("state"),
            "region": farm_profile.get("region"),
        },
        "nass_context": {"years": 3},
        "ams_context": {"years": 3},
    }

    parsed = _openai_generate_json(
        api_key,
        model,
        f"{system_msg}\n\nInput:\n{json.dumps(payload)}",
        purpose="param_planner",
    )
    nass = _normalize_plan_keys(parsed.get("nass", {}), selected_crops)
    ams = _normalize_plan_keys(parsed.get("ams", {}), selected_crops)
    if not nass and not ams:
        print("[agent][tool] param-planner source=empty", flush=True)
    return {"nass": nass, "ams": ams}


def validate_inputs(state: AgentState) -> Dict[str, Any]:
    farm = dict(state.get("farm_profile") or {})
    errors = list(state.get("errors") or [])

    acres = farm.get("acres", 0)
    soil_type = str(farm.get("soil_type", "")).strip()
    selected_crops = _ensure_list(farm.get("selected_crops"))
    risk = str(farm.get("risk_preference", "")).strip().lower()
    goal = str(farm.get("goal", "")).strip().lower()

    try:
        acres_num = float(acres)
    except (TypeError, ValueError):
        acres_num = 0.0

    if acres_num <= 0:
        errors.append("acres must be greater than 0")
    if not soil_type:
        errors.append("soil_type is required")
    if len(selected_crops) == 0:
        errors.append("selected_crops must contain at least one crop")
    if risk not in VALID_RISK:
        errors.append("risk_preference must be one of conservative/moderate/aggressive")
    if goal not in VALID_GOAL:
        errors.append("goal must be one of maximize_profit/balanced/minimize_risk")

    farm["selected_crops"] = selected_crops
    farm["risk_preference"] = risk
    farm["goal"] = goal
    farm["acres"] = acres_num

    return {"farm_profile": farm, "errors": errors}


def fetch_and_compute(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    farm = dict(state.get("farm_profile") or {})
    api_plan = dict(state.get("api_plan") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    location = dict(farm.get("location") or {})
    lat = float(location.get("lat", 39.8283))
    lng = float(location.get("lng", -98.5795))
    force_live_calls = os.environ.get("FORCE_LIVE_API_CALLS", "1") == "1"

    try:
        print(f"[agent][run] force_live_api_calls={force_live_calls}", flush=True)
        max_workers = max(2, int(os.environ.get("AGENT_PROVIDER_MAX_WORKERS", "2")))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_nass = ex.submit(
                fetch_ohio_crop_stats,
                selected_crops,
                3,
                force_live_calls,
                api_plan.get("nass", {}),
            )
            future_prices = ex.submit(
                fetch_price_series,
                selected_crops,
                3,
                force_live_calls,
                api_plan.get("ams", {}),
                api_plan.get("ams_seed", {}),
            )
            nass_df = future_nass.result()
            price_df = future_prices.result()

        weather = fetch_weather_features(
            lat, lng, last_n_years=3, max_stations=3, force_refresh=force_live_calls
        )
        soil = fetch_soil_features(
            lat, lng, soil_type=str(farm.get("soil_type", "")), force_refresh=force_live_calls
        )
        costs = fetch_cost_per_acre(selected_crops, force_refresh=force_live_calls)
        crop_results = compute_forecasts(
            farm_profile=farm,
            nass_df=nass_df,
            price_df=price_df,
            costs_per_acre=costs,
            weather=weather,
        )
        datasets_summary = {
            "weather": {
                "summary": weather.get("summary", ""),
                "features": weather.get("features", {}),
                "stations": weather.get("stations", []),
            },
            "soil": {
                "summary": soil.get("summary", ""),
                "features": soil.get("features", {}),
            },
            "market": _build_market_stats(price_df),
            "nass_rows": int(len(nass_df)) if nass_df is not None else 0,
            "price_rows": int(len(price_df)) if price_df is not None else 0,
            "costs": costs,
            "api_plan": api_plan,
        }
        return {"crop_results": crop_results, "datasets_summary": datasets_summary}
    except Exception as exc:
        errors.append(f"fetch_and_compute failed: {exc}")
        return {"errors": errors}


def plan_sources(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    farm = dict(state.get("farm_profile") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    llm_seed_plan = _llm_plan_params(selected_crops, farm)
    for crop in selected_crops:
        ams_seed = (llm_seed_plan.get("ams", {}) or {}).get(crop, {})
        if isinstance(ams_seed, dict):
            queries = ams_seed.get("queries")
            if isinstance(queries, list) and queries:
                q0 = queries[0] if isinstance(queries[0], dict) else {}
                print(
                    f"[agent][tool] ams-seed crop={crop} queries={len(queries)} "
                    f"endpoint={str(q0.get('endpoint',''))[:120]} "
                    f"params_keys={list((q0.get('params') or {}).keys()) if isinstance(q0.get('params'), dict) else []}",
                    flush=True,
                )
            else:
                print(f"[agent][tool] ams-seed crop={crop} queries=0", flush=True)
    api_plan: Dict[str, Any] = {"nass": {}, "ams": {}, "ams_seed": llm_seed_plan.get("ams", {})}
    try:
        api_plan["nass"] = discover_nass_params(
            selected_crops,
            last_n_years=3,
            # Keep discovery cache on even in live mode for faster repeated runs.
            force_refresh=False,
            seed_plan=llm_seed_plan.get("nass", {}),
        )
    except Exception as exc:
        print(f"[agent][tool] nass-discovery source=error error={exc}", flush=True)

    try:
        api_plan["ams"] = discover_mars_params(
            selected_crops,
            last_n_years=3,
            # Keep discovery cache on even in live mode for faster repeated runs.
            force_refresh=False,
            seed_plan=llm_seed_plan.get("ams", {}),
        )
    except Exception as exc:
        print(f"[agent][tool] ams-discovery source=error error={exc}", flush=True)

    return {"api_plan": api_plan}


def llm_enrich(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    crop_results = list(state.get("crop_results") or [])
    ds = dict(state.get("datasets_summary") or {})
    weather_stats = dict(ds.get("weather") or {})
    soil_stats = dict(ds.get("soil") or {})
    market_stats = dict(ds.get("market") or {})

    if errors or not crop_results:
        return {
            "weather_summary": _default_weather_summary(weather_stats),
            "market_outlook": _default_market_outlook(market_stats),
        }

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        errors.append("OPENAI_API_KEY not set; using deterministic summaries")
        return {
            "errors": errors,
            "weather_summary": _default_weather_summary(weather_stats),
            "market_outlook": _default_market_outlook(market_stats),
        }

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    farm = dict(state.get("farm_profile") or {})
    top6 = crop_results[:6]

    prompt_payload = {
        "farm_profile": {
            "acres": farm.get("acres"),
            "soil_type": farm.get("soil_type"),
            "has_irrigation": farm.get("has_irrigation"),
            "risk_preference": farm.get("risk_preference"),
            "goal": farm.get("goal"),
            "selected_crops": farm.get("selected_crops"),
            "location": farm.get("location"),
        },
        "top_crops_numeric": [
            {
                "crop_name": r.get("crop_name"),
                "yield_forecast": r.get("yield_forecast"),
                "price_forecast": r.get("price_forecast"),
                "expected_profit": r.get("expected_profit"),
                "profit_p10": r.get("profit_p10"),
                "profit_p50": r.get("profit_p50"),
                "profit_p90": r.get("profit_p90"),
                "risk_score": r.get("risk_score"),
                "risk_level": r.get("risk_level"),
                "soil_compatibility": r.get("soil_compatibility"),
            }
            for r in top6
        ],
        "weather_stats": weather_stats,
        "soil_stats": soil_stats,
        "market_stats": market_stats,
    }

    system_msg = (
        "You are an agricultural analyst. Return strict JSON only with this schema: "
        "{\"weather_summary\":\"...\",\"market_outlook\":\"...\","
        "\"soil_explanations\":{\"Crop\":\"...\"}}. "
        "Do not include markdown, code fences, or extra keys."
    )

    parsed = _openai_generate_json(
        api_key,
        model,
        f"{system_msg}\n\nInput JSON:\n{json.dumps(prompt_payload)}",
        purpose="enrich",
    )
    if not parsed:
        errors.append("llm_enrich failed: openai returned empty/invalid json")
        return {
            "errors": errors,
            "weather_summary": _default_weather_summary(weather_stats),
            "market_outlook": _default_market_outlook(market_stats),
        }

    weather_summary = str(parsed.get("weather_summary") or _default_weather_summary(weather_stats))
    market_outlook = str(parsed.get("market_outlook") or _default_market_outlook(market_stats))
    soil_map = parsed.get("soil_explanations") if isinstance(parsed.get("soil_explanations"), dict) else {}

    updated = []
    for r in crop_results:
        crop_name = str(r.get("crop_name", ""))
        if crop_name in soil_map and isinstance(soil_map[crop_name], str):
            r = dict(r)
            r["soil_explanation"] = soil_map[crop_name]
        updated.append(r)

    return {
        "crop_results": updated,
        "weather_summary": weather_summary,
        "market_outlook": market_outlook,
        "errors": errors,
    }


def finalize(state: AgentState) -> Dict[str, Any]:
    ds = dict(state.get("datasets_summary") or {})
    weather_stats = dict(ds.get("weather") or {})
    market_stats = dict(ds.get("market") or {})
    weather_summary = state.get("weather_summary") or _default_weather_summary(weather_stats)
    market_outlook = state.get("market_outlook") or _default_market_outlook(market_stats)

    return {
        "farm_profile": state.get("farm_profile") or {},
        "datasets_summary": ds,
        "crop_results": state.get("crop_results") or [],
        "weather_summary": weather_summary,
        "market_outlook": market_outlook,
        "errors": state.get("errors") or [],
    }
