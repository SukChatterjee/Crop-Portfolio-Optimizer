from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from openai import OpenAI

from agent_tools.ams import fetch_price_series
from agent_tools.compute import compute_forecasts
from agent_tools.costs import fetch_cost_per_acre
from agent_tools.nass import fetch_ohio_crop_stats
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
    selected_crops = _ensure_list(farm.get("selected_crops"))
    location = dict(farm.get("location") or {})
    lat = float(location.get("lat", 39.8283))
    lng = float(location.get("lng", -98.5795))
    force_live_calls = os.environ.get("FORCE_LIVE_API_CALLS", "1") == "1"

    try:
        print(f"[agent][run] force_live_api_calls={force_live_calls}", flush=True)
        nass_df = fetch_ohio_crop_stats(selected_crops, last_n_years=3, force_refresh=force_live_calls)
        weather = fetch_weather_features(
            lat, lng, last_n_years=3, max_stations=3, force_refresh=force_live_calls
        )
        soil = fetch_soil_features(
            lat, lng, soil_type=str(farm.get("soil_type", "")), force_refresh=force_live_calls
        )
        price_df = fetch_price_series(selected_crops, last_n_years=3, force_refresh=force_live_calls)
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
        }
        return {"crop_results": crop_results, "datasets_summary": datasets_summary}
    except Exception as exc:
        errors.append(f"fetch_and_compute failed: {exc}")
        return {"errors": errors}


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
        "You are an agricultural analyst. Respond ONLY with strict JSON and no markdown. "
        "Schema: {\"weather_summary\":\"...\",\"market_outlook\":\"...\","
        "\"soil_explanations\":{\"Crop\":\"...\"}}"
    )

    max_retries = int(os.environ.get("OPENAI_MAX_RETRIES", "0"))
    client = OpenAI(api_key=api_key, max_retries=max_retries)
    try:
        print(f"[agent][api-call] provider=openai model={model} max_retries={max_retries}", flush=True)
        completion = client.chat.completions.create(
            model=model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": json.dumps(prompt_payload)},
            ],
        )
        text = completion.choices[0].message.content or "{}"
        parsed = json.loads(text)
    except Exception as exc:
        errors.append(f"llm_enrich failed: {exc}")
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
