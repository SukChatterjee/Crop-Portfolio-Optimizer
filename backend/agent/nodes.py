from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
from openai import OpenAI
import pandas as pd

from analysis_progress import append_analysis_log, complete_analysis_stage, set_analysis_stage
from agent_tools.compute import DEFAULT_PRICE_BY_CROP, DEFAULT_UNITS_BY_CROP, compute_forecasts, normalize_and_predict_inputs
from agent_tools.costs import fetch_cost_per_acre
from agent_tools.fred import fetch_fred_series
from agent_tools.nass import discover_nass_params, fetch_ohio_crop_stats
from agent_tools.noaa import fetch_weather_features
from agent_tools.soil import fetch_soil_features

from .state import AgentState


VALID_RISK = {"conservative", "moderate", "aggressive"}
VALID_GOAL = {"maximize_profit", "balanced", "minimize_risk"}


def _truncate_json(data: Any, max_chars: int = 4000) -> str:
    try:
        text = json.dumps(data, default=str)
    except Exception:
        text = str(data)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...<truncated>"


def _log_agent_io(agent_name: str, stage: str, payload: Any) -> None:
    print(f"[agent][io] agent={agent_name} stage={stage} payload={_truncate_json(payload)}", flush=True)


def _progress_job_id(state: AgentState) -> str:
    return str(state.get("progress_job_id") or "").strip()


def _set_stage(state: AgentState, stage_id: str, message: str) -> None:
    job_id = _progress_job_id(state)
    if job_id:
        set_analysis_stage(job_id, stage_id, message)


def _complete_stage(state: AgentState, stage_id: str, message: str) -> None:
    job_id = _progress_job_id(state)
    if job_id:
        complete_analysis_stage(job_id, stage_id, message)


def _append_stage_log(state: AgentState, step: str, text: str, is_ok: bool = False) -> None:
    job_id = _progress_job_id(state)
    if job_id:
        append_analysis_log(job_id, step, text, is_ok)


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


def _build_market_price_df(selected_crops: List[str], nass_df: pd.DataFrame) -> pd.DataFrame:
    years: List[int] = []
    if nass_df is not None and not nass_df.empty and "year" in nass_df.columns:
        try:
            years = sorted({int(y) for y in pd.to_numeric(nass_df["year"], errors="coerce").dropna().tolist()})
        except Exception:
            years = []
    if not years:
        current_year = pd.Timestamp.utcnow().year
        years = [current_year - 2, current_year - 1, current_year]

    rows = []
    year_offsets = {-2: 0.96, -1: 1.0, 0: 1.04}
    for crop in selected_crops:
        crop_key = str(crop).strip().lower()
        baseline = float(DEFAULT_PRICE_BY_CROP.get(crop_key, 3.5))
        for year in years:
            offset = year - years[-1]
            multiplier = year_offsets.get(offset, 1.0)
            rows.append(
                {
                    "year": int(year),
                    "crop": crop,
                    "avg_price": round(baseline * multiplier, 4),
                    "price_unit": str(DEFAULT_UNITS_BY_CROP.get(crop_key, {}).get("price_unit", "$/unit")),
                    "source": "default_baseline",
                }
            )
        print(
            f"[agent][tool] market-baseline crop={crop} source=default rows={len(years)} baseline={baseline}",
            flush=True,
        )
    return pd.DataFrame(rows)


def _df_to_records(df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    safe = df.copy()
    for column in columns:
        if column not in safe.columns:
            safe[column] = None
    return safe[columns].to_dict(orient="records")


def _records_to_df(records: Any, columns: List[str]) -> pd.DataFrame:
    if not isinstance(records, list) or not records:
        return pd.DataFrame(columns=columns)
    rows = [row for row in records if isinstance(row, dict)]
    if not rows:
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(rows)
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _llm_plan_params(selected_crops: List[str], farm_profile: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("[agent][tool] param-planner source=skipped reason=missing_openai_api_key", flush=True)
        return {"nass": {}}

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    system_msg = (
        "You are an API query planner. Return strict JSON only. "
        "Given user crop names, propose candidate query parameters for USDA NASS API. "
        "Output schema: "
        "{\"nass\":{\"<crop>\":{\"queries\":["
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"YIELD\"},"
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"PRODUCTION\"},"
        "{\"state_name\":\"...\",\"agg_level_desc\":\"...\",\"sector_desc\":\"...\","
        "\"group_desc\":\"...\",\"commodity_desc\":\"...\",\"statisticcat_desc\":\"AREA HARVESTED\"}"
        "]}}}. "
        "Prefer actual commodity strings used by API responses. Return at least one query per crop if possible. "
        "Do not assume a fixed state unless user context implies it; include national candidates too. "
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
    }

    parsed = _openai_generate_json(
        api_key,
        model,
        f"{system_msg}\n\nInput:\n{json.dumps(payload)}",
        purpose="param_planner",
    )
    nass = _normalize_plan_keys(parsed.get("nass", {}), selected_crops)
    if not nass:
        print("[agent][tool] param-planner source=empty", flush=True)
    return {"nass": nass}


def _llm_plan_costs_params(selected_crops: List[str], farm_profile: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {"query_candidates": []}
    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    state_name = str(((farm_profile.get("location") or {}).get("state")) or "OHIO").strip().upper() or "OHIO"
    prompt = (
        "You are an API query planner for production cost-per-acre extraction.\n"
        "Return strict JSON only with schema:\n"
        "{\"query_candidates\":[{\"report\":\"...\",\"category\":\"...\",\"category_value\":\"...\",\"state\":\"...\",\"state_name\":\"...\"}]}\n"
        "Rules:\n"
        "- Build 3-8 candidates for ERS/ARMS-like APIs.\n"
        "- Include state/state_name and set them to requested state when possible.\n"
        "- Prefer cost/expense oriented report terms.\n"
        "- No markdown."
    )
    payload = {
        "selected_crops": selected_crops,
        "state": state_name,
        "goal": farm_profile.get("goal"),
        "risk_preference": farm_profile.get("risk_preference"),
    }
    parsed = _openai_generate_json(
        api_key,
        model,
        f"{prompt}\n\nInput:\n{json.dumps(payload)}",
        purpose="costs_param_planner",
    )
    candidates = parsed.get("query_candidates") if isinstance(parsed, dict) else []
    if not isinstance(candidates, list):
        candidates = []
    cleaned = []
    for c in candidates:
        if not isinstance(c, dict):
            continue
        cc = {str(k): str(v) for k, v in c.items() if str(v).strip()}
        if cc:
            cleaned.append(cc)
    print(f"[agent][tool] costs-planner candidates={len(cleaned)}", flush=True)
    return {"query_candidates": cleaned}


def _llm_plan_fred_params(selected_crops: List[str], farm_profile: Dict[str, Any]) -> Dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("[agent][tool] fred-planner source=skipped reason=missing_openai_api_key", flush=True)
        return {"query_candidates": []}

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    catalog = [
        {"series_id": "CPIAUCSL", "title": "Consumer Price Index for All Urban Consumers: All Items", "why": "broad inflation"},
        {"series_id": "PPIACO", "title": "Producer Price Index by Commodity: All Commodities", "why": "producer price pressure"},
        {"series_id": "DCOILWTICO", "title": "Crude Oil Prices: West Texas Intermediate", "why": "fuel and input cost pressure"},
        {"series_id": "DGS10", "title": "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity", "why": "interest-rate climate"},
        {"series_id": "FEDFUNDS", "title": "Effective Federal Funds Rate", "why": "short-rate climate"},
        {"series_id": "UNRATE", "title": "Unemployment Rate", "why": "macro demand and labor backdrop"},
        {"series_id": "DTWEXBGS", "title": "Trade Weighted U.S. Dollar Index: Broad, Goods", "why": "export competitiveness"},
        {"series_id": "M2SL", "title": "M2 Money Stock", "why": "broad liquidity backdrop"},
    ]
    prompt = (
        "You are an API query planner for the St. Louis Fed FRED API.\n"
        "Return strict JSON only with schema:\n"
        "{\"lookback_years\":5,\"query_candidates\":[{\"series_id\":\"...\",\"title\":\"...\",\"purpose\":\"...\",\"units\":\"lin|chg|ch1|pch|pc1|pca|cch|cca|log\",\"frequency\":\"m|q|a|\",\"aggregation_method\":\"avg|sum|eop\",\"observation_start\":\"YYYY-MM-DD\"}]}\n"
        "Rules:\n"
        "- Choose 3 to 6 series from the provided catalog only.\n"
        "- Favor macro indicators useful for crop pricing, demand, export conditions, financing, and farm input costs.\n"
        "- observation_start should reflect a sensible lookback window for trend analysis.\n"
        "- Keep frequency empty when the native cadence should be used.\n"
        "- No markdown."
    )
    payload = {
        "selected_crops": selected_crops,
        "farm_profile": {
            "goal": farm_profile.get("goal"),
            "risk_preference": farm_profile.get("risk_preference"),
            "state": ((farm_profile.get("location") or {}).get("state")),
        },
        "catalog": catalog,
    }
    parsed = _openai_generate_json(
        api_key,
        model,
        f"{prompt}\n\nInput:\n{json.dumps(payload)}",
        purpose="fred_param_planner",
    )
    candidates = parsed.get("query_candidates") if isinstance(parsed, dict) else []
    if not isinstance(candidates, list):
        candidates = []
    allowed = {item["series_id"] for item in catalog}
    cleaned = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        series_id = str(item.get("series_id") or "").strip().upper()
        if not series_id or series_id not in allowed:
            continue
        cleaned.append(
            {
                "series_id": series_id,
                "title": str(item.get("title") or "").strip(),
                "purpose": str(item.get("purpose") or "").strip(),
                "units": str(item.get("units") or "lin").strip() or "lin",
                "frequency": str(item.get("frequency") or "").strip(),
                "aggregation_method": str(item.get("aggregation_method") or "avg").strip() or "avg",
                "observation_start": str(item.get("observation_start") or "").strip(),
            }
        )
    print(f"[agent][tool] fred-planner candidates={len(cleaned)}", flush=True)
    return {
        "lookback_years": int(parsed.get("lookback_years") or 5) if isinstance(parsed, dict) else 5,
        "query_candidates": cleaned,
    }


def validate_inputs(state: AgentState) -> Dict[str, Any]:
    _append_stage_log(state, "Backend Sync", "Validating farm profile inputs...")
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


def fetch_source_data(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    _set_stage(state, "data_ingestion", "Fetching source datasets and farm inputs...")
    farm = dict(state.get("farm_profile") or {})
    api_plan = dict(state.get("api_plan") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    location = dict(farm.get("location") or {})
    lat = float(location.get("lat", 39.8283))
    lng = float(location.get("lng", -98.5795))
    force_live_calls = os.environ.get("FORCE_LIVE_API_CALLS", "1") == "1"

    try:
        print(f"[agent][run] force_live_api_calls={force_live_calls}", flush=True)
        _append_stage_log(state, "Data Ingestion", "Loading production cost estimates...")
        # Fetch costs first so downstream compute/LLM always receives the latest
        # cost-per-acre context before other data collection and forecasting.
        costs = fetch_cost_per_acre(
            selected_crops,
            force_refresh=force_live_calls,
            api_plan=(api_plan.get("costs", {}) if isinstance(api_plan, dict) else {}),
        )
        print(f"[agent][tool] costs-prefetch crops={len(selected_crops)}", flush=True)
        _append_stage_log(state, "Data Ingestion", "Fetching NASS yield and production history...")

        max_workers = max(2, int(os.environ.get("AGENT_PROVIDER_MAX_WORKERS", "2")))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_nass = ex.submit(
                fetch_ohio_crop_stats,
                selected_crops,
                3,
                force_live_calls,
                api_plan.get("nass", {}),
            )
            future_fred = ex.submit(
                fetch_fred_series,
                api_plan.get("fred", {}),
                force_live_calls,
            )
            nass_df = future_nass.result()
            fred_data = future_fred.result()
        _append_stage_log(state, "Data Ingestion", "Collecting soil profile and macro indicators...")
        price_df = _build_market_price_df(selected_crops, nass_df)
        soil = fetch_soil_features(
            lat, lng, soil_type=str(farm.get("soil_type", "")), force_refresh=force_live_calls
        )
        _complete_stage(state, "data_ingestion", "Data ingestion complete.")

        _set_stage(state, "weather_analysis", "Fetching NOAA climate and weather features...")
        weather = fetch_weather_features(
            lat, lng, last_n_years=3, max_stations=3, force_refresh=force_live_calls
        )
        _complete_stage(state, "weather_analysis", "Weather analysis complete.")
        analysis_inputs = {
            "nass_rows": _df_to_records(
                nass_df,
                ["year", "crop", "yield", "yield_unit", "yield_desc", "production", "production_unit", "production_desc", "area", "area_unit", "area_desc"],
            ),
            "price_rows": _df_to_records(price_df, ["year", "crop", "avg_price", "price_unit", "source"]),
            "costs": costs,
            "weather": weather,
            "soil": soil,
            "fred": fred_data,
        }
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
            "fred": fred_data,
            "api_plan": api_plan,
        }
        return {"analysis_inputs": analysis_inputs, "datasets_summary": datasets_summary}
    except Exception as exc:
        errors.append(f"fetch_source_data failed: {exc}")
        return {"errors": errors}


def plan_sources(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    _set_stage(state, "data_ingestion", "Planning the best API calls for the selected crops...")
    farm = dict(state.get("farm_profile") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    _log_agent_io(
        "agent1_param_planner",
        "input",
        {
            "selected_crops": selected_crops,
            "farm_profile": {
                "location": farm.get("location"),
                "soil_type": farm.get("soil_type"),
                "risk_preference": farm.get("risk_preference"),
                "goal": farm.get("goal"),
            },
        },
    )
    llm_seed_plan = _llm_plan_params(selected_crops, farm)
    api_plan: Dict[str, Any] = {"nass": {}, "costs": {}, "fred": {}}
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
        api_plan["costs"] = _llm_plan_costs_params(selected_crops, farm)
    except Exception as exc:
        print(f"[agent][tool] costs-planner source=error error={exc}", flush=True)
    try:
        api_plan["fred"] = _llm_plan_fred_params(selected_crops, farm)
    except Exception as exc:
        print(f"[agent][tool] fred-planner source=error error={exc}", flush=True)

    _append_stage_log(state, "Data Ingestion", "API query planning complete.", True)
    _log_agent_io("agent1_param_planner", "output", api_plan)
    return {"api_plan": api_plan}


def agent2_predict(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    _set_stage(state, "yield_modeling", "Normalizing fetched inputs and modeling crop yields...")
    farm = dict(state.get("farm_profile") or {})
    analysis_inputs = dict(state.get("analysis_inputs") or {})
    nass_df = _records_to_df(
        analysis_inputs.get("nass_rows"),
        ["year", "crop", "yield", "yield_unit", "yield_desc", "production", "production_unit", "production_desc", "area", "area_unit", "area_desc"],
    )
    price_df = _records_to_df(analysis_inputs.get("price_rows"), ["year", "crop", "avg_price", "price_unit", "source"])
    costs = dict(analysis_inputs.get("costs") or {})
    weather = dict(analysis_inputs.get("weather") or {})
    soil = dict(analysis_inputs.get("soil") or {})
    fred_data = dict(analysis_inputs.get("fred") or {})
    _log_agent_io(
        "agent2_advisory_predictor",
        "input",
        {
            "farm_profile": {
                "selected_crops": farm.get("selected_crops"),
                "acres": farm.get("acres"),
                "soil_type": farm.get("soil_type"),
                "has_irrigation": farm.get("has_irrigation"),
                "risk_preference": farm.get("risk_preference"),
                "goal": farm.get("goal"),
            },
            "nass_rows": int(len(nass_df)),
            "price_rows": int(len(price_df)),
            "cost_crops": list(costs.keys()),
            "weather_summary": weather.get("summary"),
            "soil_summary": soil.get("summary"),
            "fred_summary": fred_data.get("summary"),
        },
    )

    predictions, err = normalize_and_predict_inputs(
        farm_profile=farm,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs,
        weather=weather,
        fred_data=fred_data,
    )
    if err:
        print(f"[agent][tool] agent2 source=fallback reason={err}", flush=True)
        return {"agent2_predictions": {}}

    print(f"[agent][tool] agent2 source=ok crops={list((predictions or {}).keys())}", flush=True)
    _complete_stage(state, "yield_modeling", "Yield modeling complete.")
    _log_agent_io("agent2_advisory_predictor", "output", predictions or {})
    return {"agent2_predictions": predictions or {}}


def compute_results(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}

    _set_stage(state, "market_forecast", "Forecasting crop prices and building revenue inputs...")
    farm = dict(state.get("farm_profile") or {})
    analysis_inputs = dict(state.get("analysis_inputs") or {})
    nass_df = _records_to_df(
        analysis_inputs.get("nass_rows"),
        ["year", "crop", "yield", "yield_unit", "yield_desc", "production", "production_unit", "production_desc", "area", "area_unit", "area_desc"],
    )
    price_df = _records_to_df(analysis_inputs.get("price_rows"), ["year", "crop", "avg_price", "price_unit", "source"])
    costs = dict(analysis_inputs.get("costs") or {})
    weather = dict(analysis_inputs.get("weather") or {})
    soil = dict(analysis_inputs.get("soil") or {})
    agent2_predictions = dict(state.get("agent2_predictions") or {})
    fred_data = dict(analysis_inputs.get("fred") or {})
    _log_agent_io(
        "deterministic_compute",
        "input",
        {
            "selected_crops": farm.get("selected_crops"),
            "nass_rows": int(len(nass_df)),
            "price_rows": int(len(price_df)),
            "cost_crops": list(costs.keys()),
            "agent2_prediction_crops": list(agent2_predictions.keys()),
            "weather_features": weather.get("features"),
            "soil_features": soil.get("features"),
        },
    )

    crop_results = compute_forecasts(
        farm_profile=farm,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs,
        weather=weather,
        soil=soil,
        fred_data=fred_data,
        agent2_predictions=agent2_predictions,
    )
    _complete_stage(state, "market_forecast", "Market forecasting complete.")
    _set_stage(state, "profit_simulation", "Running profit and risk simulations...")
    _log_agent_io(
        "deterministic_compute",
        "output",
        {
            "crop_count": len(crop_results),
            "crops": [
                {
                    "crop_name": r.get("crop_name"),
                    "forecast_source": r.get("forecast_source"),
                    "yield_forecast": r.get("yield_forecast"),
                    "yield_unit": r.get("yield_unit"),
                    "price_forecast": r.get("price_forecast"),
                    "price_unit": r.get("price_unit"),
                    "expected_profit": r.get("expected_profit"),
                    "risk_score": r.get("risk_score"),
                    "risk_level": r.get("risk_level"),
                }
                for r in crop_results
            ],
        },
    )
    return {"crop_results": crop_results}


def llm_enrich(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    crop_results = list(state.get("crop_results") or [])
    ds = dict(state.get("datasets_summary") or {})
    weather_stats = dict(ds.get("weather") or {})
    soil_stats = dict(ds.get("soil") or {})
    market_stats = dict(ds.get("market") or {})
    fred_stats = dict((ds.get("fred") or {}).get("summary") or {})

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
        "fred_stats": fred_stats,
    }
    _log_agent_io("agent3_enrichment", "input", prompt_payload)

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

    _log_agent_io(
        "agent3_enrichment",
        "output",
        {
            "weather_summary": weather_summary,
            "market_outlook": market_outlook,
            "soil_explanations": soil_map,
        },
    )
    _complete_stage(state, "profit_simulation", "Profit simulation complete.")
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
