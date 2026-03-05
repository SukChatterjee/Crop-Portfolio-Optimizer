from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
from openai import OpenAI
import pandas as pd

from agent.planners.ams_planner import plan_ams_for_crop
from agent_tools.ams import get_prices_for_crop_with_plan
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


def _build_price_df_from_ams(selected_crops: List[str], ams_prices: Dict[str, Any]) -> pd.DataFrame:
    target_by_crop = {
        "corn": "usd_per_bu",
        "wheat": "usd_per_bu",
        "soybeans": "usd_per_bu",
        "rice": "usd_per_cwt",
        "cotton": "usd_per_lb",
        "tomatoes": "usd_per_cwt",
        "potatoes": "usd_per_cwt",
        "onions": "usd_per_cwt",
        "lettuce": "usd_per_cwt",
        "apples": "usd_per_cwt",
    }

    def _unit_key(unit: str) -> str:
        u = str(unit or "").strip().lower()
        if not u:
            return "missing"
        if any(tok in u for tok in ["index", "%", "percent", "rating", "score"]):
            return "non_price"
        is_cents = any(tok in u for tok in ["cent", "cents", "¢"])
        if "bu" in u or "bushel" in u:
            return "cents_per_bu" if is_cents else "usd_per_bu"
        if "cwt" in u:
            return "cents_per_cwt" if is_cents else "usd_per_cwt"
        # Keep /lb before generic 'lb' token.
        if "/lb" in u or " per lb" in u or " pound" in u or "lb" in u:
            return "cents_per_lb" if is_cents else "usd_per_lb"
        if "ton" in u:
            return "cents_per_ton" if is_cents else "usd_per_ton"
        if "box" in u:
            return "cents_per_box" if is_cents else "usd_per_box"
        if any(tok in u for tok in ["$", "usd", "dollar"]):
            return "usd_unknown_basis"
        return "unknown"

    def _convert_to_target(raw_price: float, from_key: str, target_key: str) -> Any:
        p = float(raw_price)
        factors = {
            ("usd_per_bu", "usd_per_bu"): 1.0,
            ("cents_per_bu", "usd_per_bu"): 0.01,
            ("usd_per_lb", "usd_per_lb"): 1.0,
            ("cents_per_lb", "usd_per_lb"): 0.01,
            ("usd_per_cwt", "usd_per_cwt"): 1.0,
            ("cents_per_cwt", "usd_per_cwt"): 0.01,
            ("usd_per_ton", "usd_per_ton"): 1.0,
            ("cents_per_ton", "usd_per_ton"): 0.01,
            ("usd_per_box", "usd_per_box"): 1.0,
            ("cents_per_box", "usd_per_box"): 0.01,
            # Cross-basis conversions where physically valid.
            ("usd_per_cwt", "usd_per_lb"): 0.01,
            ("cents_per_cwt", "usd_per_lb"): 0.0001,
            ("usd_per_lb", "usd_per_cwt"): 100.0,
            ("cents_per_lb", "usd_per_cwt"): 1.0,
        }
        factor = factors.get((from_key, target_key))
        if factor is None:
            return None
        return p * factor

    def _try_infer_normalized_price(raw_price: float, target_key: str) -> Any:
        """
        For AMS rows with missing/unknown unit, try plausible interpretations
        and keep the first normalized value that passes sanity bounds.
        """
        p = float(raw_price)
        candidates: List[str] = []
        if target_key:
            candidates.append(target_key)
            if target_key.startswith("usd_"):
                candidates.append(target_key.replace("usd_", "cents_", 1))
            elif target_key.startswith("cents_"):
                candidates.append(target_key.replace("cents_", "usd_", 1))
        # Preserve order and remove duplicates.
        seen = set()
        ordered = []
        for c in candidates:
            if c and c not in seen:
                ordered.append(c)
                seen.add(c)
        for source_key in ordered:
            normalized = _convert_to_target(p, source_key, target_key)
            if normalized is None:
                continue
            val = float(normalized)
            if 0.05 <= val <= 100.0:
                return val
        return None

    rows = []
    for crop in selected_crops:
        payload = ams_prices.get(crop) if isinstance(ams_prices, dict) else None
        series = payload.get("series") if isinstance(payload, dict) else None
        target_key = target_by_crop.get(str(crop).strip().lower())
        if not isinstance(series, list):
            continue
        grouped: Dict[int, List[float]] = {}
        unit_counts: Dict[str, int] = {}
        kept_rows = 0
        dropped_rows = 0
        inferred_rows = 0
        for row in series:
            if not isinstance(row, dict):
                continue
            row_unit = str(row.get("unit") or "").strip().lower()
            date_s = str(row.get("date", "")).strip()
            if len(date_s) < 4 or not date_s[:4].isdigit():
                dropped_rows += 1
                continue
            year = int(date_s[:4])
            try:
                price = float(row.get("price_avg"))
            except (TypeError, ValueError):
                dropped_rows += 1
                continue
            src_key = _unit_key(row_unit)
            unit_counts[src_key] = int(unit_counts.get(src_key, 0)) + 1
            effective_target = target_key or src_key.replace("cents_", "usd_")
            if src_key == "non_price":
                dropped_rows += 1
                continue
            if src_key in {"unknown", "missing", "usd_unknown_basis"}:
                normalized = _try_infer_normalized_price(price, effective_target)
                if normalized is not None:
                    inferred_rows += 1
            else:
                normalized = _convert_to_target(price, src_key, effective_target)
            if normalized is None:
                dropped_rows += 1
                continue
            price = float(normalized)
            # Generic sanity range for price inputs used by forecast model.
            if price < 0.05 or price > 100.0:
                dropped_rows += 1
                continue
            grouped.setdefault(year, []).append(price)
            kept_rows += 1
        for year, prices in grouped.items():
            if not prices:
                continue
            s = pd.Series(prices, dtype=float)
            if len(s) >= 4:
                q1, q3 = s.quantile(0.25), s.quantile(0.75)
                iqr = float(q3 - q1)
                lo = float(q1 - 3.0 * iqr)
                hi = float(q3 + 3.0 * iqr)
                s = s[(s >= lo) & (s <= hi)]
            if s.empty:
                continue
            rows.append({"year": int(year), "crop": crop, "avg_price": round(float(s.mean()), 4)})
        total = kept_rows + dropped_rows
        valid_ratio = (kept_rows / total) if total > 0 else 0.0
        if kept_rows < 12 or valid_ratio < 0.5:
            rows = [r for r in rows if str(r.get("crop")) != str(crop)]
            print(
                f"[agent][tool] ams-preprocess crop={crop} target={target_key or 'auto'} kept={kept_rows} dropped={dropped_rows} inferred={inferred_rows} valid_ratio={valid_ratio:.2f} unit_counts={unit_counts} status=rejected_low_quality",
                flush=True,
            )
            continue
        print(
            f"[agent][tool] ams-preprocess crop={crop} target={target_key or 'auto'} kept={kept_rows} dropped={dropped_rows} inferred={inferred_rows} valid_ratio={valid_ratio:.2f} unit_counts={unit_counts}",
            flush=True,
        )
    return pd.DataFrame(rows)


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
    ams_prices = dict(state.get("ams_prices") or {})
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
            nass_df = future_nass.result()
        price_df = _build_price_df_from_ams(selected_crops, ams_prices)

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
            "ams_prices": ams_prices,
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
    api_plan: Dict[str, Any] = {"nass": {}}
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

    return {"api_plan": api_plan}


def plan_ams_params(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}
    farm = dict(state.get("farm_profile") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    state_name = "OHIO"
    lookback_days = 1095

    out: Dict[str, Any] = {}
    for crop in selected_crops:
        plan = plan_ams_for_crop(crop_name=crop, state_name=state_name, lookback_days=lookback_days)
        out[crop] = plan
        status = (plan.get("plan") or {}).get("status")
        if status == "no_data":
            print(
                f"[agent][tool] ams-planner crop={crop} source=no_data reason={((plan.get('plan') or {}).get('reason',''))}",
                flush=True,
            )
        else:
            cq = ((plan.get("plan") or {}).get("catalog_query") or {})
            filters = cq.get("filters") if isinstance(cq.get("filters"), dict) else {}
            contains_any = filters.get("contains_any") if isinstance(filters.get("contains_any"), list) else []
            print(
                f"[agent][tool] ams-planner crop={crop} contains_any={contains_any[:6]} max_candidates={cq.get('max_candidates', 30)}",
                flush=True,
            )
    return {"ams_plans": out}


def fetch_ams_prices(state: AgentState) -> Dict[str, Any]:
    errors = list(state.get("errors") or [])
    if errors:
        return {}
    farm = dict(state.get("farm_profile") or {})
    selected_crops = _ensure_list(farm.get("selected_crops"))
    plans = dict(state.get("ams_plans") or {})
    state_name = "OHIO"
    lookback_days = 1095
    out: Dict[str, Any] = {}

    for crop in selected_crops:
        plan = plans.get(crop, {})
        result = get_prices_for_crop_with_plan(
            crop_name=crop,
            state_name=state_name,
            lookback_days=lookback_days,
            planner_json=plan,
        )
        out[crop] = result
        if result.get("status") == "ok":
            print(
                f"[agent][tool] ams crop={crop} source=ok slug_id={result.get('chosen_slug_id')} rows={len(result.get('series') or [])}",
                flush=True,
            )
        else:
            print(
                f"[agent][tool] ams crop={crop} source=no_data reason={result.get('reason', 'unknown')}",
                flush=True,
            )
    return {"ams_prices": out}


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
