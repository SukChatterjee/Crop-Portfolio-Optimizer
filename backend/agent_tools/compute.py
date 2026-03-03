from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import json
import os

import pandas as pd


HIGH_WATER_CROPS = {"rice", "cotton", "tomatoes", "lettuce"}


def _safe_series_stats(series: pd.Series, default: float) -> Dict[str, float]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return {"last": default, "first": default, "mean": default}
    return {
        "last": float(clean.iloc[-1]),
        "first": float(clean.iloc[0]),
        "mean": float(clean.mean()),
    }


def _parse_json_payload(text: str) -> Optional[object]:
    payload = (text or "").strip()
    if not payload:
        return None
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
            return None
    start = payload.find("[")
    end = payload.rfind("]")
    if start >= 0 and end > start:
        try:
            return json.loads(payload[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None


def _coerce_float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: object, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _build_llm_payload(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
) -> Dict:
    weather_features = weather.get("features", {}) if isinstance(weather, dict) else {}
    payload = {
        "farm_profile": {
            "acres": farm_profile.get("acres"),
            "soil_type": farm_profile.get("soil_type"),
            "has_irrigation": farm_profile.get("has_irrigation"),
            "risk_preference": farm_profile.get("risk_preference"),
            "goal": farm_profile.get("goal"),
            "selected_crops": farm_profile.get("selected_crops"),
            "location": farm_profile.get("location"),
        },
        "weather_features": weather_features,
        "costs_per_acre": costs_per_acre,
        "nass_series": [],
        "price_series": [],
    }
    if nass_df is not None and not nass_df.empty:
        payload["nass_series"] = [
            {
                "year": int(row.get("year")),
                "crop": row.get("crop"),
                "yield": _coerce_float(row.get("yield"), 0.0),
                "production": _coerce_float(row.get("production"), 0.0),
                "area": _coerce_float(row.get("area"), 0.0),
            }
            for _, row in nass_df.iterrows()
        ]
    if price_df is not None and not price_df.empty:
        payload["price_series"] = [
            {
                "year": int(row.get("year")),
                "crop": row.get("crop"),
                "avg_price": _coerce_float(row.get("avg_price"), 0.0),
            }
            for _, row in price_df.iterrows()
        ]
    return payload


def _llm_forecast(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    if os.environ.get("USE_LLM_FORECAST", "0") != "1":
        return None, "USE_LLM_FORECAST disabled"

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None, "OPENAI_API_KEY not set"

    try:
        from openai import OpenAI
    except ImportError:
        return None, "openai not installed"

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    client = OpenAI(api_key=api_key)

    prompt_payload = _build_llm_payload(
        farm_profile=farm_profile,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs_per_acre,
        weather=weather,
    )

    system_msg = (
        "You are an agricultural forecasting analyst. Use only the provided 3-year data to forecast. "
        "Return strict JSON only as a list of crop result objects with this exact schema: "
        "[{\"crop_name\":\"...\",\"yield_forecast\":0.0,\"price_forecast\":0.0,"
        "\"expected_profit\":0.0,\"profit_p10\":0.0,\"profit_p50\":0.0,\"profit_p90\":0.0,"
        "\"soil_compatibility\":0.0,\"risk_score\":0.0,\"risk_level\":\"Low|Medium|High\","
        "\"soil_explanation\":\"Human-readable formula used for this crop.\"}]. "
        "The soil_explanation must describe the formula in words and math, and mention the input series used. "
        "Do not include markdown, code fences, or extra keys."
    )

    try:
        prompt = f"{system_msg}\n\nInput JSON:\n{json.dumps(prompt_payload)}"
        response = client.responses.create(
            model=model,
            input=prompt,
            temperature=0.2,
        )
        text = getattr(response, "output_text", "") or ""
        parsed = _parse_json_payload(text)
    except Exception as exc:
        return None, f"LLM call failed: {exc}"

    if isinstance(parsed, dict) and isinstance(parsed.get("crop_results"), list):
        parsed = parsed["crop_results"]
    if not isinstance(parsed, list):
        return None, "LLM response not a list"

    results: List[Dict] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        crop_name = str(item.get("crop_name") or "").strip()
        if not crop_name:
            continue
        results.append(
            {
                "crop_name": crop_name,
                "yield_forecast": round(_coerce_float(item.get("yield_forecast"), 0.0), 2),
                "price_forecast": round(_coerce_float(item.get("price_forecast"), 0.0), 4),
                "expected_profit": round(_coerce_float(item.get("expected_profit"), 0.0), 2),
                "profit_p10": round(_coerce_float(item.get("profit_p10"), 0.0), 2),
                "profit_p50": round(_coerce_float(item.get("profit_p50"), 0.0), 2),
                "profit_p90": round(_coerce_float(item.get("profit_p90"), 0.0), 2),
                "soil_compatibility": round(_coerce_float(item.get("soil_compatibility"), 0.0), 1),
                "risk_score": round(_coerce_float(item.get("risk_score"), 0.0), 1),
                "risk_level": str(item.get("risk_level") or "Medium"),
                "soil_explanation": str(item.get("soil_explanation") or ""),
            }
        )

    if not results:
        print(f"empty results using fallback")
        return None, "LLM returned empty results"
    results.sort(key=lambda r: r.get("expected_profit", 0.0), reverse=True)
    return results, None


def _soil_compatibility(soil_type: str, crop: str) -> float:
    st = (soil_type or "").lower()
    crop_key = crop.lower()
    base = 0.70

    if "loam" in st:
        base = 0.90
    elif "silt" in st:
        base = 0.84
    elif "clay" in st:
        base = 0.78
    elif "sand" in st:
        base = 0.74

    if crop_key in {"rice"} and "clay" in st:
        base += 0.08
    if crop_key in {"potatoes", "onions", "lettuce"} and "sandy" in st:
        base += 0.06
    if crop_key in {"apples"} and "loam" in st:
        base += 0.05

    return max(0.55, min(0.98, base))


def _soil_explanation(soil_type: str, crop: str, score: float) -> str:
    return (
        f"Soil compatibility estimated from soil_type='{soil_type}'. "
        f"{crop} suitability score is {score * 100:.1f}%."
    )


def compute_forecasts(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
) -> List[Dict]:
    llm_results, _ = _llm_forecast(
        farm_profile=farm_profile,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs_per_acre,
        weather=weather,
    )
    if llm_results:
        return llm_results

    selected_crops = farm_profile.get("selected_crops") or []
    acres = float(farm_profile.get("acres", 0.0))
    has_irrigation = bool(farm_profile.get("has_irrigation", False))
    soil_type = farm_profile.get("soil_type", "")
    risk_pref = farm_profile.get("risk_preference", "moderate")

    weather_features = weather.get("features", {}) if isinstance(weather, dict) else {}
    weather_risk = float(weather_features.get("risk_index", 0.35))
    precip_cv = float(weather_features.get("precip_cv", 0.4))

    risk_pref_mult = {"conservative": 0.85, "moderate": 1.0, "aggressive": 1.15}.get(risk_pref, 1.0)

    results = []
    for crop in selected_crops:
        crop_mask_n = nass_df["crop"].str.lower() == str(crop).lower() if not nass_df.empty else pd.Series([], dtype=bool)
        crop_mask_p = price_df["crop"].str.lower() == str(crop).lower() if not price_df.empty else pd.Series([], dtype=bool)
        crop_nass = nass_df[crop_mask_n].sort_values("year") if not nass_df.empty else pd.DataFrame()
        crop_price = price_df[crop_mask_p].sort_values("year") if not price_df.empty else pd.DataFrame()

        yield_stats = _safe_series_stats(crop_nass["yield"] if not crop_nass.empty else pd.Series(dtype=float), default=120.0)
        price_stats = _safe_series_stats(crop_price["avg_price"] if not crop_price.empty else pd.Series(dtype=float), default=3.5)

        yield_trend = (yield_stats["last"] - yield_stats["first"]) / max(abs(yield_stats["first"]), 1.0)
        price_trend = (price_stats["last"] - price_stats["first"]) / max(abs(price_stats["first"]), 0.1)

        soil_comp = _soil_compatibility(soil_type, crop)
        irrigation_mult = 1.0
        if crop.lower() in HIGH_WATER_CROPS:
            irrigation_mult = 1.10 if has_irrigation else 0.90

        weather_yield_adj = max(0.82, min(1.08, 1.0 - 0.18 * weather_risk + 0.03 * (1 - precip_cv)))
        yield_forecast = yield_stats["last"] * (1.0 + 0.25 * yield_trend) * (0.85 + 0.30 * soil_comp) * irrigation_mult * weather_yield_adj
        price_forecast = max(0.05, price_stats["last"] * (1.0 + 0.20 * price_trend))

        cost_per_acre = float(costs_per_acre.get(crop, costs_per_acre.get(crop.lower(), 700.0)))
        expected_profit = acres * (yield_forecast * price_forecast - cost_per_acre)

        uncertainty = max(0.10, min(0.40, 0.14 + 0.35 * weather_risk + 0.25 * abs(price_trend) + 0.15 * precip_cv))
        profit_p50 = expected_profit
        profit_p10 = expected_profit * (1.0 - 1.28 * uncertainty)
        profit_p90 = expected_profit * (1.0 + 1.28 * uncertainty)

        risk_score = max(
            5.0,
            min(
                99.0,
                (uncertainty * 150.0 + weather_risk * 35.0 + (1.0 - soil_comp) * 30.0) * risk_pref_mult,
            ),
        )
        risk_level = "Low" if risk_score < 35 else "Medium" if risk_score < 65 else "High"

        results.append(
            {
                "crop_name": crop,
                "yield_forecast": round(float(yield_forecast), 2),
                "price_forecast": round(float(price_forecast), 4),
                "expected_profit": round(float(expected_profit), 2),
                "profit_p10": round(float(profit_p10), 2),
                "profit_p50": round(float(profit_p50), 2),
                "profit_p90": round(float(profit_p90), 2),
                "soil_compatibility": round(float(soil_comp * 100.0), 1),
                "risk_score": round(float(risk_score), 1),
                "risk_level": risk_level,
                "soil_explanation": _soil_explanation(soil_type, crop, soil_comp),
            }
        )

    results.sort(key=lambda r: r["expected_profit"], reverse=True)
    return results
