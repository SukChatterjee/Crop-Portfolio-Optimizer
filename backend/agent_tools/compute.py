from __future__ import annotations

from typing import Dict, List

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

