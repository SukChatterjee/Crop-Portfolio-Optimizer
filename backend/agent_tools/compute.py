from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import json
import os
import re

import pandas as pd


HIGH_WATER_CROPS = {"rice", "cotton", "tomatoes", "lettuce"}
DEFAULT_PRICE_BY_CROP = {
    "corn": 3.5,
    "wheat": 3.5,
    "soybeans": 10.0,
    "rice": 12.0,
    "cotton": 0.8,
    "tomatoes": 3.5,
    "potatoes": 8.0,
    "onions": 8.0,
    "apples": 25.0,
    "lettuce": 18.0,
}
DEFAULT_YIELD_BY_CROP = {
    "corn": 150.0,
    "wheat": 55.0,
    "soybeans": 50.0,
    "rice": 7000.0,
    "cotton": 950.0,
    "tomatoes": 900.0,
    "potatoes": 450.0,
    "onions": 550.0,
    "apples": 16000.0,
    "lettuce": 300.0,
}


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
        pass
    text = str(value or "").strip().replace(",", "")
    if not text:
        return default
    # Keep first numeric token from strings like "$3.5/bu", "about 50", "3.5 USD".
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return default
    try:
        return float(m.group(0))
    except (TypeError, ValueError):
        return default


def _coerce_int(value: object, default: int) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _canonical_crop_key(value: object) -> str:
    return str(value or "").strip().lower()


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


def _price_baseline_by_crop(selected_crops: List[str], price_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out: Dict[str, float] = {}
    for crop in selected_crops:
        key = _canonical_crop_key(crop)
        fallback = float(DEFAULT_PRICE_BY_CROP.get(key, 3.5))
        if price_df is None or price_df.empty:
            out[key] = fallback
            continue
        mask = price_df["crop"].str.lower() == key
        series = pd.to_numeric(price_df.loc[mask, "avg_price"], errors="coerce").dropna()
        if series.empty:
            out[key] = fallback
        else:
            out[key] = max(0.05, float(series.iloc[-1]))
    return out


def _yield_baseline_by_crop(selected_crops: List[str], nass_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for crop in selected_crops:
        key = _canonical_crop_key(crop)
        fallback = float(DEFAULT_YIELD_BY_CROP.get(key, 120.0))
        if nass_df is None or nass_df.empty:
            out[key] = fallback
            continue
        mask = nass_df["crop"].str.lower() == key
        series = pd.to_numeric(nass_df.loc[mask, "yield"], errors="coerce").dropna()
        if series.empty:
            out[key] = fallback
        else:
            out[key] = max(0.01, float(series.iloc[-1]))
    return out


def _llm_predict_current_year(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
) -> Tuple[Optional[Dict[str, Dict[str, float]]], Optional[str]]:
    # Enabled by default. Set USE_LLM_FORECAST=0 to disable.
    if os.environ.get("USE_LLM_FORECAST", "1") != "1":
        print("[agent][tool] llm-forecast source=skipped reason=USE_LLM_FORECAST_disabled", flush=True)
        return None, "USE_LLM_FORECAST disabled"

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("[agent][tool] llm-forecast source=skipped reason=missing_openai_api_key", flush=True)
        return None, "OPENAI_API_KEY not set"

    try:
        from openai import OpenAI
    except ImportError:
        print("[agent][tool] llm-forecast source=skipped reason=openai_import_failed", flush=True)
        return None, "openai not installed"

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
    client = OpenAI(api_key=api_key)

    prompt_payload = _build_llm_payload(
        farm_profile=farm_profile,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs_per_acre,
        weather=weather,
    )
    selected_crops = [str(c).strip() for c in (farm_profile.get("selected_crops") or []) if str(c).strip()]
    price_baseline = _price_baseline_by_crop(selected_crops, price_df)
    yield_baseline = _yield_baseline_by_crop(selected_crops, nass_df)

    system_msg = (
        "You are ONE autonomous analysis agent.\n"
        "Task inside a single pass: preprocess API data -> infer features -> forecast this-year inputs -> validate trustability.\n"
        "Return strict JSON only with this schema:\n"
        "{"
        "\"agent_summary\":{\"status\":\"ok|partial|no_data\",\"notes\":\"...\"},"
        "\"predictions\":["
        "{"
        "\"crop_name\":\"...\","
        "\"cleaned_features\":{\"price_last\":0.0,\"price_trend\":0.0,\"yield_last\":0.0,\"yield_trend\":0.0,\"cost_api\":0.0},"
        "\"prediction\":{\"yield_forecast\":0.0,\"price_forecast\":0.0,\"cost_adjustment_factor\":1.0,\"cost_per_acre\":0.0},"
        "\"quality_checks\":{\"unit_consistency\":true,\"range_sanity\":true,\"data_sufficiency\":true,\"approved\":true,\"issues\":[]},"
        "\"confidence\":0.0,"
        "\"reasoning\":\"short reason\""
        "}"
        "]"
        "}.\n"
        "Rules:\n"
        "- Use ONLY the provided API-derived input.\n"
        "- cost_adjustment_factor must be in [0.7, 1.4].\n"
        "- If prediction is not trustworthy, set approved=false and explain in issues.\n"
        "- Do not compute profit/p10/p50/p90/risk.\n"
        "- No markdown/code fences/extra keys."
    )

    try:
        print(f"[agent][api-call] provider=openai model={model} purpose=forecast_current_year", flush=True)
        prompt = f"{system_msg}\n\nInput JSON:\n{json.dumps(prompt_payload)}"
        response = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        text = response.choices[0].message.content or ""
        parsed = _parse_json_payload(text)
    except Exception as exc:
        print(f"[agent][tool] llm-forecast source=error error={exc}", flush=True)
        return None, f"LLM call failed: {exc}"

    predictions_raw = parsed.get("predictions") if isinstance(parsed, dict) else None
    if not isinstance(predictions_raw, list):
        preview = text[:600].replace("\n", " ")
        print(
            f"[agent][tool] llm-forecast source=empty reason=missing_predictions_list raw_preview={preview}",
            flush=True,
        )
        return None, "LLM response missing predictions list"

    out: Dict[str, Dict[str, float]] = {}
    dropped_rows = 0
    for item in predictions_raw:
        if not isinstance(item, dict):
            dropped_rows += 1
            continue
        crop_name = str(item.get("crop_name") or "").strip()
        if not crop_name:
            dropped_rows += 1
            continue
        pred = item.get("prediction") if isinstance(item.get("prediction"), dict) else item
        quality = item.get("quality_checks") if isinstance(item.get("quality_checks"), dict) else {}
        yhat = _coerce_float(pred.get("yield_forecast"), 0.0)
        phat = _coerce_float(pred.get("price_forecast"), 0.0)
        chat = _coerce_float(pred.get("cost_per_acre"), 0.0)
        crop_key = _canonical_crop_key(crop_name)
        yield_imputed = False
        price_imputed = False
        if yhat <= 0.0:
            yhat = float(yield_baseline.get(crop_key, DEFAULT_YIELD_BY_CROP.get(crop_key, 120.0)))
            yield_imputed = True
            print(
                f"[agent][tool] llm-forecast crop={crop_name} yield_imputed_from_baseline={yhat:.4f}",
                flush=True,
            )
        if phat <= 0.0:
            baseline = price_baseline.get(crop_key)
            phat = float(baseline if baseline and baseline > 0.0 else DEFAULT_PRICE_BY_CROP.get(crop_key, 3.5))
            price_imputed = True
            print(
                f"[agent][tool] llm-forecast crop={crop_name} price_imputed_from_baseline={phat:.4f}",
                flush=True,
            )
        if not crop_key:
            dropped_rows += 1
            print(
                f"[agent][tool] llm-forecast row-dropped crop={crop_name or 'unknown'} reason=invalid_crop_key raw_pred={pred}",
                flush=True,
            )
            continue
        issues = quality.get("issues") if isinstance(quality.get("issues"), list) else []
        approved = bool(quality.get("approved", True))
        only_missing_price_issue = all(
            isinstance(x, str) and "no price data available" in x.lower() for x in issues
        ) if issues else False
        soft_approved = (not approved) and price_imputed and only_missing_price_issue

        row_obj = {
            "yield_forecast": float(yhat),
            "price_forecast": float(phat),
            "cost_per_acre": float(chat) if chat > 0.0 else 0.0,
            "cost_adjustment_factor": float(_coerce_float(pred.get("cost_adjustment_factor"), 1.0)),
            "confidence": max(0.0, min(1.0, _coerce_float(item.get("confidence"), 0.6))),
            "reasoning": str(item.get("reasoning") or "").strip(),
            "approved": approved,
            "soft_approved": soft_approved,
            "price_imputed": price_imputed,
            "yield_imputed": yield_imputed,
            "issues": issues,
        }
        if crop_key in out:
            prev_conf = float(out[crop_key].get("confidence", 0.0))
            if row_obj["confidence"] <= prev_conf:
                dropped_rows += 1
                print(
                    f"[agent][tool] llm-forecast row-dropped crop={crop_key} reason=duplicate_lower_confidence prev={prev_conf:.2f} new={row_obj['confidence']:.2f}",
                    flush=True,
                )
                continue
        out[crop_key] = row_obj

    if not out:
        preview = text[:600].replace("\n", " ")
        print(
            f"[agent][tool] llm-forecast source=empty reason=no_valid_prediction_rows dropped_rows={dropped_rows} raw_preview={preview}",
            flush=True,
        )
        return None, "LLM returned empty results"
    print(
        f"[agent][tool] llm-forecast source=ok crops={list(out.keys())} dropped_rows={dropped_rows}",
        flush=True,
    )
    for crop_key, pred in out.items():
        print(
            f"[agent][tool] llm-forecast crop={crop_key} approved={pred['approved']} soft_approved={pred.get('soft_approved', False)} price_imputed={pred.get('price_imputed', False)} yield_imputed={pred.get('yield_imputed', False)} yield={pred['yield_forecast']:.4f} price={pred['price_forecast']:.4f} cost={pred['cost_per_acre']:.4f} factor={pred['cost_adjustment_factor']:.3f} confidence={pred['confidence']:.2f} issues={pred['issues']}",
            flush=True,
        )
    return out, None


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


def _prediction_is_plausible(
    pred_yield: float,
    pred_price: float,
    pred_factor: float,
    yield_stats: Dict[str, float],
    price_stats: Dict[str, float],
) -> Tuple[bool, List[str]]:
    issues: List[str] = []

    # Absolute bounds.
    if pred_yield <= 0 or pred_yield > 50000:
        issues.append("yield_out_of_abs_range")
    if pred_price < 0.05 or pred_price > 100:
        issues.append("price_out_of_abs_range")
    if pred_factor < 0.7 or pred_factor > 1.4:
        issues.append("cost_factor_out_of_range")

    # Relative-to-history bounds.
    y_last = max(1.0, float(yield_stats.get("last", 1.0)))
    p_mean = max(0.05, float(price_stats.get("mean", 0.05)))
    if pred_yield < 0.4 * y_last or pred_yield > 2.5 * y_last:
        issues.append("yield_out_of_hist_range")
    if pred_price < 0.4 * p_mean or pred_price > 2.5 * p_mean:
        issues.append("price_out_of_hist_range")

    return (len(issues) == 0), issues


def compute_forecasts(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
) -> List[Dict]:
    llm_predictions, llm_err = _llm_predict_current_year(
        farm_profile=farm_profile,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs_per_acre,
        weather=weather,
    )
    if llm_err:
        print(f"[agent][tool] llm-forecast source=fallback reason={llm_err}", flush=True)

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
        # Fallback baseline must be last-year observed values (no synthetic drift).
        last_year_yield_forecast = max(0.01, float(yield_stats["last"]))
        last_year_price_forecast = max(0.05, float(price_stats["last"]))

        llm_item = (llm_predictions or {}).get(_canonical_crop_key(crop), {})
        llm_gate_pass = isinstance(llm_item, dict) and bool(llm_item)
        if (
            isinstance(llm_item, dict)
            and llm_item.get("yield_forecast")
            and llm_item.get("price_forecast")
            and llm_gate_pass
        ):
            yield_forecast = float(llm_item["yield_forecast"])
            price_forecast = max(0.05, float(llm_item["price_forecast"]))
            forecast_source = "llm_api_based"
            forecast_confidence = float(llm_item.get("confidence") or 0.0)
        else:
            # If LLM is unavailable/unapproved, use last observed year, not trend projection.
            yield_forecast = float(last_year_yield_forecast)
            price_forecast = float(last_year_price_forecast)
            forecast_source = "fallback_last_year_observed"
            forecast_confidence = 0.0
        print(
            f"[agent][tool] compute crop={str(crop).lower()} forecast_source={forecast_source} yield={yield_forecast:.4f} price={price_forecast:.4f}",
            flush=True,
        )

        api_cost = float(costs_per_acre.get(crop, costs_per_acre.get(crop.lower(), 700.0)))
        llm_cost = float(llm_item.get("cost_per_acre") or 0.0) if isinstance(llm_item, dict) else 0.0
        llm_factor = float(llm_item.get("cost_adjustment_factor") or 1.0) if isinstance(llm_item, dict) else 1.0
        # Guardrail: always derive final cost from API baseline via bounded LLM factor.
        llm_factor = max(0.7, min(1.4, llm_factor))
        if forecast_source == "llm_api_based":
            cost_per_acre = api_cost * llm_factor
            cost_source = "llm_api_based_factor_bounded"
        else:
            cost_per_acre = api_cost
            cost_source = "api_or_default"
        print(
            f"[agent][tool] compute-cost crop={str(crop).lower()} cost_source={cost_source} api_cost={api_cost:.4f} llm_abs_cost={llm_cost:.4f} factor={llm_factor:.3f} cost_per_acre={cost_per_acre:.4f}",
            flush=True,
        )
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
                "forecast_source": forecast_source,
                "forecast_confidence": round(float(forecast_confidence), 2),
                "cost_per_acre": round(float(cost_per_acre), 2),
                "cost_source": cost_source,
            }
        )

    results.sort(key=lambda r: r["expected_profit"], reverse=True)
    return results
