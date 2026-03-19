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
    "soybeans": 11.0,
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

DEFAULT_UNITS_BY_CROP = {
    "corn": {"yield_unit": "bu/acre", "price_unit": "$/bu"},
    "wheat": {"yield_unit": "bu/acre", "price_unit": "$/bu"},
    "soybeans": {"yield_unit": "bu/acre", "price_unit": "$/bu"},
    "rice": {"yield_unit": "lb/acre", "price_unit": "$/cwt"},
    "cotton": {"yield_unit": "lb/acre", "price_unit": "$/lb"},
    "tomatoes": {"yield_unit": "cwt/acre", "price_unit": "$/cwt"},
    "potatoes": {"yield_unit": "cwt/acre", "price_unit": "$/cwt"},
    "onions": {"yield_unit": "cwt/acre", "price_unit": "$/cwt"},
    "apples": {"yield_unit": "cwt/acre", "price_unit": "$/cwt"},
    "lettuce": {"yield_unit": "cwt/acre", "price_unit": "$/cwt"},
}
BUSHEL_WEIGHT_LB_BY_CROP = {
    "corn": 56.0,
    "soybeans": 60.0,
    "wheat": 60.0,
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
    fred_data: Optional[Dict] = None,
) -> Dict:
    weather_features = weather.get("features", {}) if isinstance(weather, dict) else {}
    fred_summary = (fred_data or {}).get("summary", {}) if isinstance(fred_data, dict) else {}
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
        "fred_summary": fred_summary,
        "costs_per_acre": costs_per_acre,
        "nass_series": [],
        "price_series": [],
        "fred_series": [],
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
                "price_unit": str(row.get("price_unit") or "").strip(),
                "source": str(row.get("source") or "").strip(),
            }
            for _, row in price_df.iterrows()
        ]
    fred_series = (fred_data or {}).get("series") if isinstance(fred_data, dict) else []
    if isinstance(fred_series, list):
        payload["fred_series"] = [
            {
                "series_id": row.get("series_id"),
                "title": row.get("title"),
                "purpose": row.get("purpose"),
                "latest_value": row.get("latest_value"),
                "latest_change_pct": row.get("latest_change_pct"),
                "observations": row.get("observations", [])[-12:],
            }
            for row in fred_series
            if isinstance(row, dict)
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


def _api_yield_unit_by_crop(selected_crops: List[str], nass_df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if nass_df is None or nass_df.empty or "yield_unit" not in nass_df.columns:
        return out
    for crop in selected_crops:
        key = _canonical_crop_key(crop)
        crop_rows = nass_df.loc[nass_df["crop"].astype(str).str.lower() == key]
        if crop_rows.empty:
            continue
        if "year" in crop_rows.columns:
            crop_rows = crop_rows.sort_values("year")
        units = [
            str(value).strip()
            for value in crop_rows["yield_unit"].tolist()
            if str(value).strip()
        ]
        if units:
            out[key] = units[-1]
    return out


def _api_yield_desc_by_crop(selected_crops: List[str], nass_df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if nass_df is None or nass_df.empty or "yield_desc" not in nass_df.columns:
        return out
    for crop in selected_crops:
        key = _canonical_crop_key(crop)
        crop_rows = nass_df.loc[nass_df["crop"].astype(str).str.lower() == key]
        if crop_rows.empty:
            continue
        if "year" in crop_rows.columns:
            crop_rows = crop_rows.sort_values("year")
        descs = [
            str(value).strip()
            for value in crop_rows["yield_desc"].tolist()
            if str(value).strip()
        ]
        if descs:
            out[key] = descs[-1]
    return out


def _price_meta_by_crop(selected_crops: List[str], price_df: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if price_df is None or price_df.empty:
        return out
    for crop in selected_crops:
        key = _canonical_crop_key(crop)
        crop_rows = price_df.loc[price_df["crop"].astype(str).str.lower() == key]
        if crop_rows.empty:
            continue
        if "year" in crop_rows.columns:
            crop_rows = crop_rows.sort_values("year")
        latest = crop_rows.iloc[-1]
        out[key] = {
            "price_unit": str(latest.get("price_unit") or "").strip(),
            "source": str(latest.get("source") or "").strip(),
        }
    return out


def _default_units_for_crop(crop_name: str) -> Dict[str, str]:
    crop_key = _canonical_crop_key(crop_name)
    defaults = DEFAULT_UNITS_BY_CROP.get(crop_key, {})
    yield_unit = str(defaults.get("yield_unit") or "units/acre")
    price_unit = str(defaults.get("price_unit") or "$/unit")
    calc_yield_unit = _calc_yield_unit_from_price_unit(price_unit) or yield_unit
    return {
        "yield_unit": yield_unit,
        "price_unit": price_unit,
        "calc_yield_unit": calc_yield_unit,
    }


def _basis_from_unit(unit: str) -> str:
    text = str(unit or "").strip().lower().replace(" ", "")
    if not text:
        return ""
    if "bu" in text or "bushel" in text:
        return "bu"
    if "cwt" in text:
        return "cwt"
    if "/lb" in text or "lb/" in text or "lb" in text or "pound" in text:
        return "lb"
    if "ton" in text:
        return "ton"
    if "box" in text:
        return "box"
    return ""


def _unit_label_from_basis(basis: str) -> str:
    mapping = {
        "bu": "bu/acre",
        "cwt": "cwt/acre",
        "lb": "lb/acre",
        "ton": "ton/acre",
        "box": "box/acre",
    }
    return mapping.get(str(basis or "").strip().lower(), "")


def _calc_yield_unit_from_price_unit(price_unit: str) -> str:
    return _unit_label_from_basis(_basis_from_unit(price_unit))


def _expected_yield_basis_for_crop(crop_name: str) -> str:
    defaults = _default_units_for_crop(crop_name)
    return _basis_from_unit(defaults.get("yield_unit", ""))


def _api_yield_unit_is_compatible(crop_name: str, api_yield_unit: str) -> bool:
    actual = _basis_from_unit(api_yield_unit)
    expected = _expected_yield_basis_for_crop(crop_name)
    if not actual or not expected:
        return True
    return actual == expected


def _convert_mass_to_bushels(crop_name: str, value: float, from_basis: str) -> Optional[float]:
    crop_key = _canonical_crop_key(crop_name)
    pounds_per_bushel = BUSHEL_WEIGHT_LB_BY_CROP.get(crop_key)
    if not pounds_per_bushel:
        return None
    pounds = _convert_between_bases(value, from_basis, "lb")
    if pounds is None:
        return None
    return float(pounds) / pounds_per_bushel


def _is_grain_descriptor(desc: str) -> bool:
    text = str(desc or "").strip().lower()
    if not text:
        return False
    bad_cues = ("silage", "green chop", "forage", "haylage", "fresh market")
    if any(cue in text for cue in bad_cues):
        return False
    good_cues = ("grain", "dry", "all classes")
    return any(cue in text for cue in good_cues)


def _convert_incompatible_api_yield_if_safe(
    crop_name: str,
    raw_yield: float,
    api_yield_unit: str,
    yield_desc: str,
    target_price_unit: str,
) -> Optional[Dict[str, object]]:
    crop_key = _canonical_crop_key(crop_name)
    actual_basis = _basis_from_unit(api_yield_unit)
    target_basis = _basis_from_unit(target_price_unit) or _expected_yield_basis_for_crop(crop_name)
    if not actual_basis or not target_basis:
        return None

    converted_value: Optional[float] = None
    if target_basis == "bu":
        if not _is_grain_descriptor(yield_desc):
            return None
        converted_value = _convert_mass_to_bushels(crop_name, raw_yield, actual_basis)
    else:
        converted_value = _convert_between_bases(raw_yield, actual_basis, target_basis)

    if converted_value is None:
        return None

    return {
        "yield_forecast": float(converted_value),
        "yield_unit": _unit_label_from_basis(target_basis) or _default_units_for_crop(crop_key)["yield_unit"],
        "calc_yield_for_profit": float(converted_value),
        "calc_yield_unit": _unit_label_from_basis(target_basis) or _default_units_for_crop(crop_key)["calc_yield_unit"],
    }


def _convert_between_bases(value: float, from_basis: str, to_basis: str) -> Optional[float]:
    if not from_basis or not to_basis:
        return None
    if from_basis == to_basis:
        return float(value)
    factors = {
        ("lb", "cwt"): 0.01,
        ("cwt", "lb"): 100.0,
        ("lb", "ton"): 0.0005,
        ("ton", "lb"): 2000.0,
        ("cwt", "ton"): 0.05,
        ("ton", "cwt"): 20.0,
    }
    factor = factors.get((from_basis, to_basis))
    if factor is None:
        return None
    return float(value) * factor


def _normalize_yield_for_profit(
    crop_name: str,
    raw_yield: float,
    api_yield_unit: str = "",
    yield_unit: str = "",
    price_unit: str = "",
    explicit_value: Optional[float] = None,
    explicit_unit: str = "",
) -> Dict[str, object]:
    defaults = _default_units_for_crop(crop_name)
    api_unit = str(api_yield_unit or "").strip()
    agent_unit = str(yield_unit or "").strip()
    resolved_yield_unit = str(api_unit or agent_unit or defaults["yield_unit"]).strip()
    resolved_price_unit = str(price_unit or defaults["price_unit"]).strip()
    value = max(0.01, float(raw_yield))
    raw_basis = _basis_from_unit(resolved_yield_unit)
    price_basis = _basis_from_unit(resolved_price_unit)
    explicit_basis = _basis_from_unit(explicit_unit)
    default_calc_unit = str(defaults["calc_yield_unit"]).strip()

    if price_basis:
        calc_basis = price_basis
        resolved_calc_unit = _unit_label_from_basis(price_basis) or default_calc_unit or resolved_yield_unit
    else:
        calc_basis = explicit_basis or raw_basis
        resolved_calc_unit = (
            str(explicit_unit).strip()
            or _unit_label_from_basis(calc_basis)
            or default_calc_unit
            or resolved_yield_unit
        )

    deterministic = _convert_between_bases(value, raw_basis, calc_basis)
    if deterministic is None:
        deterministic = value

    agent_basis = _basis_from_unit(agent_unit)
    if api_unit and agent_unit and raw_basis and agent_basis and raw_basis != agent_basis:
        print(
            f"[agent][tool] unit-source crop={_canonical_crop_key(crop_name)} api_yield_unit={api_unit} agent_yield_unit={agent_unit} chosen={resolved_yield_unit}",
            flush=True,
        )

    if explicit_value is None or explicit_value <= 0.0:
        return {
            "yield_unit": resolved_yield_unit,
            "price_unit": resolved_price_unit,
            "calc_yield_unit": resolved_calc_unit,
            "calc_yield_for_profit": float(deterministic),
        }

    explicit = float(explicit_value)
    if explicit_basis and calc_basis and explicit_basis != calc_basis:
        converted_explicit = _convert_between_bases(explicit, explicit_basis, calc_basis)
        if converted_explicit is not None:
            explicit = converted_explicit

    if deterministic <= 0.0:
        return {
            "yield_unit": resolved_yield_unit,
            "price_unit": resolved_price_unit,
            "calc_yield_unit": resolved_calc_unit,
            "calc_yield_for_profit": explicit,
        }

    ratio = max(explicit, deterministic) / max(min(explicit, deterministic), 0.0001)
    if ratio > 1.2:
        print(
            f"[agent][tool] unit-normalize crop={_canonical_crop_key(crop_name)} source=deterministic raw_yield={value:.4f} explicit_calc_yield={explicit:.4f} normalized_yield={deterministic:.4f} yield_unit={resolved_yield_unit} price_unit={resolved_price_unit}",
            flush=True,
        )
        chosen = deterministic
    else:
        chosen = explicit
    return {
        "yield_unit": resolved_yield_unit,
        "price_unit": resolved_price_unit,
        "calc_yield_unit": resolved_calc_unit,
        "calc_yield_for_profit": float(chosen),
    }


def _llm_predict_current_year(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
    fred_data: Optional[Dict] = None,
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
        fred_data=fred_data,
    )
    selected_crops = [str(c).strip() for c in (farm_profile.get("selected_crops") or []) if str(c).strip()]
    price_baseline = _price_baseline_by_crop(selected_crops, price_df)
    yield_baseline = _yield_baseline_by_crop(selected_crops, nass_df)
    api_yield_units = _api_yield_unit_by_crop(selected_crops, nass_df)

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
        "\"prediction\":{\"yield_forecast\":0.0,\"price_forecast\":0.0,\"calc_yield_for_profit\":0.0,\"yield_unit\":\"...\",\"calc_yield_unit\":\"...\",\"price_unit\":\"...\",\"cost_adjustment_factor\":1.0,\"cost_per_acre\":0.0},"
        "\"quality_checks\":{\"unit_consistency\":true,\"range_sanity\":true,\"data_sufficiency\":true,\"approved\":true,\"issues\":[]},"
        "\"confidence\":0.0,"
        "\"reasoning\":\"short reason\""
        "}"
        "]"
        "}.\n"
        "Rules:\n"
        "- Use ONLY the provided API-derived input.\n"
        "- Use FRED macro indicators when they help explain price pressure, financing conditions, export competitiveness, or input-cost trends.\n"
        "- Agent 2 is responsible for normalizing units so profit math can use calc_yield_for_profit * price_forecast consistently.\n"
        "- If raw yield units do not match the price basis, convert yield into the price basis and return that in calc_yield_for_profit.\n"
        "- Always return yield_unit, calc_yield_unit, and price_unit.\n"
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
        calc_yield_for_profit = _coerce_float(pred.get("calc_yield_for_profit"), 0.0)
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
        api_yield_unit = str(api_yield_units.get(crop_key, "")).strip()
        if api_yield_unit and not _api_yield_unit_is_compatible(crop_name, api_yield_unit):
            dropped_rows += 1
            print(
                f"[agent][tool] llm-forecast row-dropped crop={crop_key} reason=incompatible_api_yield_unit api_yield_unit={api_yield_unit}",
                flush=True,
            )
            continue
        normalization = _normalize_yield_for_profit(
            crop_name,
            yhat,
            api_yield_unit=api_yield_unit,
            yield_unit=str(pred.get("yield_unit") or "").strip(),
            price_unit=str(pred.get("price_unit") or "").strip(),
            explicit_value=calc_yield_for_profit,
            explicit_unit=str(pred.get("calc_yield_unit") or "").strip(),
        )
        issues = quality.get("issues") if isinstance(quality.get("issues"), list) else []
        approved = bool(quality.get("approved", True))
        only_missing_price_issue = all(
            isinstance(x, str) and "no price data available" in x.lower() for x in issues
        ) if issues else False
        soft_approved = (not approved) and price_imputed and only_missing_price_issue

        row_obj = {
            "yield_forecast": float(yhat),
            "price_forecast": float(phat),
            "calc_yield_for_profit": float(normalization["calc_yield_for_profit"]),
            "yield_unit": str(normalization["yield_unit"]),
            "calc_yield_unit": str(normalization["calc_yield_unit"]),
            "price_unit": str(normalization["price_unit"]),
            "cost_per_acre": float(chat) if chat > 0.0 else 0.0,
            "cost_adjustment_factor": float(_coerce_float(pred.get("cost_adjustment_factor"), 1.0)),
            "confidence": 0.0,
            "reasoning": str(item.get("reasoning") or "").strip(),
            "approved": approved,
            "soft_approved": soft_approved,
            "price_imputed": price_imputed,
            "yield_imputed": yield_imputed,
            "issues": issues,
        }
        if crop_key in out:
            dropped_rows += 1
            print(
                f"[agent][tool] llm-forecast row-dropped crop={crop_key} reason=duplicate_replaced_with_latest",
                flush=True,
            )
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
            f"[agent][tool] llm-forecast crop={crop_key} approved={pred['approved']} soft_approved={pred.get('soft_approved', False)} price_imputed={pred.get('price_imputed', False)} yield_imputed={pred.get('yield_imputed', False)} yield={pred['yield_forecast']:.4f} calc_yield={pred['calc_yield_for_profit']:.4f} price={pred['price_forecast']:.4f} cost={pred['cost_per_acre']:.4f} factor={pred['cost_adjustment_factor']:.3f} issues={pred['issues']}",
            flush=True,
        )
    return out, None


def normalize_and_predict_inputs(
    farm_profile: Dict,
    nass_df: pd.DataFrame,
    price_df: pd.DataFrame,
    costs_per_acre: Dict[str, float],
    weather: Dict,
    fred_data: Optional[Dict] = None,
) -> Tuple[Optional[Dict[str, Dict[str, float]]], Optional[str]]:
    return _llm_predict_current_year(
        farm_profile=farm_profile,
        nass_df=nass_df,
        price_df=price_df,
        costs_per_acre=costs_per_acre,
        weather=weather,
        fred_data=fred_data,
    )


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
    fred_data: Optional[Dict] = None,
    agent2_predictions: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[Dict]:
    llm_predictions = agent2_predictions
    llm_err = None
    if agent2_predictions is None:
        llm_predictions, llm_err = _llm_predict_current_year(
            farm_profile=farm_profile,
            nass_df=nass_df,
            price_df=price_df,
            costs_per_acre=costs_per_acre,
            weather=weather,
            fred_data=fred_data,
        )
        if llm_err:
            print(f"[agent][tool] llm-forecast source=fallback reason={llm_err}", flush=True)
    else:
        print(
            f"[agent][tool] compute source=agent2_predictions crops={list((agent2_predictions or {}).keys())}",
            flush=True,
        )

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
    api_yield_units = _api_yield_unit_by_crop(selected_crops, nass_df)
    api_yield_descs = _api_yield_desc_by_crop(selected_crops, nass_df)
    price_meta_by_crop = _price_meta_by_crop(selected_crops, price_df)
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
        price_meta = price_meta_by_crop.get(_canonical_crop_key(crop), {})
        api_yield_unit = str(api_yield_units.get(_canonical_crop_key(crop), "")).strip()
        api_yield_desc = str(api_yield_descs.get(_canonical_crop_key(crop), "")).strip()
        incompatible_api_yield_unit = bool(api_yield_unit) and not _api_yield_unit_is_compatible(crop, api_yield_unit)
        resolved_price_unit = (
            str(price_meta.get("price_unit") or "").strip()
            or str(DEFAULT_UNITS_BY_CROP.get(_canonical_crop_key(crop), {}).get("price_unit", "$/unit"))
        )
        price_source = str(price_meta.get("source") or "").strip() or "default_baseline"

        if incompatible_api_yield_unit:
            converted = _convert_incompatible_api_yield_if_safe(
                crop_name=crop,
                raw_yield=float(last_year_yield_forecast),
                api_yield_unit=api_yield_unit,
                yield_desc=api_yield_desc,
                target_price_unit=resolved_price_unit,
            )
            if converted is not None:
                yield_forecast = float(converted["yield_forecast"])
                normalization = {
                    "yield_unit": str(converted["yield_unit"]),
                    "price_unit": resolved_price_unit,
                    "calc_yield_unit": str(converted["calc_yield_unit"]),
                    "calc_yield_for_profit": float(converted["calc_yield_for_profit"]),
                }
                print(
                    f"[agent][tool] compute-convert crop={str(crop).lower()} reason=incompatible_api_yield_unit api_yield_unit={api_yield_unit} api_yield_desc={api_yield_desc} converted_yield={yield_forecast:.4f} converted_yield_unit={normalization['yield_unit']}",
                    flush=True,
                )
            else:
                yield_forecast = float(DEFAULT_YIELD_BY_CROP.get(_canonical_crop_key(crop), max(0.01, float(last_year_yield_forecast))))
                normalization = _normalize_yield_for_profit(
                    crop,
                    yield_forecast,
                    api_yield_unit="",
                    yield_unit=str(DEFAULT_UNITS_BY_CROP.get(_canonical_crop_key(crop), {}).get("yield_unit", "")),
                    price_unit=resolved_price_unit,
                )
                print(
                    f"[agent][tool] compute-fallback crop={str(crop).lower()} reason=incompatible_api_yield_unit api_yield_unit={api_yield_unit} api_yield_desc={api_yield_desc} fallback_yield={yield_forecast:.4f} fallback_yield_unit={normalization['yield_unit']}",
                    flush=True,
                )
        else:
            yield_forecast = float(last_year_yield_forecast)
            normalization = _normalize_yield_for_profit(
                crop,
                yield_forecast,
                api_yield_unit=api_yield_unit,
                price_unit=resolved_price_unit,
            )
        calc_yield_for_profit = float(normalization["calc_yield_for_profit"])
        yield_unit = str(normalization["yield_unit"])
        calc_yield_unit = str(normalization["calc_yield_unit"])
        price_unit = str(normalization["price_unit"])
        price_forecast = float(last_year_price_forecast)
        if incompatible_api_yield_unit:
            if converted is not None:
                forecast_source = "converted_api_yield_incompatible_unit"
                forecast_confidence = 0.3
            else:
                forecast_source = "fallback_default_yield_incompatible_api_unit"
                forecast_confidence = 0.2
            effective_price_trend = 0.0
        elif price_source == "default_baseline":
            forecast_source = "deterministic_observed_yield_default_price"
            forecast_confidence = 0.35
            effective_price_trend = 0.0
        else:
            forecast_source = "deterministic_observed_inputs"
            forecast_confidence = 0.55
            effective_price_trend = price_trend
        if isinstance(llm_item, dict) and llm_item:
            print(
                f"[agent][tool] agent2-advisory crop={str(crop).lower()} action=ignored_for_final_numeric_forecast",
                flush=True,
            )
        print(
            f"[agent][tool] compute crop={str(crop).lower()} forecast_source={forecast_source} yield={yield_forecast:.4f} yield_unit={yield_unit} calc_yield={calc_yield_for_profit:.4f} calc_unit={calc_yield_unit} price={price_forecast:.4f} price_unit={price_unit}",
            flush=True,
        )

        api_cost_raw = costs_per_acre.get(crop, costs_per_acre.get(crop.lower()))
        if api_cost_raw is not None:
            api_cost = float(api_cost_raw)
            cost_per_acre = api_cost
            cost_source = "api_or_default"
        else:
            print(
                f"[agent][tool] compute-cost crop={str(crop).lower()} cost_source=missing_no_api_no_llm action=skip",
                flush=True,
            )
            continue
        print(
            f"[agent][tool] compute-cost crop={str(crop).lower()} cost_source={cost_source} api_cost={api_cost:.4f} cost_per_acre={cost_per_acre:.4f}",
            flush=True,
        )
        revenue_per_acre = calc_yield_for_profit * price_forecast
        profit_per_acre = revenue_per_acre - cost_per_acre
        expected_profit = acres * profit_per_acre

        uncertainty = max(0.10, min(0.40, 0.14 + 0.35 * weather_risk + 0.25 * abs(effective_price_trend) + 0.15 * precip_cv))
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
                "yield_unit": yield_unit,
                "price_forecast": round(float(price_forecast), 4),
                "price_unit": price_unit,
                "calc_yield_for_profit": round(float(calc_yield_for_profit), 4),
                "calc_yield_unit": calc_yield_unit,
                "revenue_per_acre": round(float(revenue_per_acre), 2),
                "profit_per_acre": round(float(profit_per_acre), 2),
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
