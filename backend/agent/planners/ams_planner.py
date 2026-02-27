from __future__ import annotations

import json
import os
from typing import Any, Dict

from openai import OpenAI


def _extract_json_object(text: str) -> Dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        return {}
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass

    start = payload.find("{")
    end = payload.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(payload[start : end + 1])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            return {}
    return {}


def _default_no_data(crop_name: str, state_name: str, lookback_days: int, reason: str) -> Dict[str, Any]:
    return {
        "crop": crop_name,
        "state": state_name,
        "lookback_days": int(lookback_days),
        "plan": {"status": "no_data", "reason": reason},
    }


def plan_ams_for_crop(crop_name: str, state_name: str = "OHIO", lookback_days: int = 1095) -> Dict[str, Any]:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _default_no_data(crop_name, state_name, lookback_days, "missing_openai_api_key")

    model = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    client = OpenAI(api_key=api_key, max_retries=int(os.environ.get("OPENAI_MAX_RETRIES", "2")))

    schema = {
        "crop": crop_name,
        "state": state_name,
        "lookback_days": int(lookback_days),
        "plan": {
            "catalog_query": {
                "endpoint": "/reports",
                "filters": {
                    "contains_any": [],
                    "exclude_any": [],
                },
                "max_candidates": 30,
            },
            "ranking_heuristics": [
                "Prefer reports whose slug_name/title contains the crop keyword",
                "Prefer reports with frequent publication and recent dates",
                "Prefer prices with clear units like $/BU or $/cwt",
                "Prefer reports relevant to OHIO if state/location appears in metadata; otherwise accept national",
            ],
            "selection": {
                "top_k_slug_ids": [],
                "fetch_details_for_each": True,
                "final_choice_rule": "choose slug_id with highest parsed_row_count in last lookback_days and matching unit",
            },
        },
        "parse_hints": {
            "acceptable_units_regex": r"(\$\s*/\s*BU|\$/BU|BUSHEL|\$\s*/\s*CWT|\$/CWT|CWT)",
            "price_fields_priority": [
                "weighted_avg",
                "weighted_average",
                "price",
                "low_price",
                "high_price",
                "range",
            ],
        },
    }

    prompt = (
        "You are planning AMS MyMarketNews report discovery for a crop. Return strict JSON only.\n"
        "Rules:\n"
        "1) Do NOT invent slug IDs.\n"
        "2) top_k_slug_ids must be an empty list unless explicit IDs are provided by inputs.\n"
        "3) Build dynamic contains_any/exclude_any terms using crop/state/market language.\n"
        "4) Keep endpoint as '/reports'.\n"
        "5) If insufficient confidence, return plan.status='no_data' with a reason.\n\n"
        f"Return JSON in this structure:\n{json.dumps(schema)}"
    )

    try:
        print(f"[agent][api-call] provider=openai model={model} purpose=ams_param_planner", flush=True)
        res = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
        )
        parsed = _extract_json_object(res.choices[0].message.content or "{}")
        if not isinstance(parsed, dict) or not parsed:
            return _default_no_data(crop_name, state_name, lookback_days, "empty_planner_response")
        parsed.setdefault("crop", crop_name)
        parsed.setdefault("state", state_name)
        parsed.setdefault("lookback_days", int(lookback_days))
        return parsed
    except Exception as exc:
        print(f"[agent][tool] ams-planner source=error crop={crop_name} error={exc}", flush=True)
        return _default_no_data(crop_name, state_name, lookback_days, f"planner_error:{exc}")

