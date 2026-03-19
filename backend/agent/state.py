from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class AgentState(TypedDict, total=False):
    farm_profile: Dict[str, Any]
    api_plan: Dict[str, Any]
    analysis_inputs: Dict[str, Any]
    agent2_predictions: Dict[str, Any]
    datasets_summary: Dict[str, Any]
    crop_results: List[Dict[str, Any]]
    weather_summary: str
    market_outlook: str
    errors: List[str]
