from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Optional


ANALYSIS_STAGES = [
    {"id": "data_ingestion", "title": "Data Ingestion", "running_progress": 12, "completed_progress": 20},
    {"id": "weather_analysis", "title": "Weather Analysis", "running_progress": 32, "completed_progress": 40},
    {"id": "yield_modeling", "title": "Yield Modeling", "running_progress": 52, "completed_progress": 60},
    {"id": "market_forecast", "title": "Market Forecasting", "running_progress": 72, "completed_progress": 80},
    {"id": "profit_simulation", "title": "Profit Simulation", "running_progress": 92, "completed_progress": 100},
]


_STAGE_MAP = {stage["id"]: stage for stage in ANALYSIS_STAGES}
_JOBS: Dict[str, Dict[str, Any]] = {}
_LOCK = Lock()

# Used across this module to timestamp in-memory job state updates.
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# Used by stage helpers to resolve stage metadata and progress values.
def _stage_info(stage_id: str) -> Dict[str, Any]:
    return dict(_STAGE_MAP.get(stage_id, {"id": stage_id, "title": stage_id.replace("_", " ").title(), "running_progress": 0, "completed_progress": 0}))

# Used by `/api/analysis/create` and `/api/analysis/start` to create a pollable job record.
def create_analysis_job(job_id: str, user_id: str, farm_profile: Dict[str, Any]) -> Dict[str, Any]:
    job = {
        "job_id": job_id,
        "user_id": user_id,
        "status": "queued",
        "stage_id": "queued",
        "stage_title": "Queued",
        "progress_pct": 0,
        "message": "Queued for backend analysis...",
        "logs": [
            {
                "ts": _utc_now(),
                "step": "Backend Sync",
                "text": "Queued for backend analysis...",
                "is_ok": False,
            }
        ],
        "result": None,
        "error": None,
        "farm_profile": farm_profile,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
    }
    with _LOCK:
        _JOBS[job_id] = job
    return deepcopy(job)

# Used by analysis nodes and finalizers to append UI-visible backend log lines.
def append_analysis_log(job_id: str, step: str, text: str, is_ok: bool = False) -> None:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        logs = list(job.get("logs") or [])
        logs.append({"ts": _utc_now(), "step": step, "text": text, "is_ok": bool(is_ok)})
        job["logs"] = logs[-40:]
        job["updated_at"] = _utc_now()

# Used by graph-stage helpers when a backend stage begins running.
def set_analysis_stage(job_id: str, stage_id: str, message: Optional[str] = None) -> None:
    stage = _stage_info(stage_id)
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["stage_id"] = stage["id"]
        job["stage_title"] = stage["title"]
        job["progress_pct"] = int(stage["running_progress"])
        if message:
            job["message"] = message
        job["updated_at"] = _utc_now()
    if message:
        append_analysis_log(job_id, stage["title"], message, False)

# Used by graph-stage helpers when a backend stage completes.
def complete_analysis_stage(job_id: str, stage_id: str, message: Optional[str] = None) -> None:
    stage = _stage_info(stage_id)
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["stage_id"] = stage["id"]
        job["stage_title"] = stage["title"]
        job["progress_pct"] = int(stage["completed_progress"])
        if message:
            job["message"] = message
        job["updated_at"] = _utc_now()
    if message:
        append_analysis_log(job_id, stage["title"], message, True)

# Used by `_run_analysis_job` in `server.py` to store successful job results.
def complete_analysis_job(job_id: str, result: Dict[str, Any]) -> None:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        job["status"] = "completed"
        job["stage_id"] = "completed"
        job["stage_title"] = "Completed"
        job["progress_pct"] = 100
        job["message"] = "Analysis complete."
        job["result"] = result
        job["error"] = None
        job["updated_at"] = _utc_now()
    append_analysis_log(job_id, "Backend Sync", "Analysis complete.", True)

# Used by `_run_analysis_job` in `server.py` to store terminal failure state.
def fail_analysis_job(job_id: str, error: str) -> None:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return
        job["status"] = "failed"
        job["stage_id"] = "failed"
        job["stage_title"] = "Failed"
        job["message"] = error
        job["error"] = error
        job["updated_at"] = _utc_now()
    append_analysis_log(job_id, "Backend Sync", error, False)

# Used by analysis job endpoints to return a safe copy of current job state.
def get_analysis_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _LOCK:
        job = _JOBS.get(job_id)
        if not job:
            return None
        return deepcopy(job)
