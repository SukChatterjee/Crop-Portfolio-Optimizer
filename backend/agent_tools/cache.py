import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd


def get_cache_dir() -> Path:
    cache_dir = os.environ.get("CACHE_DIR")
    if cache_dir:
        base = Path(cache_dir)
    else:
        base = Path(__file__).resolve().parents[1] / ".cache"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _hash_key(key: Any) -> str:
    payload = json.dumps(key, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def raw_cache_path(namespace: str, key: Any) -> Path:
    d = get_cache_dir() / namespace / "raw"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{_hash_key(key)}.json"


def parquet_cache_path(namespace: str, key: Any) -> Path:
    d = get_cache_dir() / namespace / "processed"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{_hash_key(key)}.parquet"


def load_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def load_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def save_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
    except Exception:
        return


def is_fresh(path: Path, ttl_hours: Optional[int]) -> bool:
    if not path.exists():
        return False
    if ttl_hours is None:
        return True
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return modified >= datetime.now(timezone.utc) - timedelta(hours=ttl_hours)


def cached_json(
    namespace: str,
    key: Any,
    fetcher: Callable[[], Any],
    ttl_hours: Optional[int] = None,
    force_refresh: bool = False,
) -> Any:
    path = raw_cache_path(namespace, key)
    if not force_refresh and is_fresh(path, ttl_hours):
        cached = load_json(path)
        if cached is not None:
            print(f"[agent][cache-hit] namespace={namespace} key_hash={_hash_key(key)[:12]}", flush=True)
            return cached
    print(f"[agent][api-call] namespace={namespace} key_hash={_hash_key(key)[:12]}", flush=True)
    value = fetcher()
    save_json(path, value)
    return value
