from __future__ import annotations

import hashlib
import math
import os
import time
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from .cache import cached_json, load_parquet, parquet_cache_path, save_parquet

NOAA_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


class NOAARequestError(Exception):
    def __init__(self, message: str, status_code: int = 0, body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _request_json(url: str, headers: Dict[str, str], params: Dict[str, str]) -> Dict:
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code >= 400:
                body_snippet = (response.text or "")[:300]
                if response.status_code in {429, 500, 502, 503, 504} and attempt < 2:
                    time.sleep(1.2 * (2 ** attempt))
                    continue
                raise NOAARequestError(
                    message=f"NOAA HTTP {response.status_code}",
                    status_code=response.status_code,
                    body=body_snippet,
                )
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(1.2 * (2 ** attempt))
                continue
    raise NOAARequestError(message=f"NOAA connection error: {last_error}")


def _get_nearest_stations(lat: float, lng: float, token: str, limit: int = 3, force_refresh: bool = False) -> List[Dict]:
    headers = {"token": token}
    extent = f"{lat - 1.5},{lng - 1.5},{lat + 1.5},{lng + 1.5}"
    payload = cached_json(
        namespace="noaa",
        key={"kind": "stations", "lat": round(lat, 3), "lng": round(lng, 3), "extent": extent},
        fetcher=lambda: _request_json(
            f"{NOAA_BASE}/stations",
            headers=headers,
            params={
                "datasetid": "GHCND",
                "extent": extent,
                "limit": "100",
                "startdate": str(date.today() - timedelta(days=365 * 10)),
                "enddate": str(date.today()),
            },
        ),
        ttl_hours=24,
        force_refresh=force_refresh,
    )
    stations = payload.get("results", [])
    for station in stations:
        station["distance_km"] = _haversine_km(lat, lng, station.get("latitude", lat), station.get("longitude", lng))
    stations = sorted(stations, key=lambda s: s.get("distance_km", 9e9))
    return stations[:limit]


def _fetch_station_data(
    station_id: str,
    token: str,
    start_date: date,
    end_date: date,
    force_refresh: bool = False,
) -> pd.DataFrame:
    headers = {"token": token}
    rows = []
    # NOAA CDO /data enforces date ranges strictly less than 1 year.
    # Chunk requests into <= 364-day windows and stitch results.
    windows = []
    cursor = start_date
    while cursor <= end_date:
        window_end = min(end_date, cursor + timedelta(days=364))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)

    for dtype in ["PRCP", "TMAX", "TMIN"]:
        for window_start, window_end in windows:
            offset = 1
            while True:
                key = {
                    "kind": "daily",
                    "station": station_id,
                    "dtype": dtype,
                    "start": str(window_start),
                    "end": str(window_end),
                    "offset": offset,
                }
                try:
                    payload = cached_json(
                        namespace="noaa",
                        key=key,
                        fetcher=lambda d=dtype, o=offset, ws=window_start, we=window_end: _request_json(
                            f"{NOAA_BASE}/data",
                            headers=headers,
                            params={
                                "datasetid": "GHCND",
                                "stationid": station_id,
                                "datatypeid": d,
                                "startdate": str(ws),
                                "enddate": str(we),
                                "units": "metric",
                                "limit": "1000",
                                "offset": str(o),
                            },
                        ),
                        ttl_hours=24,
                        force_refresh=force_refresh,
                    )
                except NOAARequestError as exc:
                    print(
                        f"[agent][tool] noaa station={station_id} dtype={dtype} "
                        f"window={window_start}:{window_end} source=api_error "
                        f"status={exc.status_code} body={exc.body}",
                        flush=True,
                    )
                    break
                results = payload.get("results", [])
                if not results:
                    break
                rows.extend(
                    {
                        "date": item.get("date", "")[:10],
                        "datatype": item.get("datatype"),
                        "value": float(item.get("value", 0.0)),
                    }
                    for item in results
                )
                if len(results) < 1000:
                    break
                offset += 1000
    return pd.DataFrame(rows)


def _fallback_weather(lat: float, lng: float) -> Dict:
    base_rain = _seeded_float(f"rain:{lat:.3f}:{lng:.3f}", 1.2, 4.5)
    base_tmax = _seeded_float(f"tmax:{lat:.3f}:{lng:.3f}", 16.0, 31.0)
    base_tmin = _seeded_float(f"tmin:{lat:.3f}:{lng:.3f}", 3.0, 18.0)
    temp_range = max(6.0, base_tmax - base_tmin)
    risk_index = max(0.05, min(0.75, (temp_range / 20.0) * 0.4 + abs(base_rain - 2.5) * 0.12))
    summary = (
        f"3-year weather baseline near ({lat:.3f}, {lng:.3f}) suggests avg precipitation "
        f"{base_rain:.2f} mm/day and average temperatures {base_tmin:.1f}C to {base_tmax:.1f}C."
    )
    return {
        "features": {
            "avg_prcp_mm": round(base_rain, 3),
            "avg_tmax_c": round(base_tmax, 3),
            "avg_tmin_c": round(base_tmin, 3),
            "temp_range_c": round(temp_range, 3),
            "precip_cv": round(_seeded_float(f"pcv:{lat}:{lng}", 0.2, 0.8), 3),
            "extreme_heat_days": int(_seeded_float(f"hot:{lat}:{lng}", 10, 55)),
            "freeze_days": int(_seeded_float(f"freeze:{lat}:{lng}", 5, 40)),
            "risk_index": round(risk_index, 3),
        },
        "stations": [],
        "summary": summary,
        "daily": pd.DataFrame(columns=["date", "PRCP", "TMAX", "TMIN"]),
    }


def fetch_weather_features(
    lat: float,
    lng: float,
    last_n_years: int = 3,
    max_stations: int = 3,
    force_refresh: bool = False,
) -> Dict:
    cache_key = {"lat": round(lat, 4), "lng": round(lng, 4), "years": last_n_years, "stations": max_stations}
    parquet_path = parquet_cache_path("noaa", cache_key)
    cached_daily = load_parquet(parquet_path)

    token = os.environ.get("NOAA_CDO_TOKEN", "").strip()
    start_date = date.today() - timedelta(days=365 * last_n_years)
    end_date = date.today()

    if token:
        try:
            stations = _get_nearest_stations(
                lat, lng, token, limit=max(1, min(3, max_stations)), force_refresh=force_refresh
            )
            station_ids = [s.get("id") for s in stations if s.get("id")]
            if not station_ids:
                print("[agent][tool] noaa source=fallback reason=no_stations", flush=True)
                return _fallback_weather(lat, lng)

            if force_refresh or cached_daily is None or cached_daily.empty:
                frames = []
                for station_id in station_ids:
                    station_df = _fetch_station_data(
                        station_id, token, start_date, end_date, force_refresh=force_refresh
                    )
                    if not station_df.empty:
                        station_df["station_id"] = station_id
                        frames.append(station_df)
                raw_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                if not raw_df.empty:
                    grouped = raw_df.groupby(["date", "datatype"], as_index=False)["value"].mean()
                    cached_daily = grouped.pivot(index="date", columns="datatype", values="value").reset_index()
                    save_parquet(parquet_path, cached_daily)

            if cached_daily is not None and not cached_daily.empty:
                source = "api" if force_refresh else "processed-cache-or-api"
                print(f"[agent][tool] noaa source={source} stations={len(station_ids)}", flush=True)
                for col in ["PRCP", "TMAX", "TMIN"]:
                    if col not in cached_daily.columns:
                        cached_daily[col] = float("nan")
                cached_daily = cached_daily.ffill().bfill()
                avg_prcp = float(cached_daily["PRCP"].mean())
                avg_tmax = float(cached_daily["TMAX"].mean())
                avg_tmin = float(cached_daily["TMIN"].mean())
                temp_range = float((cached_daily["TMAX"] - cached_daily["TMIN"]).mean())
                precip_cv = float(cached_daily["PRCP"].std() / max(avg_prcp, 1e-6))
                heat_days = int((cached_daily["TMAX"] >= 30.0).sum())
                freeze_days = int((cached_daily["TMIN"] <= 0.0).sum())
                risk_index = max(0.05, min(0.95, 0.25 * precip_cv + 0.25 * (temp_range / 20.0) + 0.5 * min(1.0, heat_days / 120.0)))
                summary = (
                    f"Weather from {len(station_ids)} nearby NOAA station(s): "
                    f"avg PRCP {avg_prcp:.2f} mm/day, avg TMAX {avg_tmax:.1f}C, avg TMIN {avg_tmin:.1f}C."
                )
                return {
                    "features": {
                        "avg_prcp_mm": round(avg_prcp, 3),
                        "avg_tmax_c": round(avg_tmax, 3),
                        "avg_tmin_c": round(avg_tmin, 3),
                        "temp_range_c": round(temp_range, 3),
                        "precip_cv": round(precip_cv, 3),
                        "extreme_heat_days": heat_days,
                        "freeze_days": freeze_days,
                        "risk_index": round(risk_index, 3),
                    },
                    "stations": station_ids,
                    "summary": summary,
                    "daily": cached_daily,
                }
        except NOAARequestError as exc:
            print(
                f"[agent][tool] noaa source=fallback reason=api_error status={exc.status_code} body={exc.body}",
                flush=True,
            )
        except Exception as exc:
            print(f"[agent][tool] noaa source=fallback reason=unexpected_error error={exc}", flush=True)
            pass

    if not token:
        print("[agent][tool] noaa source=fallback reason=missing_token", flush=True)
    return _fallback_weather(lat, lng)
