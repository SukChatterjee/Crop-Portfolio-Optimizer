from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .cache import cached_json, load_parquet, parquet_cache_path, save_parquet

SDA_URL = "https://sdmdataaccess.sc.egov.usda.gov/tabular/post.rest"


def _seeded_float(seed: str, low: float, high: float) -> float:
    h = int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12], 16)
    ratio = (h % 10_000) / 10_000
    return low + (high - low) * ratio


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=6))
def _run_sda_query(sql: str) -> Dict[str, Any]:
    response = requests.post(
        SDA_URL,
        json={"query": sql, "format": "JSON"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _rows(payload: Dict[str, Any], columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    table = payload.get("Table")
    if not isinstance(table, list) or not table:
        return []
    if isinstance(table[0], dict):
        return table
    if isinstance(table[0], list):
        col_names = payload.get("ColumnNames")
        if not isinstance(col_names, list) or not col_names:
            col_names = columns or [f"col_{i}" for i in range(len(table[0]))]
        out: List[Dict[str, Any]] = []
        for row in table:
            if not isinstance(row, list):
                continue
            out.append({str(col_names[i]): row[i] if i < len(row) else None for i in range(len(col_names))})
        return out
    return []


def _extract_mukeys(payload: Dict[str, Any]) -> List[str]:
    rows = _rows(payload, columns=["mukey"])
    mukeys: List[str] = []
    for r in rows:
        raw = r.get("mukey")
        if raw is None:
            raw = r.get("MUKEY")
        if raw is None:
            raw = r.get("col_0")
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            mukeys.append(text)
    return sorted(set(mukeys))


def _fallback_soil(lat: float, lng: float, soil_type: str) -> Dict[str, Any]:
    st = (soil_type or "").lower()
    avg_ph = 6.7 if "loam" in st else 6.2 if "clay" in st else 6.9 if "sand" in st else 6.5
    avg_om = 3.2 if "loam" in st else 2.8 if "clay" in st else 1.9 if "sand" in st else 2.6
    avg_awc = 0.21 if "loam" in st else 0.24 if "clay" in st else 0.11 if "sand" in st else 0.18
    avg_slope = _seeded_float(f"slope:{lat:.4f}:{lng:.4f}", 1.0, 8.0)
    drainage = "Well drained" if "sand" in st or "loam" in st else "Moderately well drained"
    texture = soil_type or "Unknown"

    summary = (
        f"Soil baseline near ({lat:.4f}, {lng:.4f}) indicates pH {avg_ph:.2f}, "
        f"organic matter {avg_om:.2f}%, drainage '{drainage}', slope {avg_slope:.1f}%, "
        f"and available water capacity proxy {avg_awc:.3f}."
    )
    return {
        "features": {
            "avg_ph": round(avg_ph, 3),
            "avg_organic_matter": round(avg_om, 3),
            "drainage_class": drainage,
            "texture_class": texture,
            "avg_slope_pct": round(float(avg_slope), 3),
            "avg_awc": round(avg_awc, 4),
            "mukeys": [],
        },
        "summary": summary,
    }


def fetch_soil_features(lat: float, lng: float, soil_type: str = "", force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = {"lat": round(lat, 5), "lng": round(lng, 5), "soil_type": soil_type}
    parquet_path = parquet_cache_path("soil", cache_key)
    cached = load_parquet(parquet_path)
    if not force_refresh and cached is not None and not cached.empty:
        print("[agent][tool] soil source=processed-cache", flush=True)
        row = cached.iloc[0].to_dict()
        return {
            "features": {
                "avg_ph": float(row.get("avg_ph", 0.0)),
                "avg_organic_matter": float(row.get("avg_organic_matter", 0.0)),
                "drainage_class": str(row.get("drainage_class", "")),
                "texture_class": str(row.get("texture_class", "")),
                "avg_slope_pct": float(row.get("avg_slope_pct", 0.0)),
                "avg_awc": float(row.get("avg_awc", 0.0)),
                "mukeys": [m for m in str(row.get("mukeys", "")).split(",") if m],
            },
            "summary": str(row.get("summary", "")),
        }

    try:
        point_wkt = f"POINT({lng} {lat})"
        mukey_sql = (
            "SELECT mukey FROM SDA_Get_Mukey_from_intersection_with_WktWgs84("
            f"'{point_wkt}')"
        )
        mukey_payload = cached_json(
            namespace="soil",
            key={"kind": "mukey", "lat": round(lat, 5), "lng": round(lng, 5)},
            fetcher=lambda: _run_sda_query(mukey_sql),
            ttl_hours=48,
            force_refresh=force_refresh,
        )
        mukeys = _extract_mukeys(mukey_payload)
        if not mukeys:
            fallback = _fallback_soil(lat, lng, soil_type)
            print("[agent][tool] soil source=fallback reason=no_mukey", flush=True)
            save_parquet(
                parquet_path,
                pd.DataFrame(
                    [
                        {
                            "avg_ph": fallback["features"]["avg_ph"],
                            "avg_organic_matter": fallback["features"]["avg_organic_matter"],
                            "drainage_class": fallback["features"]["drainage_class"],
                            "texture_class": fallback["features"]["texture_class"],
                            "avg_slope_pct": fallback["features"]["avg_slope_pct"],
                            "avg_awc": fallback["features"]["avg_awc"],
                            "mukeys": "",
                            "summary": fallback["summary"],
                        }
                    ]
                ),
            )
            return fallback

        mukey_in = ",".join(f"'{m}'" for m in mukeys)
        numeric_sql = (
            "SELECT "
            "AVG(ch.ph1to1h2o_r) AS avg_ph, "
            "AVG(ch.om_r) AS avg_organic_matter, "
            "AVG(ch.awc_r) AS avg_awc, "
            "AVG(co.slope_r) AS avg_slope_pct "
            "FROM component co "
            "LEFT JOIN chorizon ch ON ch.cokey = co.cokey "
            f"WHERE co.mukey IN ({mukey_in})"
        )
        class_sql = (
            "SELECT "
            "MAX(co.drainagecl) AS drainage_class "
            "FROM component co "
            f"WHERE co.mukey IN ({mukey_in})"
        )

        numeric_payload = cached_json(
            namespace="soil",
            key={"kind": "numeric", "mukeys": mukeys},
            fetcher=lambda: _run_sda_query(numeric_sql),
            ttl_hours=48,
            force_refresh=force_refresh,
        )
        class_payload = {}
        try:
            class_payload = cached_json(
                namespace="soil",
                key={"kind": "class", "mukeys": mukeys},
                fetcher=lambda: _run_sda_query(class_sql),
                ttl_hours=48,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            print(
                f"[agent][tool] soil class_subquery_failed error={exc}; using numeric+defaults",
                flush=True,
            )
        n_rows = _rows(
            numeric_payload,
            columns=["avg_ph", "avg_organic_matter", "avg_awc", "avg_slope_pct"],
        )
        c_rows = _rows(
            class_payload,
            columns=["drainage_class"],
        )
        n = n_rows[0] if n_rows else {}
        c = c_rows[0] if c_rows else {}

        def _f(v: Any, default: float = 0.0) -> float:
            try:
                return float(v)
            except (TypeError, ValueError):
                return default

        avg_ph = _f(n.get("avg_ph"), 0.0)
        avg_om = _f(n.get("avg_organic_matter"), 0.0)
        avg_awc = _f(n.get("avg_awc"), 0.0)
        avg_slope = _f(n.get("avg_slope_pct"), 0.0)
        drainage = str(c.get("drainage_class") or "Unknown")
        texture = str(soil_type or "Unknown")

        summary = (
            f"NRCS SSURGO soil profile from {len(mukeys)} mapunit(s): "
            f"pH {avg_ph:.2f}, organic matter {avg_om:.2f}%, drainage '{drainage}', "
            f"slope {avg_slope:.1f}%, AWC proxy {avg_awc:.3f}."
        )
        features = {
            "avg_ph": round(avg_ph, 3),
            "avg_organic_matter": round(avg_om, 3),
            "drainage_class": drainage,
            "texture_class": texture,
            "avg_slope_pct": round(avg_slope, 3),
            "avg_awc": round(avg_awc, 4),
            "mukeys": mukeys,
        }
        save_parquet(
            parquet_path,
            pd.DataFrame(
                [
                    {
                        **features,
                        "mukeys": ",".join(mukeys),
                        "summary": summary,
                    }
                ]
            ),
        )
        print(
            f"[agent][tool] soil source=api mukeys={len(mukeys)} class_fields={'yes' if c else 'no'}",
            flush=True,
        )
        return {"features": features, "summary": summary}
    except Exception as exc:
        print(f"[agent][tool] soil source=fallback reason=api_error error={exc}", flush=True)
        fallback = _fallback_soil(lat, lng, soil_type)
        save_parquet(
            parquet_path,
            pd.DataFrame(
                [
                    {
                        "avg_ph": fallback["features"]["avg_ph"],
                        "avg_organic_matter": fallback["features"]["avg_organic_matter"],
                        "drainage_class": fallback["features"]["drainage_class"],
                        "texture_class": fallback["features"]["texture_class"],
                        "avg_slope_pct": fallback["features"]["avg_slope_pct"],
                        "avg_awc": fallback["features"]["avg_awc"],
                        "mukeys": "",
                        "summary": fallback["summary"],
                    }
                ]
            ),
        )
        return fallback
