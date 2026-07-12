"""Forecast rainfall-hazard score — EXPERIMENTAL (Option C1).

See docs/RAINFALL_SCORE_OPTIONS.md. This is the zero-risk placeholder: a
first-order "magnitude" rain score computed by running the existing kinematic
rainfall estimator (core.rainfall_warning) over the FORECAST track instead of
the observed track. It answers "how much rain will this storm's forecast
deliver, and how anomalous is that" as a 0-100 number, computed live per
advisory for any active storm.

IMPORTANT — this does NOT feed DPS or any baked score. It is surfaced only on
the Experimental Features page so it can be watched and, after a storm,
audited against observed rainfall (IMERG / MRMS / USGS high-water marks). The
magnitude subscore is real today; the anomaly (Atlas-14 return period) and
inundation (NWM → HAND flood depth) subscores are placeholders wired to be
filled by SurgeDPS's forecast rain stack (Options B2 / B3) once its dormant
QPF path is activated.

Fail-open: any error returns a null-shaped result the caller can serialize.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)

# 100 ≡ 1000 mm (~40 in) forecast storm-total — Harvey-class (peaked ~1539 mm).
# Same reference the observed/display side uses (dps_engine 1000 mm recal).
RAIN_REF_MM = 1000.0

_KT_TO_MS = 0.514444
_METHOD = "kinematic-v1"


def _null_result(reason: str = "") -> dict:
    return {
        "available": False,
        "rain_score": None,
        "band": None,
        "forecast_total_mm": None,
        "forecast_total_in": None,
        "peak_rain_rate_mmhr": None,
        "subscores": {"magnitude": None, "anomaly": None, "inundation": None},
        "method": _METHOD,
        "note": reason,
    }


def _band(score: float) -> str:
    if score >= 80: return "Extreme"
    if score >= 60: return "High"
    if score >= 40: return "Elevated"
    if score >= 20: return "Moderate"
    return "Low"


def _haversine_nm(lat1, lon1, lat2, lon2) -> float:
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def _lonfat_rain_rate_mmhr(vmax_kt: float) -> float:
    """Peak rain rate from intensity (Lonfat et al. climatology) — the same
    curve core.rainfall_warning uses."""
    if vmax_kt < 50:
        return 5.0 + 0.10 * vmax_kt
    if vmax_kt < 100:
        return 10.0 + 0.15 * (vmax_kt - 50)
    return 17.5 + 0.10 * (vmax_kt - 100)


# Rain-hazard model constants. The hazard is what falls on the WORST-HIT point,
# = rain_rate × residence, where residence is how long the storm's rain shield
# lingers over one spot. Forward speed is the dominant term: a fast Cat 5
# (Michael, Charley) passes every point quickly → low point-total; a staller
# (Harvey, Florence, Imelda) dumps days of rain on one place. Residence
# accumulates only across CONSECUTIVE slow legs (a fast leg moves the storm to
# a new point and resets the run).
_SHIELD_KM = 300.0        # rain-shield coverage width of a point (no r34 in a forecast)
_STALL_KT = 5.0           # a true stall — a point stays under the storm for hours
_MIN_TS_KT = 34.0         # only legs at TS+ intensity produce meaningful rain
_RESIDENCE_CAP_H = 200.0  # stall ceiling (Harvey sat ~5 days)
_TOTAL_CAP_MM = 2000.0    # physical ceiling (Harvey peaked ~1539 mm)


def compute_forecast_rain_score(
    forecast_track: List[dict],
    storm_name: str = "Unknown",
    base_time: Optional[datetime] = None,
) -> dict:
    """0-100 forecast rainfall-hazard score for an active storm's forecast track.

    v1 "magnitude" model: the maximum point-total = rain_rate × residence over
    the forecast, where residence grows across consecutive slow/stalling legs.
    Fast movers score low even at Cat 5. Exposure and anomaly are deferred to
    B2/B3 (SurgeDPS). Always safe to serialize; `available` False when the
    forecast is too short. Does NOT feed DPS.
    """
    try:
        pts = sorted(
            (p for p in (forecast_track or [])
             if p.get("lat") is not None and p.get("lon") is not None),
            key=lambda p: p.get("hour", 0) or 0,
        )
        if len(pts) < 2:
            return _null_result("insufficient forecast track")

        peak_kt = max(float(p.get("max_wind_kt") or p.get("wind_kt") or 0)
                      for p in pts)

        best_mm = 0.0
        stall_h = 0.0
        run_residence = 0.0       # accumulated linger time over consecutive slow legs
        speed_num = speed_den = 0.0
        n_ts = 0
        for i in range(len(pts) - 1):
            h1 = float(pts[i].get("hour", 0) or 0)
            h2 = float(pts[i + 1].get("hour", 0) or 0)
            dt = h2 - h1
            if dt <= 0:
                continue
            w1 = float(pts[i].get("max_wind_kt") or pts[i].get("wind_kt") or 0)
            w2 = float(pts[i + 1].get("max_wind_kt") or pts[i + 1].get("wind_kt") or 0)
            seg_kt = (w1 + w2) / 2.0
            if seg_kt < _MIN_TS_KT:
                run_residence = 0.0  # weak leg breaks the stall run
                continue
            spd = _haversine_nm(pts[i]["lat"], pts[i]["lon"],
                                pts[i + 1]["lat"], pts[i + 1]["lon"]) / dt
            # Base residence = time the 300 km shield covers a point as this leg
            # passes (already rewards ordinary slowness continuously).
            crossing_h = _SHIELD_KM / max(spd * 1.852, 3.0)
            # True stalls add residence on top, accumulating over a stall run —
            # this is what separates Harvey (days over one spot) from a merely
            # slow-moving major (Katrina).
            if spd < _STALL_KT:
                run_residence += dt
            else:
                run_residence = 0.0          # storm moved on → new point, reset
            residence = min(_RESIDENCE_CAP_H, crossing_h + run_residence)
            local_mm = _lonfat_rain_rate_mmhr(seg_kt) * residence
            best_mm = max(best_mm, local_mm)
            if spd < 5.0:
                stall_h += dt
            speed_num += spd * dt
            speed_den += dt
            n_ts += 1

        if n_ts == 0 or speed_den <= 0:
            return _null_result("no tropical-storm-intensity forecast legs")

        total_mm = min(_TOTAL_CAP_MM, best_mm)
        magnitude = round(min(100.0, total_mm / RAIN_REF_MM * 100.0), 1)
        mean_kt = speed_num / speed_den

        return {
            "available": True,
            "rain_score": magnitude,   # v1 headline = magnitude only
            "band": _band(magnitude),
            "forecast_total_mm": round(total_mm, 0),
            "forecast_total_in": round(total_mm / 25.4, 1),
            "peak_rain_rate_mmhr": round(_lonfat_rain_rate_mmhr(peak_kt), 1),
            "mean_forward_speed_kt": round(mean_kt, 1),
            "stall_hours": round(stall_h, 0),
            "subscores": {
                "magnitude": magnitude,
                "anomaly": None,      # B2: Atlas-14 return period (pending SurgeDPS)
                "inundation": None,   # B3: NWM → HAND flood depth (pending SurgeDPS)
            },
            "method": _METHOD,
            "note": "Experimental. Forward-speed-driven kinematic estimate from "
                    "the forecast track; not used in DPS. To be replaced by "
                    "WPC-QPF + Atlas-14 + NWM/HAND from SurgeDPS.",
        }
    except Exception as e:  # noqa: BLE001 — fail open
        logger.warning("[rain_forecast] compute failed: %s", e)
        return _null_result(f"error: {str(e)[:80]}")
