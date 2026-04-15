"""
Unified DPS Engine — the single source of truth for Destructive Power Score.

This module is called from both:
  1. compile_cache.py (batch-compute presets into compiled_bundle.json)
  2. api/routes.py  (on-demand compute for ad-hoc searches, cache to persistent volume)

Both call sites pass identical inputs (storm_id, snapshots, name, year) and
receive identical outputs, guaranteeing that the hero card, accordion, map
markers, and DPS values in the catalog are all derived from one formula.

Design goals:
  - Deterministic: same input → same output bit-for-bit
  - Self-contained: no dependency on file paths, caches, or globals
  - Serializable output: returns a plain dict suitable for json.dumps
  - Fast: ~5ms per storm, safe to call from request handlers

The per-snapshot DPS series (for map marker coloring) is included in the
output so the frontend never recomputes DPS client-side. This eliminates
the "score jumps on load" bug where client-computed scores showed briefly
before being replaced by compiled scores.
"""

from __future__ import annotations

import math
import os
import sys
from typing import Any, Dict, List, Optional

# compile_cache.py lives at the repo root, not inside a package. Under
# gunicorn --preload the repo root isn't always on sys.path at request time
# (only at boot), which breaks the deferred import below with
# ModuleNotFoundError: No module named 'compile_cache'. Add it explicitly.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# Public API — the only function callers should import.
def compute_storm_dps(
    storm_id: str,
    snapshots: List[Dict[str, Any]],
    storm_name: str,
    storm_year: int,
    category_hint: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compute the canonical DPS bundle for a storm.

    Args:
        storm_id:    Storm identifier (AL092022, IBTrACS SID, etc.)
        snapshots:   List of snapshot dicts from preload_bundle / IKE pipeline.
                     Each snapshot should have: timestamp, lat, lon, max_wind_ms,
                     min_pressure_hpa, r34_nm, r64_nm, rmw_nm, forward_speed_knots,
                     ike_total_tj, and optionally r34_quadrants.
        storm_name:  Display name (e.g. "Katrina")
        storm_year:  Year for era adjustments (e.g. 2005)
        category_hint: Lifetime peak category from metadata; if not supplied it's
                       derived from peak wind.

    Returns:
        A dict matching the schema of compiled_bundle.json's per-storm entry:
        keys include `dps`, `peak_dps`, `dps_label`, category, basin, all factor
        breakdowns, `dpi_timeseries` (for map marker coloring), and `landfalls`.
    """
    # Imports are deferred to keep the module load cheap and to avoid a circular
    # dependency with compile_cache.py (which imports from this module).
    from core.cumulative_dpi import compute_cumulative_dpi, categorize_dpi
    from core.rainfall_warning import compute_rainfall_warning
    from compile_cache import (
        detect_landfall_events,
        detect_basin,
        compute_exposure_factor,
        compute_perpendicular_factor,
        apply_basin_dps_adjustment,
        COASTAL_EXPOSURE_WEIGHTS,
        COASTAL_REGIONS,
        US_MAINLAND_REGIONS,
        _wind_kt_to_category,
    )

    # 1. Cumulative DPI — peak + duration + breadth (no 100 cap; see cumulative_dpi.py v6)
    cum_result = compute_cumulative_dpi(
        snapshots, storm_name=storm_name, storm_year=storm_year
    )

    # 2. Landfall detection (needed for most downstream factors)
    landfall_events = detect_landfall_events(snapshots)

    # 3. Basin detection
    basin = detect_basin(snapshots)

    # 4. Population exposure (R3)
    exposure_factor, exposure_region = compute_exposure_factor(landfall_events)

    # 5. Perpendicular surge (R4)
    perp_factor, us_lf_count = compute_perpendicular_factor(
        landfall_events, cum_result.total_coastal_hours
    )

    # 6. Rainfall warning (feeds stall and rain_inland factors)
    rain_result = compute_rainfall_warning(snapshots, storm_name=storm_name)

    # 6b. Ground-truth override: if we have an authoritative observed peak
    # rainfall total (NHC TCR, NWS, NCEI, or MRMS), use it in place of the
    # stall-hour heuristic. Observed data is always more accurate than any
    # kinematic estimator we can build from wind/track alone.
    from core import ground_truth as _gt
    _truth = _gt.get(storm_id) or _gt.get_by_name_year(storm_name, storm_year)
    _observed_rain_in: Optional[float] = None
    if _truth is not None and _truth.peak_rainfall_in is not None:
        _observed_rain_in = _truth.peak_rainfall_in
        _observed_rain_mm = _truth.peak_rainfall_in * 25.4
        # Observed peak-station accumulation is authoritative — replace the
        # heuristic estimate. Keep warning_score consistent: scale relative to
        # a 500mm reference (where 500mm ~ historic-class rainfall).
        rain_result.estimated_total_mm = _observed_rain_mm
        _rain_score_from_obs = min(100.0, (_observed_rain_mm / 500.0) * 100.0)
        rain_result.warning_score = max(rain_result.warning_score, _rain_score_from_obs)
        if rain_result.warning_score >= 80:
            rain_result.warning_level = "HISTORIC"
        elif rain_result.warning_score >= 60:
            rain_result.warning_level = "EXTREME"
        elif rain_result.warning_score >= 40:
            rain_result.warning_level = "HIGH"
        elif rain_result.warning_score >= 20:
            rain_result.warning_level = "ELEVATED"
        rain_result.is_anomalous = rain_result.warning_score >= 25

    # 7. Rainfall stall bonus (R7 + F2 + F11) — scaled by economic weight of stall location
    STALL_THRESHOLD_HOURS = 4
    STALL_BONUS_PER_HOUR = 0.01
    SLOW_BONUS_PER_HOUR = 0.005
    STALL_BONUS_CAP = 0.05
    effective_stall_hours = (
        rain_result.total_stall_hours + rain_result.total_slow_hours * 0.5
    )
    if effective_stall_hours > STALL_THRESHOLD_HOURS and cum_result.peak_dpi > 0:
        raw_stall = min(
            rain_result.total_stall_hours * STALL_BONUS_PER_HOUR
            + rain_result.total_slow_hours * SLOW_BONUS_PER_HOUR,
            STALL_BONUS_CAP,
        )
        slowest_snap = min(
            snapshots, key=lambda s: (s.get("forward_speed_knots", 99) or 99)
        )
        stall_lat = slowest_snap.get("lat", 0)
        stall_lon = slowest_snap.get("lon", 0)
        stall_region = "Coast"
        for lat_min, lat_max, lon_min, lon_max, rname in COASTAL_REGIONS:
            if lat_min <= stall_lat <= lat_max and lon_min <= stall_lon <= lon_max:
                stall_region = rname
                break
        stall_econ = COASTAL_EXPOSURE_WEIGHTS.get(stall_region, 0.20)
        stall_bonus = raw_stall * max(stall_econ, 0.10)
    else:
        stall_bonus = 0.0

    # 8. Rainfall inland factor (R12 + F2)
    RAIN_WARN_THRESHOLD = 30
    RAIN_MM_THRESHOLD = 250
    RAIN_INLAND_CAP = 0.04
    if (
        rain_result.warning_score > RAIN_WARN_THRESHOLD
        and rain_result.estimated_total_mm > RAIN_MM_THRESHOLD
        and cum_result.peak_dpi > 0
    ):
        raw_rain_inland = min(rain_result.warning_score / 100.0 * 0.08, RAIN_INLAND_CAP)
        rain_econ = COASTAL_EXPOSURE_WEIGHTS.get(exposure_region, 0.20)
        rain_inland_factor = raw_rain_inland * max(rain_econ, 0.15)
    else:
        rain_inland_factor = 0.0

    # 9. Inland flood penetration (F5)
    INLAND_PEN_CAP = 0.04
    INLAND_PEN_PER_SNAP = 0.008
    INLAND_MIN_WIND_MS = 18.0
    inland_pen_factor = 0.0
    if landfall_events and cum_result.peak_dpi > 0:
        us_lf_events = [
            e for e in landfall_events if e.get("region", "") in US_MAINLAND_REGIONS
        ]
        if us_lf_events:
            first_lf_idx = min(
                e.get("snapshot_idx", len(snapshots)) for e in us_lf_events
            )
        else:
            first_lf_idx = min(
                e.get("snapshot_idx", len(snapshots)) for e in landfall_events
            )
        inland_ts_snaps = 0
        for snap in snapshots[first_lf_idx:]:
            wind = snap.get("max_wind_ms", 0) or 0
            lat, lon = snap.get("lat", 0), snap.get("lon", 0)
            if not (25.0 <= lat <= 48.0 and -100.0 <= lon <= -66.0):
                continue
            near_coast = False
            for lat_min, lat_max, lon_min, lon_max, _ in COASTAL_REGIONS:
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    near_coast = True
                    break
            if wind >= INLAND_MIN_WIND_MS and not near_coast:
                inland_ts_snaps += 1
        if inland_ts_snaps >= 2:
            rain_scale = min(
                1.0,
                max(
                    rain_result.warning_score,
                    rain_result.estimated_total_mm / 10.0,
                )
                / 50.0,
            )
            inland_pen_factor = min(
                INLAND_PEN_CAP, inland_ts_snaps * INLAND_PEN_PER_SNAP * rain_scale
            )

    # 10. Combine regional boost factors
    combined_boost = (
        exposure_factor
        + perp_factor
        + stall_bonus
        + rain_inland_factor
        + inland_pen_factor
    )
    if combined_boost > 0 and cum_result.peak_dpi > 0:
        current_multiplier = cum_result.cum_dpi / cum_result.peak_dpi
        boosted = cum_result.peak_dpi * (current_multiplier + combined_boost)
    else:
        boosted = cum_result.cum_dpi

    # 11. Basin-specific adjustment + sqrt compression
    adjusted_dps, basin_name, adjustment_notes = apply_basin_dps_adjustment(
        boosted, basin, snapshots,
        duration_factor=getattr(cum_result, "duration_factor", None),
        breadth_factor=getattr(cum_result, "breadth_factor", None),
    )

    # 12. Peak snapshot stats for display
    peak_wind = max((s.get("max_wind_ms", 0) or 0) for s in snapshots) if snapshots else 0
    peak_ike = (
        max((s.get("ike_total_tj", 0) or 0) for s in snapshots) if snapshots else 0
    )
    min_pressure = (
        min((s.get("min_pressure_hpa", 1013) or 1013) for s in snapshots)
        if snapshots
        else 1013
    )

    # 13. Landfall-based category (F4 — NHC convention)
    lifetime_cat = category_hint if category_hint is not None else _wind_kt_to_category(
        peak_wind / 0.514444 if peak_wind else 0
    )
    if landfall_events:
        lf_peak_wind_ms = max(e.get("max_wind_ms", 0) for e in landfall_events)
        lf_peak_kt = lf_peak_wind_ms / 0.514444
        landfall_cat = _wind_kt_to_category(lf_peak_kt)
    else:
        landfall_cat = lifetime_cat

    basin_adjusted_category = categorize_dpi(adjusted_dps)

    # 14. Scale the per-snapshot DPI series to the final adjusted peak, so the
    # map markers use the same canonical scaling as the hero card. This replaces
    # the frontend's client-side scaling dance — now done once, server-side.
    raw_peak = cum_result.peak_dpi or 1.0
    scale = adjusted_dps / raw_peak if raw_peak > 0 else 0.0
    dpi_timeseries = [
        {"t": s.get("timestamp", ""), "dpi": round(min(99.0, s["dpi"] * scale), 1)}
        for s in cum_result.dpi_timeseries
        if s["dpi"] > 0
    ]

    # 15. Build output dict matching compiled_bundle.json schema
    return {
        "name": storm_name,
        "year": storm_year,
        "category": landfall_cat,
        "category_lifetime": lifetime_cat,
        # Core DPS — the single authoritative score used by hero/accordion/map
        "dps": adjusted_dps,
        "dps_label": basin_adjusted_category,
        "peak_dps": cum_result.peak_dpi,
        # Basin info
        "basin": basin,
        "basin_name": basin_name,
        "dps_original": cum_result.cum_dpi,
        # Factor breakdown (for debugging and tooltip display)
        "exposure_factor": exposure_factor,
        "exposure_region": exposure_region,
        "perp_factor": perp_factor,
        "us_landfall_count": us_lf_count,
        "stall_bonus": stall_bonus,
        "stall_hours": rain_result.total_stall_hours,
        "rain_inland_factor": rain_inland_factor,
        "inland_pen_factor": inland_pen_factor,
        "duration_factor": cum_result.duration_factor,
        "breadth_factor": cum_result.breadth_factor,
        "coastal_hours": cum_result.total_coastal_hours,
        "track_hours": cum_result.total_track_hours,
        "peak_ike_tj": cum_result.peak_ike_tj,
        "adjustment_notes": adjustment_notes,
        # Rainfall warning (red bar)
        "rainfall_warning": rain_result.warning_score,
        "rainfall_level": rain_result.warning_level,
        "rainfall_text": rain_result.warning_text,
        "rainfall_anomalous": rain_result.is_anomalous,
        "rainfall_stall_hours": rain_result.total_stall_hours,
        "rainfall_est_mm": rain_result.estimated_total_mm,
        # Quick peak stats
        "peak_wind_ms": round(peak_wind, 1),
        "peak_wind_kt": round(peak_wind / 0.514444, 0) if peak_wind else 0,
        "min_pressure_hpa": round(min_pressure, 0) if min_pressure else 1013,
        "peak_ike": round(peak_ike, 1),
        "snapshot_count": len(snapshots),
        # Landfall events
        "landfalls": landfall_events,
        # Per-snapshot DPS series — scaled to match adjusted peak so map markers
        # use the canonical cumulative values directly (no client-side rescaling).
        "dpi_timeseries": dpi_timeseries,
        # Ground-truth reference values (NHC TCR / NCEI / OpenFEMA). Present only
        # for storms we have curated observations for; frontend should treat as
        # optional enrichment for the hero card and accordion.
        "ground_truth": _truth.to_dict() if _truth is not None else None,
        "observed_rainfall_in": _observed_rain_in,
    }


__all__ = ["compute_storm_dps"]
