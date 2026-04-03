"""
Rainfall Anomaly Warning — Standalone "Red Bar" metric.

This module is deliberately SEPARATE from the DPI formula. The DPI captures
destructive potential from wind, surge, and economic vulnerability at landfall.
Anomalous rainfall events (Harvey 2017, Helene 2024, Florence 2018) cause
catastrophic damage through a mechanism the DPI was not designed to measure:
prolonged freshwater flooding driven by stalling or slow-moving storms dumping
extraordinary rain totals over river basins for days after landfall.

Design philosophy:
  The DPI is "the bread and butter — risk at landfall." People evacuate for
  wind and surge. They are LESS likely to evacuate for rain alone, so rainfall
  anomaly storms represent a distinct hazard that deserves a separate, prominent
  warning. This module provides that warning in a "red bar" format: a 0-100
  severity score with a plain-language alert that can be displayed alongside
  (but independently of) the DPI score.

Detection signals for rainfall anomaly storms:
  1. STALL DURATION: Forward speed drops below 5 kt for extended periods while
     near or over land. Harvey stalled at 2 kt for 5+ days over Houston.
  2. SLOW TRANSLATION: Forward speed consistently below 8 kt during landfall
     approach and post-landfall. Helene moved faster but dumped rain over
     Appalachian terrain that amplified runoff catastrophically.
  3. RAIN SHIELD SIZE: Large r34 radius means a wider rain swath. Sandy's
     900km wind field, Florence's broad circulation.
  4. MOISTURE LOADING: Storm intensity × size × slow speed = extreme
     precipitable water delivery. High IKE + low forward speed is a signal.
  5. TERRAIN INTERACTION: Storms moving inland over mountainous terrain
     (Appalachians for Helene) get orographic rainfall enhancement that
     multiplies the accumulation beyond what flat-terrain models predict.
  6. RIVER BASIN FACTOR: The storm stalls over or near a major river basin
     (Buffalo Bayou for Harvey, Cape Fear for Florence) causing compound
     flooding as runoff accumulates downstream.

Scoring:
  rainfall_warning_score = 0-100 composite from:
    - stall_factor (0-40):  Duration of near-stall conditions over/near land
    - moisture_factor (0-30): Rain delivery rate from intensity × size × speed
    - terrain_factor (0-15): Orographic enhancement from inland terrain
    - basin_factor (0-15): River basin compound flooding potential

  Alert levels:
    0-20:  Normal — Typical hurricane rainfall, no special warning
    20-40: Elevated — Above-average rainfall expected, flash flood risk
    40-60: High — Significant inland flooding likely, river flood warnings
    60-80: Extreme — Catastrophic rainfall event, life-threatening flooding
    80-100: Historic — Generational rainfall disaster (Harvey-class)

Reference events:
  Harvey (2017):  Score ≈ 90+ (5-day stall, 1500mm+ rain, Houston basin)
  Helene (2024):  Score ≈ 65+ (fast-ish but Appalachian terrain amplification)
  Florence (2018): Score ≈ 70+ (slow-moving, 3-day stall, NC river basins)
  Imelda (2019):  Score ≈ 55+ (stalled over SE TX, short duration but extreme rates)
"""

import math
import logging
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

KT_TO_MS = 0.514444
NM_TO_M = 1852.0

# ============================================================================
#  THRESHOLDS AND REFERENCE VALUES
# ============================================================================

# Stall detection thresholds
STALL_SPEED_KT = 5.0          # Below this = "stalled"
SLOW_SPEED_KT = 8.0           # Below this = "slow-moving" (still dumps rain)
STALL_REF_HOURS = 48.0        # Harvey stalled ~120h; 48h is a strong stall event

# Moisture delivery
MOISTURE_REF_TJ = 100.0       # IKE reference for moisture proxy
MOISTURE_REF_R34_NM = 150.0   # Reference r34 for rain shield size

# Terrain enhancement zones — rough bounding boxes for elevated terrain
# that causes orographic rainfall amplification when storms track inland
TERRAIN_ZONES = [
    # (lat_min, lat_max, lon_min, lon_max, enhancement, label)
    (34.0, 37.5, -84.0, -79.0, 1.8, "Southern Appalachians"),    # Helene's killer zone
    (37.0, 41.0, -82.0, -76.0, 1.5, "Central Appalachians"),     # WV/VA mountains
    (33.0, 36.0, -86.0, -83.0, 1.3, "Alabama/GA highlands"),     # Northern GA/AL
    (17.5, 18.5, -67.0, -65.5, 1.6, "PR Cordillera Central"),    # Maria's terrain trap
    (33.0, 36.5, -80.0, -77.0, 1.2, "Carolina Piedmont"),        # Inland NC/SC
]

# River basin compound flooding zones — areas where stalling storms create
# downstream compound flooding through river basin drainage
RIVER_BASIN_ZONES = [
    # (lat_min, lat_max, lon_min, lon_max, basin_factor, label)
    (27.5, 30.5, -97.5, -93.5, 1.8, "Houston / Buffalo Bayou-San Jacinto"),  # Harvey
    (33.5, 35.5, -80.0, -77.0, 1.5, "NC Cape Fear / Neuse / Tar"),           # Florence
    (29.5, 31.5, -92.0, -89.0, 1.4, "Louisiana Mississippi Basin"),           # Many storms
    (37.0, 40.0, -77.5, -74.0, 1.3, "Mid-Atlantic Potomac / Delaware"),      # Sandy-type
    (25.5, 27.5, -81.5, -80.0, 1.2, "SW FL Peace / Caloosahatchee"),         # Ian-type
    (34.0, 37.0, -85.5, -81.0, 1.6, "TN/NC French Broad / Pigeon"),          # Helene inland
    (30.5, 34.5, -85.0, -82.0, 1.3, "GA/AL Chattahoochee / Flint"),           # Helene mid-track
]

# Land bounding boxes (near-land check for stall relevance)
LAND_BOXES = [
    (24.5, 49.0, -98.0, -66.0, "CONUS"),
    (17.0, 19.5, -68.0, -64.0, "Puerto Rico / USVI"),
    (21.0, 27.5, -80.0, -72.0, "Bahamas"),
]


@dataclass
class RainfallWarningResult:
    """
    Standalone rainfall anomaly warning result.

    Displayed as a "red bar" alongside the DPI — not integrated into the DPI
    formula. Provides a separate signal for evacuation/shelter decisions when
    the primary hazard is freshwater flooding rather than wind/surge.

    Attributes:
        warning_score: 0-100 composite rainfall anomaly severity
        warning_level: Plain-language alert level
        warning_text: Human-readable description of the rainfall threat
        stall_factor: Contribution from stalling/slow-moving behavior (0-40)
        moisture_factor: Contribution from rain delivery potential (0-30)
        terrain_factor: Contribution from orographic enhancement (0-15)
        basin_factor: Contribution from river basin compound flooding (0-15)
        total_stall_hours: Hours at stall speed near/over land
        total_slow_hours: Hours at slow speed near/over land
        avg_stall_speed_kt: Average forward speed during stall periods
        peak_rain_rate_mmhr: Estimated peak rainfall rate
        estimated_total_mm: Rough total rainfall accumulation estimate
        affected_terrain: List of terrain zones the track crosses
        affected_basins: List of river basins at risk
        is_anomalous: True if this storm triggers a red bar warning (score >= 25)
    """
    warning_score: float
    warning_level: str
    warning_text: str
    stall_factor: float
    moisture_factor: float
    terrain_factor: float
    basin_factor: float
    total_stall_hours: float
    total_slow_hours: float
    avg_stall_speed_kt: float
    peak_rain_rate_mmhr: float
    estimated_total_mm: float
    affected_terrain: List[str]
    affected_basins: List[str]
    is_anomalous: bool


# ============================================================================
#  WARNING LEVEL CLASSIFICATION
# ============================================================================

def classify_warning(score: float) -> Tuple[str, str]:
    """
    Map rainfall warning score to alert level and descriptive text.

    Returns:
        (level, description) tuple
    """
    if score < 20:
        return ("Normal", "Typical hurricane rainfall. Standard flash flood precautions.")
    elif score < 40:
        return ("Elevated",
                "Above-average rainfall expected. Flash flooding likely in "
                "low-lying and urban areas. Monitor river gauges.")
    elif score < 60:
        return ("High",
                "Significant inland flooding likely. Expect dangerous river "
                "flooding 1-3 days after landfall. Life-threatening flash floods "
                "in terrain-amplified zones.")
    elif score < 80:
        return ("Extreme",
                "Catastrophic rainfall event. Major river flooding, widespread "
                "freshwater inundation expected. This storm's rainfall threat "
                "exceeds its wind/surge threat. Evacuate flood-prone areas.")
    else:
        return ("Historic",
                "Generational rainfall disaster. Multi-day flooding comparable "
                "to Harvey (2017). Extreme danger in all low-lying areas. "
                "Catastrophic river crests expected. Evacuate immediately.")


# ============================================================================
#  TERRAIN AND BASIN LOOKUP
# ============================================================================

def _check_terrain(lat: float, lon: float) -> List[Tuple[float, str]]:
    """Check if a point falls in any orographic enhancement zone."""
    matches = []
    for lat_min, lat_max, lon_min, lon_max, enhancement, label in TERRAIN_ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            matches.append((enhancement, label))
    return matches


def _check_basins(lat: float, lon: float) -> List[Tuple[float, str]]:
    """Check if a point falls in any river basin compound flooding zone."""
    matches = []
    for lat_min, lat_max, lon_min, lon_max, factor, label in RIVER_BASIN_ZONES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            matches.append((factor, label))
    return matches


def _is_near_land(lat: float, lon: float) -> bool:
    """Quick check if point is near or over land (US/Caribbean)."""
    for lat_min, lat_max, lon_min, lon_max, _ in LAND_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return True
    return False


# ============================================================================
#  MAIN COMPUTATION
# ============================================================================

def compute_rainfall_warning(
    snapshots: List[Dict],
    storm_name: str = "Unknown",
) -> RainfallWarningResult:
    """
    Compute the rainfall anomaly warning score from multi-snapshot track data.

    This function analyzes the entire storm track to detect rainfall anomaly
    signals: stalling, slow translation, terrain interaction, and river basin
    exposure. It produces a standalone 0-100 "red bar" warning that is
    displayed independently of the DPI.

    Args:
        snapshots: List of snapshot dicts from preload_bundle.json
                   Expected keys: lat, lon, timestamp, forward_speed_knots,
                   max_wind_ms, ike_total_tj, r34_nm, min_pressure_hpa
        storm_name: Storm name for logging/display

    Returns:
        RainfallWarningResult with warning score and breakdown
    """
    if not snapshots:
        return _empty_result(storm_name)

    # ── Parse timestamps for Δt computation ──
    from core.cumulative_dpi import _parse_timestamp
    timestamps = []
    for snap in snapshots:
        try:
            timestamps.append(_parse_timestamp(snap.get("timestamp", "")))
        except (ValueError, TypeError):
            timestamps.append(None)

    # ── FACTOR 1: STALL / SLOW TRANSLATION (0-40 pts) ──
    # Scan for periods where forward speed drops below thresholds while
    # the storm is near or over land. This is the primary Harvey signal.
    stall_hours = 0.0
    slow_hours = 0.0
    stall_speeds = []  # for computing average
    near_land_slow_snapshots = []

    for i, snap in enumerate(snapshots):
        lat, lon = snap["lat"], snap["lon"]
        fwd_kt = snap.get("forward_speed_knots")

        # Treat 0 or None as missing data, not stall.
        # Real stall events report actual speeds (1-5 kt). A forward speed
        # of exactly 0 in advisory data means "not reported" — Michael (2018)
        # was a fast-moving storm but its bundle has 0 for all snapshots.
        if fwd_kt is None or fwd_kt == 0:
            continue  # Skip — can't determine motion from this snapshot

        if not _is_near_land(lat, lon):
            continue

        # Compute Δt for this snapshot
        dt_hours = _delta_hours(timestamps, i, len(snapshots))

        if fwd_kt <= STALL_SPEED_KT:
            stall_hours += dt_hours
            stall_speeds.append(fwd_kt)
            near_land_slow_snapshots.append(snap)
        elif fwd_kt <= SLOW_SPEED_KT:
            slow_hours += dt_hours
            near_land_slow_snapshots.append(snap)

    # Stall factor: hours of stalling normalized to reference, plus partial
    # credit for slow-moving periods
    stall_norm = min(1.0, stall_hours / STALL_REF_HOURS)
    slow_norm = min(1.0, slow_hours / (STALL_REF_HOURS * 1.5))
    stall_factor = 40.0 * (0.70 * stall_norm + 0.30 * slow_norm)

    avg_stall_speed = (
        sum(stall_speeds) / len(stall_speeds) if stall_speeds else 99.0
    )

    # ── FACTOR 2: MOISTURE DELIVERY POTENTIAL (0-30 pts) ──
    # Combines storm intensity, size (rain shield), and slow speed to
    # estimate how much moisture the storm can deliver to a single area.
    # Large, intense, slow-moving storms are the worst rain producers.

    # Peak intensity along track (for moisture loading)
    peak_vmax = max((s.get("max_wind_ms", 0) or 0) for s in snapshots)
    peak_ike = max((s.get("ike_total_tj", 0) or 0) for s in snapshots)
    max_r34 = max((s.get("r34_nm", 0) or 0) for s in snapshots)

    # Rain rate estimate (from storm_surge.py Lonfat climatology)
    vmax_kt = peak_vmax / KT_TO_MS if peak_vmax > 0 else 0
    if vmax_kt < 50:
        peak_rain_rate = 5.0 + 0.10 * vmax_kt
    elif vmax_kt < 100:
        peak_rain_rate = 10.0 + 0.15 * (vmax_kt - 50)
    else:
        peak_rain_rate = 17.5 + 0.10 * (vmax_kt - 100)

    # Moisture delivery = intensity × size × duration-near-land
    # Normalize each component
    ike_norm = min(1.0, peak_ike / MOISTURE_REF_TJ)
    size_norm = min(1.0, max_r34 / MOISTURE_REF_R34_NM)
    # Effective rain hours = stall + slow + some credit for normal-speed land hours
    total_land_hours = stall_hours + slow_hours
    for i, snap in enumerate(snapshots):
        fwd_kt_s = snap.get("forward_speed_knots")
        if fwd_kt_s is None or fwd_kt_s == 0:
            continue  # Missing data
        if (_is_near_land(snap["lat"], snap["lon"])
                and fwd_kt_s > SLOW_SPEED_KT):
            total_land_hours += _delta_hours(timestamps, i, len(snapshots)) * 0.3

    rain_duration_norm = min(1.0, total_land_hours / 72.0)  # 72h = extreme

    # Composite moisture score
    moisture_raw = (
        0.35 * ike_norm +
        0.30 * size_norm +
        0.35 * rain_duration_norm
    )
    moisture_factor = 30.0 * min(1.0, moisture_raw)

    # Rough total rainfall estimate (mm) for display
    # TCR ≈ rain_rate × residence_hours × terrain_enhancement
    # Always compute rain-shield crossing time as a floor estimate
    effective_rain_hours = stall_hours + slow_hours * 0.6
    # Even storms with some slow hours need a floor based on rain-shield size.
    # Compute rain-shield crossing time and total land-track hours.
    land_snaps = [s for s in snapshots
                  if _is_near_land(s["lat"], s["lon"])
                  and (s.get("forward_speed_knots") or 0) > 0]
    if effective_rain_hours < 12.0:
        # Use average forward speed of all land snapshots for crossing estimate
        if land_snaps:
            avg_fwd = sum(
                (s.get("forward_speed_knots", 10) or 10) for s in land_snaps
            ) / len(land_snaps)
        else:
            avg_fwd = 15.0  # Assume normal crossing speed

        if avg_fwd == 0:
            effective_rain_hours = 72.0
        else:
            rain_shield_km = max_r34 * NM_TO_M * 1.5 * 2 / 1000.0 if max_r34 > 0 else 300.0
            rain_shield_km = min(rain_shield_km, 600.0)
            crossing_hours = min(24.0, max(3.0, rain_shield_km / (avg_fwd * 1.852)))
            # For large storms crossing long land distances, add credit for
            # total land-track hours (Helene 2024 maintained TS-force winds
            # 500+ km inland, continuously dumping rain)
            total_land_track_h = sum(
                _delta_hours(timestamps, i, len(snapshots))
                for i, s in enumerate(snapshots)
                if _is_near_land(s["lat"], s["lon"])
                and (s.get("forward_speed_knots") or 0) > 0
            )
            land_bonus = min(12.0, total_land_track_h * 0.3)
            shield_estimate = max(crossing_hours, crossing_hours * 0.5 + land_bonus)
            # Take the larger of stall-based or shield-based estimate
            effective_rain_hours = max(effective_rain_hours, shield_estimate)

    estimated_total_mm = peak_rain_rate * effective_rain_hours

    # ── FACTOR 3: TERRAIN ENHANCEMENT (0-15 pts) ──
    # Check if the track crosses elevated terrain that amplifies rainfall
    # through orographic lifting. Helene (2024) is the canonical example:
    # moisture-laden remnants pushed into the southern Appalachians produced
    # catastrophic rainfall far from the coast.
    terrain_hits = set()
    max_terrain_enhancement = 1.0
    for snap in snapshots:
        matches = _check_terrain(snap["lat"], snap["lon"])
        for enhancement, label in matches:
            terrain_hits.add(label)
            max_terrain_enhancement = max(max_terrain_enhancement, enhancement)

    # Terrain factor: enhancement beyond 1.0 × number of zones crossed
    if max_terrain_enhancement > 1.0:
        terrain_excess = max_terrain_enhancement - 1.0  # 0-0.8 range
        zone_breadth = min(1.0, len(terrain_hits) / 2.0)  # Multiple zones = worse
        # Slow movement amplifies terrain rainfall, but fast-moving Cat 3+
        # storms (Helene 2024) can also produce catastrophic terrain flooding
        # because their sheer moisture volume overwhelms drainage capacity.
        # The moisture delivery factor (IKE × size) compensates for speed.
        combined_slow = stall_hours + slow_hours
        if combined_slow > 12.0:
            slow_present = 1.0     # Clear stall/slow pattern
        elif combined_slow > 6.0:
            slow_present = 0.6     # Moderate slow behavior
        elif combined_slow > 0:
            slow_present = 0.35    # Brief slow period
        else:
            # [R6] Fast-moving but intense storms: grant terrain credit scaled
            # by moisture delivery. Helene was fast (25kt) but Cat 4 with
            # massive moisture feed → catastrophic Appalachian flooding.
            # IKE proxy: high IKE = massive moisture reservoir even at speed.
            intensity_compensator = min(0.6, ike_norm * 0.5 + size_norm * 0.3)
            slow_present = max(0.15, intensity_compensator)
        terrain_factor = 15.0 * min(1.0, terrain_excess * zone_breadth * slow_present / 0.5)
    else:
        terrain_factor = 0.0

    # Apply terrain enhancement to rainfall estimate
    estimated_total_mm *= max_terrain_enhancement

    # ── FACTOR 4: RIVER BASIN COMPOUND FLOODING (0-15 pts) ──
    # Check if stall/slow periods occur over major river basins where
    # upstream rainfall drains into populated downstream areas.
    basin_hits = set()
    max_basin_factor = 1.0
    for snap in near_land_slow_snapshots:
        matches = _check_basins(snap["lat"], snap["lon"])
        for factor, label in matches:
            basin_hits.add(label)
            max_basin_factor = max(max_basin_factor, factor)

    # Also check all over-land snapshots (even at normal speed, a large rain
    # shield can dump on a basin)
    for snap in snapshots:
        if _is_near_land(snap["lat"], snap["lon"]):
            matches = _check_basins(snap["lat"], snap["lon"])
            for factor, label in matches:
                basin_hits.add(label)
                # Only credit basin at 50% if storm isn't slow/stalling there
                max_basin_factor = max(max_basin_factor, 1.0 + (factor - 1.0) * 0.5)

    if max_basin_factor > 1.0:
        basin_excess = max_basin_factor - 1.0
        # Basin flooding needs duration to accumulate — but very large, intense
        # storms can overwhelm basins even at normal speed (Helene 2024: Cat 4,
        # massive moisture feed → catastrophic French Broad / Pigeon flooding).
        slow_duration = min(1.0, (stall_hours + slow_hours) / 24.0)
        # Intensity proxy: large IKE + large wind field = massive moisture dump
        intensity_proxy = min(0.6, ike_norm * 0.4 + size_norm * 0.3 + rain_duration_norm * 0.3)
        duration_present = max(slow_duration, intensity_proxy)
        basin_factor = 15.0 * min(1.0, basin_excess * duration_present / 0.6)
    else:
        basin_factor = 0.0

    # ── COMPOSITE SCORE ──
    warning_score = stall_factor + moisture_factor + terrain_factor + basin_factor
    warning_score = min(100.0, max(0.0, warning_score))

    # Classify
    warning_level, warning_text = classify_warning(warning_score)
    is_anomalous = warning_score >= 25.0

    # Build descriptive text with specifics
    if is_anomalous:
        specifics = []
        if stall_hours > 6:
            specifics.append(
                f"Storm stalls at avg {avg_stall_speed:.0f} kt for "
                f"{stall_hours:.0f}+ hours near land"
            )
        elif slow_hours > 12:
            specifics.append(
                f"Slow-moving ({slow_hours:.0f}h below {SLOW_SPEED_KT:.0f} kt)"
            )
        if terrain_hits:
            specifics.append(f"Terrain amplification: {', '.join(sorted(terrain_hits))}")
        if basin_hits:
            specifics.append(f"River basin risk: {', '.join(sorted(basin_hits))}")
        if estimated_total_mm > 300:
            specifics.append(f"Est. {estimated_total_mm:.0f}mm ({estimated_total_mm/25.4:.0f}in) rainfall")

        if specifics:
            warning_text += " " + "; ".join(specifics) + "."

    return RainfallWarningResult(
        warning_score=round(warning_score, 1),
        warning_level=warning_level,
        warning_text=warning_text,
        stall_factor=round(stall_factor, 1),
        moisture_factor=round(moisture_factor, 1),
        terrain_factor=round(terrain_factor, 1),
        basin_factor=round(basin_factor, 1),
        total_stall_hours=round(stall_hours, 1),
        total_slow_hours=round(slow_hours, 1),
        avg_stall_speed_kt=round(avg_stall_speed, 1),
        peak_rain_rate_mmhr=round(peak_rain_rate, 1),
        estimated_total_mm=round(estimated_total_mm, 0),
        affected_terrain=sorted(terrain_hits),
        affected_basins=sorted(basin_hits),
        is_anomalous=is_anomalous,
    )


def _delta_hours(timestamps, i: int, total: int) -> float:
    """Compute time delta in hours for snapshot i."""
    if i < total - 1 and timestamps[i] and timestamps[i + 1]:
        dt = (timestamps[i + 1] - timestamps[i]).total_seconds() / 3600.0
    elif i > 0 and timestamps[i] and timestamps[i - 1]:
        dt = (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600.0
    else:
        dt = 6.0  # Default 6h advisory interval
    return min(dt, 12.0)  # Cap to avoid data gaps inflating


def _empty_result(storm_name: str) -> RainfallWarningResult:
    """Return a zero-score result for empty/invalid input."""
    return RainfallWarningResult(
        warning_score=0.0,
        warning_level="Normal",
        warning_text="No data available for rainfall analysis.",
        stall_factor=0.0,
        moisture_factor=0.0,
        terrain_factor=0.0,
        basin_factor=0.0,
        total_stall_hours=0.0,
        total_slow_hours=0.0,
        avg_stall_speed_kt=0.0,
        peak_rain_rate_mmhr=0.0,
        estimated_total_mm=0.0,
        affected_terrain=[],
        affected_basins=[],
        is_anomalous=False,
    )


# ============================================================================
#  BATCH COMPUTATION FROM PRELOAD BUNDLE
# ============================================================================

def compute_all_from_bundle(bundle_path: Optional[str] = None) -> List[Dict]:
    """
    Load preload_bundle.json and compute rainfall warnings for all storms.

    Returns list of dicts with storm name and warning result, sorted by
    warning_score descending.
    """
    import json
    import os

    if bundle_path is None:
        bundle_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "preload_bundle.json"
        )

    with open(bundle_path) as f:
        data = json.load(f)

    # Storm ID to display name mapping
    STORM_NAMES = {
        "AL092017": "Harvey (2017)",
        "AL112017": "Irma (2017)",
        "AL152017": "Maria (2017)",
        "AL142018": "Michael (2018)",
        "AL062018": "Florence (2018)",
        "AL052019": "Dorian (2019)",
        "AL092020": "Isaias (2020)",
        "AL182012": "Sandy (2012)",
        "AL122005": "Katrina (2005)",
        "AL092005": "Ike (2008)",
        "AL092022": "Ian (2022)",
        "AL142024": "Milton (2024)",
        "AL012024": "Beryl (2024)",
        "AL092024": "Helene (2024)",
        "AL102023": "Idalia (2023)",
    }

    results = []
    storms = data.get("storms", {})
    # storms is a dict of {storm_id: [list_of_snapshots]}
    for storm_id, snapshots in storms.items():
        storm_name = STORM_NAMES.get(storm_id, storm_id)

        warning = compute_rainfall_warning(snapshots, storm_name=storm_name)
        results.append({
            "storm_id": storm_id,
            "storm_name": storm_name,
            "warning": warning,
        })

    results.sort(key=lambda x: x["warning"].warning_score, reverse=True)
    return results
