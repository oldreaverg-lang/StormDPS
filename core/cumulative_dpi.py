"""
Cumulative DPI — Multi-snapshot Destructive Potential Index.

The single-snapshot DPI answers "how dangerous is this storm RIGHT NOW?"
The cumulative DPI answers "how much total destruction will this storm cause?"

Design rationale:
  A hurricane's total impact depends on:
    1. Peak intensity at landfall (single-snapshot DPI captures this)
    2. Duration of exposure (how long the wind/rain/surge affect populated areas)
    3. Geographic breadth (how many communities are struck sequentially)

  Harvey (2017) illustrates the gap: single-snapshot DPI ≈ 70 (moderate Cat 4),
  but it stalled over Houston for 5 days producing $160B in damage — #2 all-time.
  Sandy (2012): single-snapshot DPI ≈ 54 (modest Cat 1), but its 900km wind field
  exposed the entire NE corridor for 36+ hours — $90B, #5 all-time.

Formula:
  cumDPI = peak_dpi × (1 + duration_factor + breadth_factor)

  Where:
    peak_dpi:        Max single-snapshot DPI along the track (our existing metric)
    duration_factor: Bonus for prolonged exposure near populated coast
                     = Σ (DPI_i / peak_dpi × Δt_hours) / T_ref,  capped at 0.40
                     Only counts snapshots where DPI_i > 30 (meaningful threat)
                     T_ref = 24 hours (typical crossing time for a "normal" storm)
    breadth_factor:  Bonus for large storms tracking along populated coast
                     = (IKE_peak / IKE_ref) × coastal_hours / coastal_ref, capped at 0.30
                     IKE_ref = 80 TJ (Katrina-class)
                     coastal_ref = 36 hours

  The cumDPI is then capped at 100.

  This design keeps the existing DPI as the anchor (a fast-crossing Cat 5 still
  scores high) while adding credit for duration and breadth effects that the
  single-snapshot model misses.

  Expected improvements:
    Harvey: ~70 → ~85+ (5 days of flooding over Houston)
    Sandy:  ~54 → ~72+ (36h of hurricane-force over NE corridor)
    Irma:   ~63 → ~73+ (24h tracking up FL peninsula)
    Ian:    ~83 → ~88  (modest duration boost)
    Katrina: ~86 → ~92 (already peaks high, modest boost)
"""

import math
import json
import os
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from core.dpi import compute_dpi_simple, DPIResult, categorize_dpi

logger = logging.getLogger(__name__)

NM_TO_M = 1852.0
KT_TO_MS = 0.514444

# Reference values for normalization
T_REF_HOURS = 24.0        # Typical crossing duration
IKE_REF_TJ = 150.0        # Above-average IKE (Katrina-class = 200+)
COASTAL_REF_HOURS = 48.0   # Reference coastal exposure time
DPI_THREAT_THRESHOLD = 25.0  # Min DPI to count as "meaningful threat"
                              # Lower than single-DPI threshold because weakened
                              # storms still cause flooding (Harvey post-landfall)
DURATION_CAP = 0.18        # Max duration bonus (fraction of peak)
BREADTH_CAP = 0.15         # Max breadth bonus (fraction of peak)

# Simple land proximity check: lat/lon bounding boxes for US coastal zones
# A snapshot is "near coast" if it falls within these boxes.
# This is a rough heuristic — the real land_proximity module gives precise
# distances, but we need something fast for batch computation.
COASTAL_BOXES = [
    # (lat_min, lat_max, lon_min, lon_max, label)
    (24.5, 31.0, -98.0, -80.0, "Gulf Coast"),         # TX to FL Panhandle
    (25.0, 27.5, -82.5, -79.5, "FL West/South"),       # SW FL, Keys
    (25.0, 31.0, -82.0, -79.5, "FL East"),             # FL Atlantic coast
    (31.0, 36.5, -82.0, -75.0, "SE Atlantic"),         # GA, SC, NC
    (36.5, 42.0, -77.0, -70.0, "Mid-Atlantic / NE"),   # VA to CT
    (17.0, 19.5, -68.0, -64.0, "Puerto Rico / USVI"),  # Caribbean US
    (23.0, 27.5, -80.0, -72.0, "Bahamas proximity"),   # Close enough to FL
    (28.0, 31.0, -94.0, -88.0, "LA / MS coast"),       # Louisiana, Mississippi
]


@dataclass
class CumulativeDPIResult:
    """Result of a multi-snapshot cumulative DPI calculation."""
    cum_dpi: float                  # Final cumulative DPI (0-100)
    cum_category: str               # Severity category
    peak_dpi: float                 # Best single-snapshot DPI
    peak_timestamp: str             # When peak occurred
    peak_location: Tuple[float, float]  # (lat, lon) of peak
    duration_factor: float          # Duration bonus applied (0 to DURATION_CAP)
    breadth_factor: float           # Breadth bonus applied (0 to BREADTH_CAP)
    total_coastal_hours: float      # Hours with DPI > threshold near coast
    total_track_hours: float        # Total storm lifetime in hours
    snapshots_computed: int         # Number of snapshots processed
    coastal_snapshots: int          # Number near-coast snapshots
    peak_ike_tj: float              # Peak IKE along track
    storm_name: str
    storm_year: int
    # Per-snapshot DPI series for charting
    dpi_timeseries: List[Dict]


def _is_near_coast(lat: float, lon: float) -> bool:
    """Quick check if a lat/lon is within a US coastal bounding box."""
    for lat_min, lat_max, lon_min, lon_max, _ in COASTAL_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return True
    return False


def _get_coastal_label(lat: float, lon: float) -> str:
    """Get the coastal zone label for a lat/lon."""
    for lat_min, lat_max, lon_min, lon_max, label in COASTAL_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return label
    return "Open Ocean"


def _parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO timestamp from preload bundle."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts_str}")


def _estimate_region_from_coords(lat: float, lon: float) -> Optional[str]:
    """Quick region key estimate from coordinates for DPI computation."""
    # Gulf Coast
    if 25.0 <= lat <= 31.0 and -98.0 <= lon <= -94.0:
        return "gulf_central_tx"
    if 28.0 <= lat <= 31.0 and -94.0 <= lon <= -88.5:
        return "gulf_la"
    if 29.0 <= lat <= 31.0 and -88.5 <= lon <= -85.0:
        return "gulf_fl_panhandle"
    if 25.0 <= lat <= 29.0 and -84.0 <= lon <= -81.0:
        return "gulf_fl_west"
    # Atlantic FL
    if 25.0 <= lat <= 31.0 and -81.5 <= lon <= -79.0:
        return "atl_fl_east"
    # SE Atlantic
    if 31.0 <= lat <= 34.0 and -82.0 <= lon <= -77.0:
        return "atl_ga_sc"
    if 33.0 <= lat <= 36.5 and -80.0 <= lon <= -75.0:
        return "atl_nc"
    # NE
    if 36.5 <= lat <= 42.0 and -77.0 <= lon <= -70.0:
        return "atl_ne"
    # Caribbean
    if 17.0 <= lat <= 19.5 and -68.0 <= lon <= -64.0:
        return "carib_pr"
    if 22.0 <= lat <= 27.5 and -80.0 <= lon <= -72.0:
        return "carib_bahamas"
    return None


def compute_snapshot_dpi(snapshot: Dict) -> Tuple[float, DPIResult]:
    """
    Compute single-snapshot DPI from a preload bundle snapshot.

    Returns: (dpi_score, full_result)
    """
    vmax_ms = snapshot.get("max_wind_ms", 0)
    if not vmax_ms or vmax_ms < 10:  # Below tropical depression
        return 0.0, None

    lat = snapshot["lat"]
    lon = snapshot["lon"]
    region = _estimate_region_from_coords(lat, lon)

    # Extract quadrant data if available
    r34_quads = None
    r50_quads = None
    r64_quads = None

    if snapshot.get("r34_quadrants"):
        q = snapshot["r34_quadrants"]
        r34_quads = {k: v * NM_TO_M for k, v in q.items()}

    r34_nm = snapshot.get("r34_nm") or 0
    r64_nm = snapshot.get("r64_nm") or 0
    rmw_nm = snapshot.get("rmw_nm") or 30

    # Estimate r50 from r34 and r64 (geometric mean of available radii)
    if r34_nm > 0 and r64_nm > 0:
        r50_est = math.sqrt(r34_nm * r64_nm)
        if r34_quads:
            ratio = r50_est / r34_nm
            r50_quads = {k: v * ratio for k, v in r34_quads.items()}
            r64_ratio = r64_nm / r34_nm
            r64_quads = {k: v * r64_ratio for k, v in r34_quads.items()}

    fwd_kt = snapshot.get("forward_speed_knots") or 5
    fwd_ms = fwd_kt * KT_TO_MS

    try:
        result = compute_dpi_simple(
            vmax_ms=vmax_ms,
            min_pressure_hpa=snapshot.get("min_pressure_hpa"),
            lat=lat,
            lon=lon,
            r34_m=r34_nm * NM_TO_M if r34_nm else None,
            rmw_m=rmw_nm * NM_TO_M,
            forward_speed_ms=fwd_ms,
            r34_quadrants_m=r34_quads,
            r50_quadrants_m=r50_quads,
            r64_quadrants_m=r64_quads,
            region_key=region,
            storm_id=snapshot.get("storm_id", "UNKNOWN"),
            name=snapshot.get("storm_id", "Unknown"),
            apply_land_dampening=False,  # We handle land proximity ourselves
        )
        return result.dpi_score, result
    except Exception as e:
        logger.debug(f"DPI computation failed for snapshot: {e}")
        return 0.0, None


def compute_cumulative_dpi(
    snapshots: List[Dict],
    storm_name: str = "Unknown",
    storm_year: int = 2024,
) -> CumulativeDPIResult:
    """
    Compute cumulative DPI from a series of storm snapshots.

    Args:
        snapshots: List of snapshot dicts from preload_bundle.json
        storm_name: Display name
        storm_year: Year for era adjustments

    Returns:
        CumulativeDPIResult with cumulative score and breakdown
    """
    if not snapshots:
        return CumulativeDPIResult(
            cum_dpi=0, cum_category="None", peak_dpi=0, peak_timestamp="",
            peak_location=(0, 0), duration_factor=0, breadth_factor=0,
            total_coastal_hours=0, total_track_hours=0, snapshots_computed=0,
            coastal_snapshots=0, peak_ike_tj=0, storm_name=storm_name,
            storm_year=storm_year, dpi_timeseries=[],
        )

    # Compute DPI for each snapshot
    dpi_series = []
    for snap in snapshots:
        dpi_val, result = compute_snapshot_dpi(snap)
        ts = snap.get("timestamp", "")
        lat, lon = snap["lat"], snap["lon"]
        near_coast = _is_near_coast(lat, lon)
        ike_tj = snap.get("ike_total_tj", 0) or 0

        dpi_series.append({
            "timestamp": ts,
            "lat": lat,
            "lon": lon,
            "dpi": dpi_val,
            "near_coast": near_coast,
            "coastal_zone": _get_coastal_label(lat, lon) if near_coast else "Open Ocean",
            "max_wind_ms": snap.get("max_wind_ms", 0),
            "ike_tj": ike_tj,
            "fwd_kt": snap.get("forward_speed_knots", 0),
        })

    # Find peak DPI
    peak_entry = max(dpi_series, key=lambda x: x["dpi"])
    peak_dpi = peak_entry["dpi"]
    peak_ike = max(s["ike_tj"] for s in dpi_series)

    if peak_dpi < 5:
        return CumulativeDPIResult(
            cum_dpi=0, cum_category="None", peak_dpi=0, peak_timestamp="",
            peak_location=(0, 0), duration_factor=0, breadth_factor=0,
            total_coastal_hours=0, total_track_hours=0,
            snapshots_computed=len(dpi_series), coastal_snapshots=0,
            peak_ike_tj=peak_ike, storm_name=storm_name,
            storm_year=storm_year, dpi_timeseries=dpi_series,
        )

    # Compute time intervals between snapshots
    timestamps = []
    for s in dpi_series:
        try:
            timestamps.append(_parse_timestamp(s["timestamp"]))
        except ValueError:
            timestamps.append(None)

    # Total track duration
    valid_ts = [t for t in timestamps if t is not None]
    if len(valid_ts) >= 2:
        total_hours = (valid_ts[-1] - valid_ts[0]).total_seconds() / 3600.0
    else:
        total_hours = len(dpi_series) * 6.0  # Assume 6h intervals

    # ── Duration Factor ──
    # Sum of (DPI_i / peak_dpi × Δt_hours) for snapshots where:
    #   1. DPI > threshold (meaningful threat)
    #   2. Near a populated coast
    # Normalized by T_ref (24h standard crossing time)
    duration_integral = 0.0
    coastal_hours = 0.0
    coastal_count = 0

    for i, s in enumerate(dpi_series):
        if s["dpi"] < DPI_THREAT_THRESHOLD or not s["near_coast"]:
            continue

        coastal_count += 1

        # Compute Δt for this snapshot
        if i < len(dpi_series) - 1 and timestamps[i] and timestamps[i + 1]:
            dt_hours = (timestamps[i + 1] - timestamps[i]).total_seconds() / 3600.0
        elif i > 0 and timestamps[i] and timestamps[i - 1]:
            dt_hours = (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600.0
        else:
            dt_hours = 6.0  # Default 6h interval

        dt_hours = min(dt_hours, 12.0)  # Cap at 12h to avoid gaps

        coastal_hours += dt_hours
        # Weight by relative DPI intensity — but use a lower threshold
        # to capture post-landfall exposure (Harvey stalling at tropical
        # storm intensity still causes massive flooding damage).
        # Weight is (DPI / peak)^0.5 to give more credit to weaker
        # but still-active snapshots.
        weight = math.sqrt(s["dpi"] / peak_dpi)
        duration_integral += weight * dt_hours

    # ── Economic Density Factor ──
    # Scale cumulative bonuses by the economic density of the most-affected zone.
    # Storms stalling over small-economy islands (Bahamas) shouldn't get the
    # same duration/breadth credit as storms stalling over Houston or Miami.
    econ_density_factor = 1.0
    if peak_entry.get("coastal_zone") == "Bahamas proximity":
        econ_density_factor = 0.15  # Tiny economy
    elif peak_entry.get("coastal_zone") == "Puerto Rico / USVI":
        econ_density_factor = 0.60  # Moderate US territory

    # ── Duration Factor ──
    # Normalize: a "standard" storm crosses in ~24h with DPI at peak.
    # Anything above that = prolonged exposure.
    excess_duration = max(0.0, duration_integral - T_REF_HOURS)
    duration_factor = min(DURATION_CAP, excess_duration / (T_REF_HOURS * 3.0) * econ_density_factor)

    # ── Breadth Factor ──
    # Large storms (high IKE) tracking along coast for extended periods
    # affect a wide geographic area. This bonus captures the "Sandy effect"
    # where a Cat 1's enormous wind field causes more total damage than
    # many Cat 4s.
    ike_norm = min(1.0, peak_ike / IKE_REF_TJ)
    coastal_time_norm = min(1.0, coastal_hours / COASTAL_REF_HOURS)
    breadth_raw = ike_norm * coastal_time_norm * 0.20 * econ_density_factor
    breadth_factor = min(BREADTH_CAP, breadth_raw)

    # ── Cumulative DPI ──
    cum_dpi = peak_dpi * (1.0 + duration_factor + breadth_factor)
    cum_dpi = min(100.0, cum_dpi)

    return CumulativeDPIResult(
        cum_dpi=round(cum_dpi, 1),
        cum_category=categorize_dpi(cum_dpi),
        peak_dpi=round(peak_dpi, 1),
        peak_timestamp=peak_entry["timestamp"],
        peak_location=(peak_entry["lat"], peak_entry["lon"]),
        duration_factor=round(duration_factor, 4),
        breadth_factor=round(breadth_factor, 4),
        total_coastal_hours=round(coastal_hours, 1),
        total_track_hours=round(total_hours, 1),
        snapshots_computed=len(dpi_series),
        coastal_snapshots=coastal_count,
        peak_ike_tj=round(peak_ike, 1),
        storm_name=storm_name,
        storm_year=storm_year,
        dpi_timeseries=dpi_series,
    )


def compute_all_from_bundle(bundle_path: Optional[str] = None) -> List[CumulativeDPIResult]:
    """
    Load preload_bundle.json and compute cumulative DPI for all storms.

    Returns list of CumulativeDPIResult sorted by cum_dpi descending.
    """
    if bundle_path is None:
        bundle_path = os.path.join(
            os.path.dirname(__file__), "..", "frontend", "preload_bundle.json"
        )

    with open(bundle_path) as f:
        data = json.load(f)

    storms = data.get("storms", {})

    # Storm ID → name mapping (from the preset names we know)
    ID_NAMES = {
        "AL122005": ("Katrina", 2005),
        "AL092024": ("Helene", 2024),
        "AL152017": ("Maria", 2017),
        "AL112017": ("Irma", 2017),
        "AL092022": ("Ian", 2022),
        "AL142018": ("Michael", 2018),
        "AL142024": ("Milton", 2024),
        "AL182012": ("Sandy", 2012),
        "AL092008": ("Ike", 2008),
        "AL092017": ("Harvey", 2017),
        "AL052019": ("Dorian", 2019),
        "AL062018": ("Florence", 2018),
        "AL102023": ("Idalia", 2023),
        "AL022024": ("Beryl", 2024),
    }

    results = []
    for storm_id, snapshots in storms.items():
        name, year = ID_NAMES.get(storm_id, (storm_id, 2024))
        result = compute_cumulative_dpi(snapshots, storm_name=name, storm_year=year)
        results.append(result)

    results.sort(key=lambda r: -r.cum_dpi)
    return results
