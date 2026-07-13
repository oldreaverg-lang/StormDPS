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
                     = Σ (DPI_i / peak_dpi × Δt_hours) / T_ref,  capped at 0.25
                     Only counts snapshots where DPI_i > 25 (meaningful threat)
                     T_ref = 24 hours (typical crossing time for a "normal" storm)
    breadth_factor:  Bonus for large storms tracking along populated coast
                     = (IKE_peak / IKE_ref) × coastal_hours / coastal_ref, capped at 0.25
                     IKE_ref = 150 TJ (above-average)
                     coastal_ref = 48 hours

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
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

from core.dpi import compute_dpi_simple, DPIResult, categorize_dpi

# [WP_DPS_AUDIT_V2 §7, Tranche B] WP land predicates are waypoint-distance
# driven (core/land_proximity wp_* points), not bounding-box driven — the
# rectangles couldn't represent the Japan/China coasts (the "tight" Japan box
# still spanned 800 km of Philippine Sea, so loitering recurvers accrued
# coastal hours and profile credit they never earned). Fail-soft: if the
# module is unavailable the box fallback keeps pre-Tranche-B behavior.
try:
    from core import land_proximity as _lp
except ImportError:  # pragma: no cover — land_proximity has no heavy deps
    _lp = None

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
DURATION_CAP = 0.10        # Max duration bonus (fraction of peak)  [v6: reduced from 0.15 to widen score spread]
BREADTH_CAP = 0.10         # Max breadth bonus (fraction of peak)  [v6: reduced from 0.15 to widen score spread]

# Per-basin breadth/duration tuning. Atlantic landfalling storms routinely spread
# across many states (Isaias, Idalia, Debby) where the geographic footprint drove
# the federal disaster response far more than peak intensity did; the default caps
# under-credited that, leaving them as low-DPS / high-FEMA-county-footprint
# outliers. The Atlantic boost (lower coastal-hours gate + higher breadth/duration
# caps) tightens that validation (Spearman ρ vs counties 0.66→0.68). It is scoped
# to the Atlantic ONLY: raising these globally re-inflated the West Pacific and
# pushed the already-over-scored Fung-Wong higher, for no benefit on a US/FEMA
# chart WP storms don't even appear on. Other basins keep the original values.
_BASIN_CUM_TUNING = {
    "ATLANTIC": {"threshold": 12.0, "duration_cap": 0.20, "breadth_cap": 0.25},
}
_DEFAULT_CUM_TUNING = {
    "threshold": DPI_THREAT_THRESHOLD,
    "duration_cap": DURATION_CAP,
    "breadth_cap": BREADTH_CAP,
}

# [F7] Per-zone weights for duration/breadth accumulation.
# Caribbean island and near-miss hours count at a fraction of US mainland hours
# for a US-centric DPS score. This prevents long Caribbean tracks (Matthew, Irma)
# from inflating duration/breadth as much as equivalent time over the US coast.
US_MAINLAND_ZONES = {
    "Gulf Coast", "FL West/South", "FL East",
    "SE Atlantic", "Mid-Atlantic / NE", "LA / MS coast",
    "New Orleans", "Biloxi / Gulfport", "Mobile",
}
ZONE_WEIGHTS = {
    # US mainland coast → full credit
    "Gulf Coast":          1.0,
    "FL West/South":       1.0,
    "FL East":             1.0,
    "SE Atlantic":         1.0,
    "Mid-Atlantic / NE":   1.0,
    "LA / MS coast":       1.0,
    # US territory (Caribbean island, moderate economy)
    "Puerto Rico / USVI":  0.40,
    # Near-miss / small economy island chain
    "Bahamas proximity":   0.15,
    # Western Pacific — mirrors compile_cache.COASTAL_EXPOSURE_WEIGHTS so WP
    # storms accumulate duration/breadth credit over coastal waters. Without
    # these entries, every WP storm gets duration_factor=0, breadth_factor=0
    # (see the Sinlaku 2026 audit).
    "Japan":               0.80,
    "Taiwan":              0.55,
    "Mariana Islands":     0.55,  # US territory — Guam, Saipan, Tinian, Rota
    "China":               0.50,
    "Philippines":         0.35,
    "Vietnam / Cambodia":  0.30,
    "Thailand / Laos":     0.25,
    # [Tranche B] Korea gets its own zone: pre-waypoint geometry lumped the
    # Korean coast into the Japan box (0.80). 0.55 sits between Taiwan and
    # Japan — dense, hardened coast but a smaller exposed corridor.
    "Korea":               0.55,
    # Eastern Pacific — added 2026-05-15 ahead of El Niño 2026 season.
    # Without these entries every EP storm got duration_factor=0 and
    # breadth_factor=0, the same way WP did pre-v9. Calibrated against
    # exposure density and historical damage profile.
    "Mexico Pacific":      0.55,  # Acapulco, Manzanillo, Mazatlán — dense + vulnerable
    "Baja California":     0.30,  # Cabo, La Paz — sparse coast, low density
    "Central America Pacific": 0.40,  # El Salvador, Guatemala, Nicaragua coast
    "Hawaii":              0.55,  # Oahu metro / Hilo / Maui — dense small islands, high-value
    # Southern Hemisphere — [SH_DPS_AUDIT 2026-07]. Without these every SI/SP
    # storm accrued duration_factor=0 / breadth_factor=0 (open-ocean).
    "E Australia":         0.70,  # Queensland — hardened, high value
    "New Zealand":         0.70,  # N Island — high value, ex-TC river floods
    "Mascarene":           0.45,  # Mauritius / Réunion — dense, wealthy islands
    "Fiji":                0.45,
    "Mozambique":          0.35,  # dense, poor, surge/flood-prone
    "Madagascar":          0.35,
    "New Caledonia":       0.45,
    "Tonga / Samoa":       0.30,
    "Vanuatu":             0.30,
    "Solomon":             0.25,
    "Timor":               0.30,
    "W Australia":         0.20,  # Pilbara/Kimberley — among the emptiest coasts
    # North Indian — [NI_DPS_AUDIT 2026-07]. The Bay of Bengal deltas are among
    # the densest, most surge-exposed coasts on Earth.
    "Bangladesh":          0.55,
    "Myanmar":             0.40,
    "Odisha":              0.45,
    "Andhra Pradesh":      0.45,
    "Tamil Nadu":          0.50,
    "Sri Lanka":           0.35,
    "Kerala":              0.45,
    "Gujarat / Pakistan":  0.55,
    "Oman / Yemen":        0.30,
    "Somalia":             0.20,
    # Default for any near-coast snapshot not in the above list
    "Open Ocean":          0.0,
}

# [Tranche B] wp_* waypoint region key → coastal-zone label. Keeps the
# zone-weight system (and its US-centric calibration) unchanged while the
# WP near-coast predicate moves from boxes to waypoint distance.
WP_KEY_TO_ZONE = {
    "wp_philippines": "Philippines",
    "wp_taiwan":      "Taiwan",
    "wp_japan":       "Japan",
    "wp_korea":       "Korea",
    "wp_south_china": "China",
    "wp_hainan":      "China",
    "wp_vietnam":     "Vietnam / Cambodia",
    "wp_marianas":    "Mariana Islands",
}

# Tranche B distance gate (km) against the wp_* waypoints
WP_COASTAL_KM = 100.0   # coastal-hours / duration / breadth accrual

# Living-legs profile reach is TIERED by the waypoint's population density —
# the distance at which a coast projects economic/surge exposure scales with
# how much coast that waypoint represents. Sub-0.20 points are remote islets
# (Batanes, Calayan, Palanan, Yakushima, Rota) that anchor landfall detection
# and the no-landfall dampener but represent no exposure surface: without
# the gate a Luzon-Strait Cat 5 brushing Basco collected the full Philippines
# surge/econ legs and out-scored actual PRD strikes (Ragasa 89 / Kong-Rey 74 /
# Saola 86 in calibration run 1). Mid-density points (small coastal towns)
# reach 75 km; city-scale coasts reach the full 150 km.
WP_PROFILE_TIERS = (
    (0.40, 150.0),   # city-scale coast: full profile reach
    (0.20, 75.0),    # town-scale coast: storm must be closing in
)                    # < 0.20: islet — no exposure profile at any distance

# South China Sea approach corridor (Paracels): a major TC here is hours
# from Hainan / N Vietnam / PRD landfall — the semi-enclosed sea leaves no
# recurve exit, so peak intensity in the corridor is committed destructive
# power (Yagi 2024, Rammasun 2014). Profile-mapping ONLY: never counts as
# land contact, landfall, or coastal hours. (From wp_recal_harness S5.)
WP_SCS_CORRIDOR = (15.5, 21.5, 108.0, 117.0)

# [SH_DPS_AUDIT 2026-07] sh_* waypoint region key → coastal-zone label +
# the same tiered profile reach as WP. Southern-latitude gated in
# land_proximity so no NH storm can observe these.
SH_KEY_TO_ZONE = {
    "sh_mozambique":     "Mozambique",
    "sh_madagascar":     "Madagascar",
    "sh_mascarene":      "Mascarene",
    "sh_w_australia":    "W Australia",
    "sh_e_australia":    "E Australia",
    "sh_fiji":           "Fiji",
    "sh_vanuatu":        "Vanuatu",
    "sh_new_caledonia":  "New Caledonia",
    "sh_tonga_samoa":    "Tonga / Samoa",
    "sh_new_zealand":    "New Zealand",
    "sh_solomon":        "Solomon",
    "sh_timor":          "Timor",
}
SH_COASTAL_KM = 100.0
SH_PROFILE_TIERS = (
    (0.40, 150.0),   # city-scale coast: full profile reach
    (0.20, 75.0),    # town-scale coast: storm must be closing in
)                    # < 0.20: sparse coast (Pilbara/remote islands) — detection only

# [NI_DPS_AUDIT 2026-07] North Indian — same architecture, ni_* waypoints.
NI_KEY_TO_ZONE = {
    "ni_bangladesh":        "Bangladesh",
    "ni_myanmar":           "Myanmar",
    "ni_odisha":            "Odisha",
    "ni_andhra":            "Andhra Pradesh",
    "ni_tamilnadu":         "Tamil Nadu",
    "ni_srilanka":          "Sri Lanka",
    "ni_kerala":            "Kerala",
    "ni_gujarat_pakistan":  "Gujarat / Pakistan",
    "ni_oman_yemen":        "Oman / Yemen",
    "ni_somalia":           "Somalia",
}
NI_COASTAL_KM = 100.0
NI_PROFILE_TIERS = (
    (0.40, 150.0),
    (0.20, 75.0),
)                    # < 0.20: Sundarbans mangroves / Socotra — detection only

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
    # --- Western Pacific ---
    # Placed before Japan so Saipan (15.2°N, 145.7°E) matches Mariana, not Japan.
    # Mirrors compile_cache.COASTAL_REGIONS. Without these, every WP storm
    # gets duration_factor=0, breadth_factor=0 (Sinlaku 2026 audit).
    #
    # [WP_DPS_AUDIT_V2 2026-07] Geometry tightened. The old Philippines box ran
    # to 135E — ~800 km of open Philippine Sea — so open-ocean recurvers
    # (Surigae 2021) accrued coastal hours and defeated the no-landfall
    # dampener; the "Vietnam / Cambodia" box (20-25N / 115-122E) was drawn in
    # the Taiwan Strait, nowhere near Vietnam. The Philippines is now split
    # into latitude bands because its east coast slants from 126.6E (Mindanao)
    # to 122.4E (Luzon): one rectangle either cuts Samar off or swallows the
    # Philippine Sea corridor where near-misses pass.
    (13.0, 20.5, 144.0, 146.5, "Mariana Islands"),     # Guam, Saipan, Tinian, Rota
    (4.5, 12.8, 116.9, 127.3, "Philippines"),          # Mindanao / Visayas / Samar
    (12.8, 15.0, 119.5, 125.3, "Philippines"),         # Bicol / Catanduanes
    (15.0, 18.8, 119.6, 122.9, "Philippines"),         # Luzon
    (18.8, 21.2, 120.4, 122.4, "Philippines"),         # Batanes / Babuyan
    (8.0, 21.8, 102.0, 110.4, "Vietnam / Cambodia"),   # actual Vietnam coast
    (21.7, 25.5, 119.8, 122.2, "Taiwan"),
    (24,   45.5, 122.5, 146,   "Japan"),
    (5.0, 15.0, 98.0, 105.2, "Thailand / Laos"),       # Gulf of Thailand
    (20,   41,   105.5, 123,   "China"),
    # --- Eastern Pacific ---
    # Added 2026-05-15 ahead of El Niño 2026 EP season. Without these
    # the EP basin had no coastal coverage; every EP storm got
    # duration_factor=0 / breadth_factor=0, the same gap WP had pre-v9.
    # Boxes are ordered specific → general so Acapulco (16.9N, -99.8W)
    # matches "Mexico Pacific" not the broader Central America box.
    (22.0, 27.0, -111.0, -105.0, "Baja California"),    # Cabo to La Paz to Loreto
    (16.0, 22.0, -106.0,  -97.0, "Mexico Pacific"),     # Acapulco, Manzanillo, PV, Mazatlán
    (10.0, 16.0,  -94.0,  -83.0, "Central America Pacific"),  # El Salvador, Guatemala, Nicaragua
    (18.5, 22.5, -160.5, -154.5, "Hawaii"),             # Hawaiian Islands
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


def _plausible_ike_tj(snapshot: Dict) -> float:
    """IKE for the cumulative layer, gated against impossible radii rows.

    [WP_DPS_AUDIT_V2 2026-07] IBTrACS carries glitch rows where a 35-40 kt
    tropical storm reports r34 of 290-350 nm — physically impossible gale
    radii that produce identical ~196 TJ IKE spikes (Ragasa, Kong-Rey, Gaemi,
    Doksuri, Haiyan, Rai all shared the exact value 196.31) and pin the
    breadth factor at its cap. When the (vmax, r34) pair is implausible the
    snapshot's IKE is clamped to 20 TJ — generous for any storm below
    hurricane strength (the glitch rows' clean neighbors run 3-17 TJ).
    """
    ike = snapshot.get("ike_total_tj", 0) or 0
    r34 = snapshot.get("r34_nm", 0) or 0
    if not r34 or ike <= 20.0:
        return ike
    vmax = snapshot.get("max_wind_ms", 0) or 0
    implausible = (
        vmax < 17.5                        # no 34-kt wind exists at all
        or (vmax < 25.7 and r34 > 150)     # <50 kt: 150 nm gale-radius ceiling
        or (vmax < 33.0 and r34 > 250)     # <64 kt: 250 nm ceiling
    )
    return min(ike, 20.0) if implausible else ike


def _wp_coast_hit(lat: float, lon: float, radius_km: float):
    """
    [Tranche B] Waypoint-distance test for WP-window coordinates.

    Returns the wp_* region key if the point is within radius_km of a WP
    coastline waypoint, "" if it's in the WP window but too far from land,
    or None if outside the window / module unavailable (caller falls back
    to the bounding-box test — pre-Tranche-B behavior).
    """
    if _lp is None:
        return None
    hit = _lp.nearest_wp_coast(lat, lon)
    if hit is None:
        return None
    dist_km, region_key, _pop = hit
    return region_key if dist_km <= radius_km else ""


def _sh_coast_hit(lat: float, lon: float, radius_km: float):
    """[SH_DPS_AUDIT] Southern-hemisphere analog of _wp_coast_hit."""
    if _lp is None:
        return None
    hit = _lp.nearest_sh_coast(lat, lon)
    if hit is None:
        return None
    dist_km, region_key, _pop = hit
    return region_key if dist_km <= radius_km else ""


def _ni_coast_hit(lat: float, lon: float, radius_km: float):
    """[NI_DPS_AUDIT] North-Indian analog. Checked BEFORE WP so the 95–97 E
    Andaman/Myanmar overlap resolves to NI, not WP."""
    if _lp is None:
        return None
    hit = _lp.nearest_ni_coast(lat, lon)
    if hit is None:
        return None
    dist_km, region_key, _pop = hit
    return region_key if dist_km <= radius_km else ""


def _is_near_coast(lat: float, lon: float) -> bool:
    """Quick check if a lat/lon is near a populated coast.

    NI/WP/SH-window coordinates use waypoint distance (<=100 km); everything
    else keeps the bounding-box test. NI is tested first so the small NI/WP
    longitude overlap (Myanmar) resolves to NI.
    """
    ni = _ni_coast_hit(lat, lon, NI_COASTAL_KM)
    if ni is not None:
        return bool(ni)
    wp = _wp_coast_hit(lat, lon, WP_COASTAL_KM)
    if wp is not None:
        return bool(wp)
    sh = _sh_coast_hit(lat, lon, SH_COASTAL_KM)
    if sh is not None:
        return bool(sh)
    for lat_min, lat_max, lon_min, lon_max, _ in COASTAL_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return True
    return False


def _get_coastal_label(lat: float, lon: float) -> str:
    """Get the coastal zone label for a lat/lon."""
    ni = _ni_coast_hit(lat, lon, NI_COASTAL_KM)
    if ni is not None:
        return NI_KEY_TO_ZONE.get(ni, "Open Ocean") if ni else "Open Ocean"
    wp = _wp_coast_hit(lat, lon, WP_COASTAL_KM)
    if wp is not None:
        return WP_KEY_TO_ZONE.get(wp, "Open Ocean") if wp else "Open Ocean"
    sh = _sh_coast_hit(lat, lon, SH_COASTAL_KM)
    if sh is not None:
        return SH_KEY_TO_ZONE.get(sh, "Open Ocean") if sh else "Open Ocean"
    for lat_min, lat_max, lon_min, lon_max, label in COASTAL_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return label
    return "Open Ocean"


def _get_zone_weight(coastal_zone: str) -> float:
    """
    [F7] Return the duration/breadth accumulation weight for a coastal zone.

    US mainland zones count at 1.0. Caribbean islands and near-miss areas are
    discounted so that storms spending most of their track over foreign islands
    (Matthew, Irma) don't get the same duration/breadth credit as storms that
    linger over populated US coast (Harvey, Sandy).
    """
    return ZONE_WEIGHTS.get(coastal_zone, 0.30)  # unknown zones: 30% credit


def _parse_timestamp(ts_str: str) -> datetime:
    """Parse an ISO timestamp from the preload bundle or live track JSON.

    Live b-deck snapshots serialize tz-aware ("2026-07-01T00:00:00+00:00");
    baked bundles are naive ("2005-08-29T12:00:00"). Normalize to naive UTC
    so downstream arithmetic can mix freely. (The old strptime list rejected
    the tz suffix, so every LIVE storm silently fell back to count×6h track
    hours and default 6h duration steps — Bavi 2026 audit.)
    """
    try:
        dt = datetime.fromisoformat(ts_str)
    except (TypeError, ValueError):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts_str, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Cannot parse timestamp: {ts_str}")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


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
    # [WP_DPS_AUDIT_V2 §7, Tranche B] Western Pacific living legs: within
    # the tiered profile reach of a wp_* coastline waypoint the snapshot
    # gets the matching coastal/economic profile. Beyond the gates the
    # region is pinned to "open_ocean" EXPLICITLY — returning None here
    # would let the surge/econ formulas auto-detect via
    # land_proximity.get_nearest_region, whose 930 km threshold would light
    # the legs up for storms sitting half a basin offshore (the baked path
    # applies no land dampening, so that generosity would be undamped).
    if _lp is not None:
        # [NI_DPS_AUDIT] North Indian FIRST (the 95–97 E Andaman/Myanmar band
        # overlaps the WP window; NI owns it). Same tiered reach + explicit
        # open_ocean pin as WP/SH.
        ni_hit = _lp.nearest_ni_coast(lat, lon)
        if ni_hit is not None:
            dist_km, key, pop = ni_hit
            for min_pop, max_km in NI_PROFILE_TIERS:
                if pop >= min_pop and dist_km <= max_km:
                    return key
            return "open_ocean"
        hit = _lp.nearest_wp_coast(lat, lon)
        if hit is not None:
            dist_km, key, pop = hit
            for min_pop, max_km in WP_PROFILE_TIERS:
                if pop >= min_pop and dist_km <= max_km:
                    return key
            la0, la1, lo0, lo1 = WP_SCS_CORRIDOR
            if la0 <= lat <= la1 and lo0 <= lon <= lo1:
                return "wp_hainan"
            return "open_ocean"
        # [SH_DPS_AUDIT] Southern Hemisphere living legs — identical tiered
        # reach, southern-latitude gated (in_sh_window). Same explicit
        # open_ocean pin beyond the tiers so the 930 km auto-detect can't
        # light the legs up mid-ocean.
        sh_hit = _lp.nearest_sh_coast(lat, lon)
        if sh_hit is not None:
            dist_km, key, pop = sh_hit
            for min_pop, max_km in SH_PROFILE_TIERS:
                if pop >= min_pop and dist_km <= max_km:
                    return key
            return "open_ocean"
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
    basin: Optional[str] = None,
) -> CumulativeDPIResult:
    """
    Compute cumulative DPI from a series of storm snapshots.

    Args:
        snapshots: List of snapshot dicts from preload_bundle.json
        storm_name: Display name
        storm_year: Year for era adjustments
        basin: Basin key (e.g. "ATLANTIC"); selects per-basin breadth/duration
            tuning (see _BASIN_CUM_TUNING). None → default tuning.

    Returns:
        CumulativeDPIResult with cumulative score and breakdown
    """
    _tune = _BASIN_CUM_TUNING.get(basin, _DEFAULT_CUM_TUNING)
    _threshold = _tune["threshold"]
    _duration_cap = _tune["duration_cap"]
    _breadth_cap = _tune["breadth_cap"]
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
        ike_tj = _plausible_ike_tj(snap)

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
            "et": bool(snap.get("et", False)),
        })

    # Find peak DPI
    peak_entry = max(dpi_series, key=lambda x: x["dpi"])
    peak_dpi = peak_entry["dpi"]
    # Peak IKE excludes extratropical snapshots (NATURE=ET / USA_STATUS=EX): the
    # post-tropical phase otherwise dominates the peak for recurving storms and
    # misrepresents the tropical-phase threat. Falls back to all if fully ET.
    #
    # [WP_DPS_AUDIT_V2 2026-07] SUSTAINED peak: max of the 3-snapshot rolling
    # mean, not the single-snapshot max. A wind field does not double for six
    # hours and vanish — single-fix spikes are radii glitches, and the breadth
    # factor was riding them straight to its cap.
    _trop = [s for s in dpi_series if not s.get("et")] or dpi_series
    _ikes = [s["ike_tj"] for s in _trop]
    if len(_ikes) >= 3:
        peak_ike = max(
            sum(_ikes[i - 1:i + 2]) / 3.0 for i in range(1, len(_ikes) - 1)
        )
    else:
        peak_ike = max(_ikes, default=0)

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
    # Sum of (DPI_i / peak_dpi × Δt_hours × zone_weight) for snapshots where:
    #   1. DPI > threshold (meaningful threat)
    #   2. Near a populated coast
    # Normalized by T_ref (24h standard crossing time).
    #
    # [F7] Each snapshot is weighted by its coastal zone (US mainland = 1.0,
    # Caribbean islands = 0.15–0.40). This prevents long Caribbean tracks
    # (Matthew, Irma) from accumulating as much duration/breadth credit as
    # equivalent time spent over the US coast.
    duration_integral = 0.0
    coastal_hours = 0.0          # Raw (unweighted) — for reporting only
    weighted_coastal_hours = 0.0 # Zone-weighted — used in breadth computation
    coastal_count = 0

    for i, s in enumerate(dpi_series):
        if s["dpi"] < _threshold or not s["near_coast"]:
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

        coastal_hours += dt_hours  # Raw hours for reporting

        # [F7] Zone weight: US mainland = 1.0, Caribbean islands = 0.15–0.40
        zone_weight = _get_zone_weight(s.get("coastal_zone", "Open Ocean"))
        weighted_coastal_hours += dt_hours * zone_weight

        # Weight by relative DPI intensity — but use a lower threshold
        # to capture post-landfall exposure (Harvey stalling at tropical
        # storm intensity still causes massive flooding damage).
        # Weight is (DPI / peak)^0.5 to give more credit to weaker
        # but still-active snapshots.
        intensity_weight = math.sqrt(s["dpi"] / peak_dpi)
        duration_integral += intensity_weight * dt_hours * zone_weight  # [F7]

    # ── Duration Factor ──
    # Normalize: a "standard" storm crosses in ~24h with DPI at peak.
    # Anything above that = prolonged exposure.
    # Zone weighting (F7) has already been folded into duration_integral,
    # so no separate econ_density_factor multiplication is needed.
    excess_duration = max(0.0, duration_integral - T_REF_HOURS)
    duration_factor = min(_duration_cap, excess_duration / (T_REF_HOURS * 3.0))

    # ── Breadth Factor ──
    # Large storms (high IKE) tracking along coast for extended periods
    # affect a wide geographic area. This bonus captures the "Sandy effect"
    # where a Cat 1's enormous wind field causes more total damage than
    # many Cat 4s.
    # [R2] Piecewise IKE normalization:
    #   Below IKE_REF: linear (0→1.0)
    #   Above IKE_REF: sqrt curve (1.0→~1.47 at 2x, ~1.82 at 3x)
    #   Capped at 2.0 to prevent runaway
    # This gives extra credit to anomalously large wind fields (Sandy 640 TJ,
    # Ida 576 TJ, Michael 321 TJ) without compressing them all to 1.0.
    # [F7] Use zone-weighted coastal hours so Caribbean tracks don't inflate
    # the breadth factor as much as equivalent US mainland exposure.
    if peak_ike <= IKE_REF_TJ:
        ike_norm = peak_ike / IKE_REF_TJ
    else:
        ike_norm = min(2.0, 1.0 + math.sqrt((peak_ike - IKE_REF_TJ) / IKE_REF_TJ))

    coastal_time_norm = min(1.0, weighted_coastal_hours / COASTAL_REF_HOURS)  # [F7]
    breadth_raw = ike_norm * coastal_time_norm * 0.20
    breadth_factor = min(_breadth_cap, breadth_raw)

    # ── Cumulative DPI ──
    # [v6] Do NOT cap at 100 here — let the raw score reflect the storm's full
    # cumulative power (Katrina ~126, Harvey ~95, Michael ~73). The compile_cache
    # compression step maps this into 0-99 display range while preserving spread.
    cum_dpi = peak_dpi * (1.0 + duration_factor + breadth_factor)

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
