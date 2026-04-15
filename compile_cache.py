#!/usr/bin/env python3
"""
Pre-compile storm data cache for instant frontend load.

Reads preload_bundle.json and pre-computes:
  1. Per-snapshot DPS scores (using the backend DPI engine)
  2. Peak DPS / IAS / ERS for each storm
  3. Cumulative DPI (now "DPS" — Destructive Power Score)
  4. Rainfall warning scores

Outputs compiled_bundle.json — a single file the frontend loads instead of
the raw preload_bundle.json. The compiled bundle includes all pre-computed
scores so the frontend does ZERO computation on startup for preset storms.

Run this once after any formula change:
    python compile_cache.py

The output file is deterministic (same input → same output) so it can be
committed to the repo and served as a static asset.
"""

import json
import os
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from core.cumulative_dpi import compute_cumulative_dpi, categorize_dpi
from core.rainfall_warning import compute_rainfall_warning


# ============================================================================
# BASIN-SPECIFIC DPS COEFFICIENTS (v2.0)
# ============================================================================

BASIN_COEFFICIENTS = {
    "ATLANTIC": {
        "dps_multiplier": 1.0,
        "ri_bonus": 0,
        "duration_factor": 1.0,
        "name": "Atlantic",
    },
    "EASTERN_PACIFIC": {
        "dps_multiplier": 1.05,  # 5% boost for RI emphasis
        "ri_bonus": 15,  # Bonus if rapid intensification detected
        "duration_factor": 1.0,
        "name": "Eastern Pacific",
    },
    "WESTERN_PACIFIC": {
        "dps_multiplier": 1.10,  # 10% boost for large wind fields
        "ri_bonus": 15,  # Typhoons RI more often than EP storms (Haiyan, Rai, Rammasun)
        "name": "Western Pacific",
        # Sub-region vulnerability-adjusted destructive-potential multipliers.
        # These compose with COASTAL_EXPOSURE_WEIGHTS (which already handles
        # asset density) — they represent OVER/UNDER-delivery of damage
        # relative to raw wind×pressure (e.g. rainfall-driven Morakot in
        # Taiwan, surge-amplifying Mekong Delta in Vietnam).
        #
        # WP_GENERAL is 1.00 so open-ocean storms don't double-count the
        # base dps_multiplier (1.10) — previous value 1.10 compounded to
        # a silent 21% boost.
        "sub_basin_multipliers": {
            # [v7 AUDIT] WP_JAPAN 1.00 → 1.05. COASTAL_EXPOSURE_WEIGHTS
            # captures density at the landfall point, but Japanese typhoon
            # damage cascades inland (Hagibis 2019: $18B mostly from Tokyo-
            # area river flooding from a small Izu Peninsula landfall). The
            # 1.00 was a principled choice to avoid double-counting but
            # systematically underweighted the downstream impact footprint.
            "WP_JAPAN":        1.05,
            "WP_KOREA":        0.98,  # Decent resilience, moderate exposure
            "WP_PHILIPPINES":  1.15,  # High vulnerability, island-arc concentration
            # [v7 AUDIT] WP_VIETNAM 1.20 → 1.10. De-risk an untested high
            # multiplier. The 1.20 rationale (Mekong Delta surge amplification)
            # is physically plausible but not yet validated against a storm
            # where the multiplier materially changed the score — the v6
            # ceiling suppressed its effect on every storm it fired on.
            # Bring in line with WP_HAINAN (1.10) until Yagi-class validation
            # lands; revisit once rec #6 (exposure integrator) or additional
            # VN storms are in the validation set.
            "WP_VIETNAM":      1.10,
            "WP_TAIWAN":       1.00,  # Orographic bonus handles rainfall separately now
            "WP_HAINAN":       1.10,  # Dense coast + agriculture, limited hardening
            "WP_SOUTH_CHINA":  1.08,  # Guangdong / Hong Kong — massive exposure
            "WP_NORTH_CHINA":  0.98,  # Extratropical transition, lower cumulative exposure
            "WP_GENERAL":      1.00,  # Default: no extra boost beyond base multiplier
        },
    },
    "NORTH_INDIAN": {
        "dps_multiplier": 1.15,  # 15% boost for surge dominance
        "ri_bonus": 0,
        "duration_factor": 1.0,
        "name": "North Indian",
    },
    "SOUTH_INDIAN": {
        "dps_multiplier": 1.03,  # 3% slight boost
        "ri_bonus": 0,
        "duration_factor": 1.0,
        "name": "South Indian",
    },
    "SOUTH_PACIFIC": {
        "dps_multiplier": 1.0,  # Baseline
        "ri_bonus": 0,
        "duration_factor": 1.0,
        "name": "South Pacific",
    },
}


def detect_basin(snapshots):
    """
    Detect basin from storm snapshots by analyzing lat/lon distribution.
    Returns the basin identifier string.
    """
    if not snapshots:
        return "ATLANTIC"  # Default

    # Get mean position for classification
    lats = [s.get("lat", 0) for s in snapshots if "lat" in s]
    lons = [s.get("lon", 0) for s in snapshots if "lon" in s]

    if not lats or not lons:
        return "ATLANTIC"

    mean_lat = sum(lats) / len(lats)
    mean_lon = sum(lons) / len(lons)

    # Normalize longitude to -180 to 180
    norm_lon = mean_lon
    if norm_lon > 180:
        norm_lon = norm_lon - 360
    if norm_lon < -180:
        norm_lon = norm_lon + 360

    # Basin classification (using geographic boundaries)
    # Atlantic: 10°W–100°W, 0°–60°N
    if -100 <= norm_lon <= -10 and 0 <= mean_lat <= 60:
        return "ATLANTIC"

    # Eastern Pacific: 100°W–140°W, 0°–35°N
    if -140 <= norm_lon <= -100 and 0 <= mean_lat <= 35:
        return "EASTERN_PACIFIC"

    # Western Pacific: 100°E–180°E, 0°–60°N
    if 100 <= norm_lon <= 180 and 0 <= mean_lat <= 60:
        return "WESTERN_PACIFIC"

    # North Indian: 30°E–100°E, 0°–35°N
    if 30 <= norm_lon <= 100 and 0 <= mean_lat <= 35:
        return "NORTH_INDIAN"

    # South Indian: 20°E–120°E, -40°–0°
    if 20 <= norm_lon <= 120 and -40 <= mean_lat < 0:
        return "SOUTH_INDIAN"

    # South Pacific: 120°E–120°W (wraps around dateline), -50°–0°
    if (norm_lon >= 120 or norm_lon <= -120) and -50 <= mean_lat < 0:
        return "SOUTH_PACIFIC"

    # Default to Atlantic
    return "ATLANTIC"


def count_significant_landfalls(snapshots):
    """
    Count significant landfalls by detecting ocean→land transitions.
    Returns count and list of landfall coordinates.
    (Legacy wrapper — use detect_landfall_events for rich data.)
    """
    events = detect_landfall_events(snapshots)
    return len(events), [(e["lat"], e["lon"]) for e in events]


# Named coastal boxes with region labels — order matters (more specific first)
COASTAL_REGIONS = [
    # US — specific sub-regions first
    (17.0, 19.5, -68.0, -64.0, "Puerto Rico / USVI"),
    (25.0, 26.5, -82.5, -79.5, "SW Florida"),
    (26.5, 27.5, -83.0, -81.5, "Tampa Bay"),
    (25.0, 27.0, -80.5, -79.5, "SE Florida"),
    (28.5, 30.5, -85.0, -82.0, "FL Big Bend"),
    (27.0, 31.0, -82.0, -79.5, "NE Florida / Georgia"),
    # Gulf coast metro sub-regions — checked before the broader LA/MS/AL boxes
    (28.0, 30.2, -91.5, -89.0, "New Orleans"),       # New Orleans metro / coastal parishes / Lake Pontchartrain
    (30.2, 30.6, -89.5, -88.7, "Biloxi / Gulfport"), # Biloxi-Gulfport metro
    (30.1, 31.0, -88.3, -87.7, "Mobile"),             # Mobile Bay metro
    (28.0, 30.5, -94.0, -88.0, "Louisiana / Mississippi"),
    (30.5, 31.0, -90.0, -88.0, "Mississippi"),
    (24.5, 30.5, -98.0, -94.0, "Texas"),
    (30.0, 31.5, -88.0, -85.0, "Alabama / FL Panhandle"),
    (31.0, 34.0, -82.0, -75.0, "Carolinas"),
    (34.0, 36.5, -78.0, -75.0, "North Carolina"),
    (36.5, 40.0, -77.0, -73.0, "Mid-Atlantic"),
    (40.0, 42.0, -74.0, -70.0, "Northeast"),
    # Caribbean / Atlantic islands
    (23.0, 27.5, -80.0, -72.0, "Bahamas"),
    (17.5, 19.0, -73.0, -68.0, "Hispaniola"),
    (14.0, 18.5, -62.0, -59.0, "Leeward Islands"),
    (12.0, 14.0, -62.0, -59.0, "Windward Islands"),
    (17.5, 22.5, -85.0, -74.0, "Cuba / Jamaica"),
    # Central America / Mexico
    (14.0, 23.0, -98.0, -85.0, "Mexico / Central America"),
    # Western Pacific
    (5, 21, 120, 135, "Philippines"),
    (20, 25, 115, 122, "Vietnam / Cambodia"),
    (21, 26, 119, 123, "Taiwan"),
    (24, 45, 123, 145, "Japan"),
    (15, 25, 105, 122, "Thailand / Laos"),
    (15, 40, 105, 125, "China"),
]


def detect_landfall_events(snapshots):
    """
    Detect landfall events with rich metadata.
    Returns list of dicts: {timestamp, lat, lon, region, snapshot_idx, max_wind_ms, min_pressure_hpa}
    Each event is an ocean→coast transition with wind >= 20 m/s.
    """

    def classify_region(lat, lon):
        for lat_min, lat_max, lon_min, lon_max, name in COASTAL_REGIONS:
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                return name
        return "Coast"

    def is_near_coast(lat, lon):
        for lat_min, lat_max, lon_min, lon_max, _ in COASTAL_REGIONS:
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                return True
        return False

    events = []
    prev_near_coast = False

    for idx, snapshot in enumerate(snapshots):
        lat = snapshot.get("lat", 0)
        lon = snapshot.get("lon", 0)
        near_coast = is_near_coast(lat, lon)
        wind = snapshot.get("max_wind_ms", 0) or 0

        if near_coast and not prev_near_coast and wind >= 20:
            events.append({
                "timestamp": snapshot.get("timestamp", ""),
                "lat": round(lat, 2),
                "lon": round(lon, 2),
                "region": classify_region(lat, lon),
                "snapshot_idx": idx,
                "max_wind_ms": round(wind, 1),
                "min_pressure_hpa": round(snapshot.get("min_pressure_hpa", 1013) or 1013, 0),
            })

        prev_near_coast = near_coast

    return events


def has_orographic_rainfall_potential(snapshots, basin):
    """
    Check if storm track passes close enough to mountains to trigger
    orographic rainfall enhancement. Returns (bool, peak_wind_ms_near).

    Activation requires a snapshot within ~110 km (1.0°) of a mountain
    range. The old threshold of 3° (~330 km) was generous enough that
    storms 300+ km offshore — never making landfall — still got the
    full orographic bonus. Orographic rainfall enhancement in tropical
    cyclones requires the low-level circulation to actually interact
    with terrain, which is a near-landfall / inland process.
    """
    if basin != "WESTERN_PACIFIC":
        return False, 0

    # Mountain regions in Western Pacific (lat, lon, elevation_m)
    mountain_zones = [
        (11.5, 124.0, 2500),   # Philippines Cordilleras
        (14.5, 121.0, 2500),   # Philippines highlands (Sierra Madre)
        (24.0, 121.0, 3952),   # Taiwan Central Mountains
        (35.0, 139.0, 3776),   # Japan Alps
        (20.5, 103.0, 2819),   # Laos mountains
        (17.0, 105.0, 2982),   # Vietnam highlands (Annamite Range)
        (37.5, 128.0, 1638),   # Korea Taebaek Range
    ]

    has_mountains = False
    max_intensity = 0

    # ~110 km at these latitudes is roughly 1.0 degree. Require a true
    # terrain encounter rather than an offshore pass.
    RADIUS_DEG = 1.0

    for snapshot in snapshots:
        lat = snapshot.get("lat", 0)
        lon = snapshot.get("lon", 0)
        wind = snapshot.get("max_wind_ms", 0) or 0

        for mt_lat, mt_lon, elevation in mountain_zones:
            # Latitude-corrected Euclidean distance in degrees
            import math as _m
            mean_lat_rad = _m.radians((lat + mt_lat) / 2.0)
            dlat = lat - mt_lat
            dlon = (lon - mt_lon) * _m.cos(mean_lat_rad)
            distance = _m.hypot(dlat, dlon)
            if distance < RADIUS_DEG:
                has_mountains = True
                if wind > max_intensity:
                    max_intensity = wind

    return has_mountains, max_intensity


def determine_wp_sub_basin(snapshots):
    """
    Determine which sub-basin the WP storm primarily affected.
    Returns a sub-basin key for multiplier lookup.

    Rewritten from the previous first-match elif chain that had overlapping
    boxes (Philippines 5-21N / Japan 24-45N / Taiwan 21-26N / China 15-40N
    all partially overlapped). We now tally every snapshot against every
    region's box and pick the region with the most snapshots, so overlap
    regions count toward both candidates and the decision is density-based,
    not match-order-based.
    """
    if not snapshots:
        return "WP_GENERAL"

    # Region bounding boxes. Tighter than the previous version and ordered
    # from most specific to least specific; a snapshot can count toward
    # multiple regions — the tally picks the winner.
    regions = [
        # key,              lat_min, lat_max, lon_min, lon_max
        ("WP_TAIWAN",       21.5,   25.5,    119.5,   122.5),
        ("WP_PHILIPPINES",   5.0,   20.0,    117.0,   127.0),
        ("WP_VIETNAM",       8.0,   22.0,    102.0,   112.0),
        ("WP_HAINAN",       17.5,   20.5,    108.0,   111.5),
        ("WP_SOUTH_CHINA",  20.0,   26.0,    108.0,   118.0),  # Guangdong, Hong Kong, Fujian
        ("WP_NORTH_CHINA",  26.0,   41.0,    117.0,   124.5),  # Shandong, Jiangsu, Bohai
        ("WP_KOREA",        33.0,   39.0,    124.5,   131.5),
        ("WP_JAPAN",        24.0,   45.5,    128.0,   146.0),
    ]

    counts = {key: 0 for key, *_ in regions}
    for snapshot in snapshots:
        lat = snapshot.get("lat", 0)
        lon = snapshot.get("lon", 0)
        for key, lat_min, lat_max, lon_min, lon_max in regions:
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                counts[key] += 1

    max_region = max(counts, key=counts.get)
    return max_region if counts[max_region] > 0 else "WP_GENERAL"


# ============================================================================
# POPULATION EXPOSURE WEIGHTS (R3 — Coastal Asset Density)
# ============================================================================
# Each coastal region gets a relative population/asset density weight (0–1.0).
# Storms making landfall in dense metro corridors get a DPS boost reflecting
# the disproportionate threat to life and property.
#
# Weights are calibrated against FEMA National Structure Inventory (NSI) counts
# per km of coastline, normalized to the densest corridor (SE Florida = 1.0).
#
# The exposure factor is computed as:
#   exposure_factor = max(weights at all landfalls) × EXPOSURE_CAP
#
# This means a storm that hits Tampa Bay AND rural FL Big Bend gets the
# Tampa Bay weight (the worst-case landfall drives the score).

EXPOSURE_CAP = 0.10  # [F3] Max exposure bonus: 10% of peak DPI (reduced from 20% — was over-inflating all major storms into 87-89 band)

COASTAL_EXPOSURE_WEIGHTS = {
    # US metro corridors — highest density
    "SE Florida":               1.00,  # Miami-Ft Lauderdale-Palm Beach, 6.1M metro
    "Tampa Bay":                0.65,  # Tampa-St Pete-Clearwater, 3.2M metro
    "Northeast":                0.90,  # NYC, Long Island, New England coast
    "Mid-Atlantic":             0.80,  # Hampton Roads, Jersey Shore, DC corridor
    "SW Florida":               0.75,  # Cape Coral-Fort Myers, Naples, ~1.5M
    # Moderate-high density
    "NE Florida / Georgia":     0.65,  # Jacksonville metro, Savannah, ~2M combined
    "Texas":                    0.60,  # Houston corridor is dense but coast is spread
    "Carolinas":                0.55,  # Charleston, Myrtle Beach, Wilmington
    "New Orleans":              0.65,  # New Orleans metro — dense, below sea level, extreme surge risk
    "Biloxi / Gulfport":       0.65,  # Biloxi-Gulfport — dense barrier island coast, high surge exposure
    "Mobile":                  0.65,  # Mobile Bay — funnel geometry amplifies surge significantly
    "Louisiana / Mississippi":  0.60,  # Broader rural LA/MS coast
    "North Carolina":           0.45,  # Outer Banks, Morehead City — lower density
    # Lower density
    "Alabama / FL Panhandle":   0.35,  # Pensacola, Panama City — smaller metros
    "Puerto Rico / USVI":       0.40,  # San Juan metro, but smaller economy
    "FL Big Bend":              0.15,  # Very rural — Cedar Key, Steinhatchee
    "Mississippi":              0.30,  # Biloxi-Gulfport, small metro
    # Caribbean / international
    "Bahamas":                  0.08,  # Small population, low asset base
    "Hispaniola":               0.25,
    "Leeward Islands":          0.15,
    "Windward Islands":         0.12,
    "Cuba / Jamaica":           0.20,
    "Mexico / Central America": 0.20,
    # Western Pacific
    "Philippines":              0.35,
    "Vietnam / Cambodia":       0.30,
    "Taiwan":                   0.55,
    "Japan":                    0.80,
    "Thailand / Laos":          0.25,
    "China":                    0.50,
    # Default
    "Coast":                    0.20,
}


def compute_exposure_factor(landfall_events):
    """
    Compute population exposure factor from landfall events.

    [F6] If the strongest landfall is on US mainland, use that region's
    weight (the old max-based approach). But if the strongest landfall is
    in the Caribbean (Bahamas, PR/USVI, etc.), use intensity-weighted
    blending — this prevents storms like Dorian (Cat 5 Bahamas, Cat 1
    US brush) from getting the high US-mainland exposure weight, while
    preserving correct scores for Harvey (TS Windward → Cat 4 Texas).

    Returns: (exposure_factor, primary_region)
    """
    if not landfall_events:
        return 0.0, "Open Ocean"

    # Find the strongest landfall (by wind)
    strongest_event = max(landfall_events,
                          key=lambda e: e.get("max_wind_ms", 0) or 0)
    strongest_region = strongest_event.get("region", "Coast")

    # If the strongest landfall is on US mainland, use max exposure weight
    # across US mainland landfalls (traditional approach — Harvey, Ian, etc.)
    if strongest_region in US_MAINLAND_REGIONS:
        best_weight = 0.0
        best_region = "Coast"
        for event in landfall_events:
            region = event.get("region", "Coast")
            if region in US_MAINLAND_REGIONS:
                weight = COASTAL_EXPOSURE_WEIGHTS.get(region, 0.20)
                if weight > best_weight:
                    best_weight = weight
                    best_region = region
        exposure_factor = best_weight * EXPOSURE_CAP
        return round(exposure_factor, 4), best_region

    # Strongest landfall is Caribbean/non-US: use intensity-weighted blend.
    # This naturally discounts when the main impact is offshore (Dorian).
    weighted_sum = 0.0
    total_weight = 0.0
    best_region = strongest_region

    for event in landfall_events:
        region = event.get("region", "Coast")
        econ_weight = COASTAL_EXPOSURE_WEIGHTS.get(region, 0.20)
        wind_ms = event.get("max_wind_ms", 33.0) or 33.0
        intensity = wind_ms ** 2
        weighted_sum += econ_weight * intensity
        total_weight += intensity

    blended_weight = weighted_sum / total_weight if total_weight > 0 else 0.20
    exposure_factor = blended_weight * EXPOSURE_CAP
    return round(exposure_factor, 4), best_region


# ============================================================================
# PERPENDICULAR SURGE FACTOR (R4)
# ============================================================================
# Storms with multiple landfalls but zero/low coastal hours are making fast,
# perpendicular crossings — direct hits that the duration/breadth formula
# misses because it rewards coast-parallel tracking.
#
# Perpendicular landfalls into concave coastlines (bays, inlets, sounds)
# can produce severe surge funneling (e.g., Eta into Tampa Bay approach,
# Claudette crossing Louisiana to NC).
#
# Factor: perp_bonus = landfalls_at_us_coasts * 0.02 (2% per US landfall)
#   - Only counts US mainland landfalls (not Caribbean pass-throughs)
#   - Only activates if coastal_hours < 12 (truly perpendicular)
#   - Capped at PERPENDICULAR_CAP

PERPENDICULAR_CAP = 0.03  # Max perpendicular bonus: 3% of peak DPI (reduced from 8%)

# US mainland regions that count for perpendicular bonus
US_MAINLAND_REGIONS = {
    "SE Florida", "Tampa Bay", "Northeast", "Mid-Atlantic", "SW Florida",
    "NE Florida / Georgia", "Texas", "Carolinas", "Louisiana / Mississippi",
    "North Carolina", "Alabama / FL Panhandle", "FL Big Bend", "Mississippi",
    "New Orleans", "Biloxi / Gulfport", "Mobile",
}


def compute_perpendicular_factor(landfall_events, coastal_hours):
    """
    Compute perpendicular surge bonus for storms making fast, direct landfalls.

    [F10] Relaxed from original: coastal_hours < 24 (was 12), us_landfalls >= 1
    (was 2). Many destructive storms make a single perpendicular hit (Michael
    into FL Panhandle, Helene into FL Big Bend) with limited coastal hours.
    Gives 3% bonus per US mainland landfall, capped at PERPENDICULAR_CAP.

    Returns: (perp_factor, us_landfall_count)
    """
    if coastal_hours >= 24:
        return 0.0, 0  # Not a perpendicular pattern

    us_landfalls = sum(
        1 for e in landfall_events
        if e.get("region", "") in US_MAINLAND_REGIONS
    )

    if us_landfalls < 1:
        return 0.0, us_landfalls  # Need at least 1 US landfall

    perp_factor = min(PERPENDICULAR_CAP, us_landfalls * 0.03)
    return round(perp_factor, 4), us_landfalls


def apply_basin_dps_adjustment(cum_dpi, basin, snapshots,
                               duration_factor=None, breadth_factor=None):
    """
    Apply basin-specific DPS adjustments including:
    - Base multiplier
    - Rapid intensification bonus
    - Multiple landfall bonus (WP)
    - Orographic rainfall bonus (WP)
    - Rainfall-footprint bonus (WP, v8)
    - Sub-basin economic multiplier (WP)

    `duration_factor` / `breadth_factor` are the per-storm 0..CAP values
    from `compute_cumulative_dpi`. They drive the v8-audit rainfall-
    footprint proxy (rec #5) in rainfall-prone WP sub-basins. Passed
    as None for back-compat with callers that don't yet thread them
    through — in which case the rainfall bonus is skipped.

    Returns: adjusted_dps, basin_name, adjustment_notes
    """
    coeffs = BASIN_COEFFICIENTS.get(basin, BASIN_COEFFICIENTS["ATLANTIC"])

    # [v7 AUDIT] Apply base multiplier, then (for WP) the sub-basin
    # multiplier, BEFORE adding the additive RI / LF / ORO bonuses. The
    # previous v6 order applied the sub-basin multiplier last — which
    # meant the bonuses were silently amplified by 1.20x in WP_VIETNAM
    # and 0.98x in WP_NORTH_CHINA. Applying the multiplier to the base
    # keeps the bonuses at absolute (basin-comparable) points.
    adjusted_dps = cum_dpi * coeffs["dps_multiplier"]
    adjustment_notes = []

    sub_basin = None
    if basin == "WESTERN_PACIFIC":
        sub_basin = determine_wp_sub_basin(snapshots)
        sub_multiplier = coeffs.get("sub_basin_multipliers", {}).get(
            sub_basin, coeffs.get("sub_basin_multipliers", {}).get("WP_GENERAL", 1.0)
        )
        if abs(sub_multiplier - 1.0) > 0.01:
            adjusted_dps *= sub_multiplier
            adjustment_notes.append(f"×{sub_multiplier:.2f}({sub_basin})")

    # Check for rapid intensification.
    #
    # Standard NHC/JTWC definition of RI: sustained wind increase of at least
    # 30 knots in 24 hours (~15.4 m/s / 24h). Snapshots are 6-hourly so we
    # scale the single-step delta by 4 to get a 24h-equivalent gain.
    #
    # Previous threshold was `> 80` (m/s per 24h = ~155 kt/24h), which is
    # physically unreachable — even Haiyan 2013 peaked at ~80 kt/24h = ~41
    # m/s/24h. The old bonus never fired on any real storm.
    #
    # [v7 AUDIT] RI bonus is now scaled to the 24h gain magnitude, instead
    # of a flat +15 fired whenever the threshold was crossed. A borderline
    # RI event (~16 m/s / 24h) previously earned the same bonus as a
    # violent RI event (~43 m/s / 24h — Surigae 2021 or Patricia 2015).
    # The new schedule ranges +5 (threshold) to +20 (physical ceiling):
    #
    #     ri_bonus = base_bonus_scale × (5 + 15 × clip((ΔV − 15.4) / 30, 0, 1))
    #
    # base_bonus_scale is `coeffs["ri_bonus"] / 15` so basins that previously
    # set ri_bonus = 15 keep the same expected magnitude mid-range.
    RI_THRESHOLD_MS_PER_24H = 15.4  # 30 kt/24h — standard RI definition
    RI_MAX_SCALE_MS_PER_24H = 45.0  # ~87 kt/24h — near the physical ceiling
    ri_bonus = 0
    if coeffs["ri_bonus"] > 0 and len(snapshots) >= 2:
        max_24h_gain = 0
        for i in range(1, len(snapshots)):
            wind_prev = snapshots[i - 1].get("max_wind_ms", 0) or 0
            wind_curr = snapshots[i].get("max_wind_ms", 0) or 0
            gain_estimate = (wind_curr - wind_prev) * 4  # m/s per 24h
            if gain_estimate > max_24h_gain:
                max_24h_gain = gain_estimate

        if max_24h_gain > RI_THRESHOLD_MS_PER_24H:
            excess = max_24h_gain - RI_THRESHOLD_MS_PER_24H
            scale  = min(excess / (RI_MAX_SCALE_MS_PER_24H - RI_THRESHOLD_MS_PER_24H), 1.0)
            base_scale = coeffs["ri_bonus"] / 15.0  # preserve historical magnitude
            ri_bonus = round(base_scale * (5.0 + 15.0 * scale), 1)
            adjusted_dps += ri_bonus
            adjustment_notes.append(f"+{ri_bonus}RI")

    # Western Pacific-specific enhancements (additive bonuses)
    if basin == "WESTERN_PACIFIC":
        # 1. Multiple landfall bonus
        landfall_count, landfall_pos = count_significant_landfalls(snapshots)
        if landfall_count > 1:
            landfall_bonus = min((landfall_count - 1) * 2.5, 8)  # Cap at +8 for 4+ landfalls
            adjusted_dps += landfall_bonus
            adjustment_notes.append(f"+{landfall_bonus:.1f}LF")

        # 2. Orographic rainfall bonus.
        #    Taiwan/Luzon/Vietnam mountains can drive rainfall-dominant
        #    damage profiles (Morakot 2009 ≈ 3000 mm over 48h → $3B damage
        #    at Cat 2 landfall). The old cap of +5 was too low for what
        #    can be the single largest driver of total damage; raised to
        #    +9 with a steeper slope on the wind scaling.
        has_orographic, max_wind_near_mountains = has_orographic_rainfall_potential(
            snapshots, basin
        )
        if has_orographic and max_wind_near_mountains >= 20:
            orographic_bonus = min(max_wind_near_mountains / 18, 9)
            adjusted_dps += orographic_bonus
            adjustment_notes.append(f"+{orographic_bonus:.1f}ORO")

        # 3. [v8 AUDIT rec #5] Rainfall-footprint proxy.
        #    Orographic rewards *peak wind at terrain*, but misses the
        #    rain-volume-over-time signal that a slow, broad storm
        #    generates in rainfall-prone sub-basins (JP Kanto stall,
        #    VN Red River delta soak, TW central mountains, S.China
        #    Fujian inland flood). The audit showed Doksuri 2023 —
        #    whose remnants drove the worst Beijing/Hebei floods in
        #    140 years, $28.5B damage — scored BELOW Goni (intense,
        #    sparse, $0.4B) in every variant without this term.
        #
        #    Proxy form: +6 × duration_factor × breadth_factor, gated
        #    on rainfall-prone sub-basins. duration_factor and
        #    breadth_factor come from the cumulative_dpi pipeline
        #    (both 0..CAP, CAP=0.10 each so each factor is 0..1 when
        #    normalised). Max contribution +6 pts for a storm that
        #    saturates both duration and breadth.
        #
        #    Principled version (future): use QPE tiles or JMA
        #    rainfall analysis around the track — see design doc
        #    EXPOSURE_INTEGRATOR_DESIGN.md §9 follow-ups.
        RAINFALL_PRONE = {"WP_JAPAN", "WP_SOUTH_CHINA",
                          "WP_VIETNAM", "WP_TAIWAN"}
        if (sub_basin in RAINFALL_PRONE
                and duration_factor is not None
                and breadth_factor is not None):
            # duration_factor, breadth_factor are each already on 0..~0.10
            # scale from the DURATION_CAP/BREADTH_CAP in cumulative_dpi.
            # Normalise by their caps so each is a 0..1 fraction, then
            # cross-multiply.
            _DUR_CAP = 0.10
            _BRD_CAP = 0.10
            dfrac = min(duration_factor / _DUR_CAP, 1.0)
            bfrac = min(breadth_factor / _BRD_CAP, 1.0)
            rainfall_bonus = 6.0 * dfrac * bfrac
            if rainfall_bonus > 0.1:
                adjusted_dps += rainfall_bonus
                adjustment_notes.append(f"+{rainfall_bonus:.1f}RAIN")

        # 4. [v7 AUDIT] No-landfall dampener.
        #    An intensity-extreme open-ocean typhoon (Surigae 2021, Nepartak
        #    2016 recurves, etc.) has real destructive potential but did
        #    not realize it. The formula without this penalty scored pure
        #    open-ocean Cat 5s identically to major landfall events — the
        #    WP leaderboard was historically polluted with ~8 intensity-
        #    extreme misses ranked alongside Haiyan-class disasters.
        #
        #    Apply a 0.60 dampener when the storm never made a significant
        #    landfall. This is applied LAST so it dampens the full score
        #    (base × sub-basin + RI + LF + ORO).
        if landfall_count == 0:
            adjusted_dps *= 0.60
            adjustment_notes.append("×0.60(no-landfall)")

    # [v7 AUDIT] Sqrt compression retuned from (T=60, S=4) to (T=70, S=2.5).
    #
    # Rationale: the previous curve was calibrated so "raw 140 → 95", but
    # in practice a Cat 4+ WP typhoon with RI + LF + ORO + sub-basin
    # multiplier lands at pre-compression 180–220, which the old curve
    # clamped to the 99 ceiling for effectively every major storm. The
    # retuned curve moves the elbow up and slows the rise so that:
    #
    #     raw  80 → 78     raw 150 → 84     raw 220 →  99 (capped)
    #     raw 100 → 83     raw 180 → 96
    #     raw 140 → 92     raw 200 → 99 (capped)
    #
    # This preserves the design intent (99 is unreachable except by
    # truly extreme storms) while restoring discrimination in the 90–98
    # band, where every major WP typhoon used to pile up at 99.
    import math as _m
    _T = 70.0   # Compression threshold
    _S = 2.5    # Spread factor (gentler than v6 so top-tier storms spread out)
    if adjusted_dps > _T:
        adjusted_dps = _T + _S * _m.sqrt(adjusted_dps - _T)
    adjusted_dps = min(adjusted_dps, 99.0)  # Hard ceiling (no storm is "perfect 100")

    return adjusted_dps, coeffs["name"], ", ".join(adjustment_notes)


# Storm metadata — hardcoded for preset storms, auto-detected for others
STORM_META = {
    "AL122005": {"name": "Katrina", "year": 2005, "cat": 5},
    "AL112017": {"name": "Irma", "year": 2017, "cat": 5},
    "AL092024": {"name": "Helene", "year": 2024, "cat": 4},
    "AL092008": {"name": "Ike", "year": 2008, "cat": 4},
    "AL092022": {"name": "Ian", "year": 2022, "cat": 4},
    "AL062018": {"name": "Florence", "year": 2018, "cat": 4},
    "AL052019": {"name": "Dorian", "year": 2019, "cat": 5},
    "AL182012": {"name": "Sandy", "year": 2012, "cat": 3},
    "AL142024": {"name": "Milton", "year": 2024, "cat": 5},
    "AL152017": {"name": "Maria", "year": 2017, "cat": 5},
    "AL142018": {"name": "Michael", "year": 2018, "cat": 5},
    "AL092017": {"name": "Harvey", "year": 2017, "cat": 4},
    "AL102023": {"name": "Idalia", "year": 2023, "cat": 3},
    "AL022024": {"name": "Beryl", "year": 2024, "cat": 5},
}

# IBTrACS SID → AL ID mapping for duplicate detection
# These IBTrACS storms are the same as existing AL-format preset storms
IBTRACS_TO_AL_DUPLICATES = {
    "2022266N12294": "AL092022",  # Ian
    "2024268N17278": "AL092024",  # Helene
}


def _wind_kt_to_category(wind_kt):
    """Convert peak wind (knots) to Saffir-Simpson category."""
    if wind_kt >= 137: return 5
    if wind_kt >= 113: return 4
    if wind_kt >= 96: return 3
    if wind_kt >= 83: return 2
    if wind_kt >= 64: return 1
    return 0


_CATALOG_CACHE = None

def _load_catalog():
    """Load and cache the IBTrACS catalog for name lookups."""
    global _CATALOG_CACHE
    if _CATALOG_CACHE is not None:
        return _CATALOG_CACHE
    
    # Use Railway persistent volume when PERSISTENT_DATA_DIR is set
    persistent_dir = Path(os.environ.get("PERSISTENT_DATA_DIR", str(Path(__file__).parent / "data")))
    catalog_path = persistent_dir / "cache" / "ibtracs_catalog.json"
    
    if catalog_path.exists():
        try:
            with open(catalog_path) as f:
                _CATALOG_CACHE = json.load(f)
        except (json.JSONDecodeError, KeyError):
            _CATALOG_CACHE = []
    else:
        _CATALOG_CACHE = []
    return _CATALOG_CACHE


def _auto_detect_meta(storm_id, snapshots):
    """Auto-detect storm metadata from snapshot data and IBTrACS catalog cache."""
    catalog = _load_catalog()

    # Try direct match by storm_id (works for IBTrACS SIDs)
    for entry in catalog:
        if entry.get("id") == storm_id:
            return {
                "name": entry.get("name", storm_id),
                "year": entry.get("year", 0),
                "cat": entry.get("category", 0),
            }

    # For ATCF IDs (AL092022 etc.), look up via the SID in the snapshot data
    # The snapshots' storm_id field contains the IBTrACS SID
    if snapshots and storm_id.startswith("AL"):
        snapshot_sid = snapshots[0].get("storm_id", "")
        if snapshot_sid and snapshot_sid != storm_id:
            for entry in catalog:
                if entry.get("id") == snapshot_sid:
                    return {
                        "name": entry.get("name", storm_id),
                        "year": entry.get("year", 0),
                        "cat": entry.get("category", 0),
                    }

    # Fallback: extract from snapshot data
    year = 0
    peak_wind_ms = 0
    if snapshots:
        ts = snapshots[0].get("timestamp", "")
        if ts:
            try:
                year = int(ts[:4])
            except ValueError:
                pass
        peak_wind_ms = max((s.get("max_wind_ms", 0) or 0) for s in snapshots)

    peak_kt = peak_wind_ms / 0.514444 if peak_wind_ms else 0
    cat = _wind_kt_to_category(peak_kt)

    return {"name": storm_id, "year": year, "cat": cat}


def compile():
    t0 = time.time()

    bundle_path = Path(__file__).parent / "frontend" / "preload_bundle.json"
    output_path = Path(__file__).parent / "frontend" / "compiled_bundle.json"

    print(f"Reading {bundle_path}...")
    with open(bundle_path) as f:
        raw = json.load(f)

    storms_raw = raw.get("storms", {})
    print(f"Found {len(storms_raw)} storms in bundle")

    # Track AL storm name+year to skip IBTrACS duplicates
    al_name_years = set()
    for sid, meta in STORM_META.items():
        al_name_years.add(f"{meta['name'].upper()}_{meta['year']}")

    # Pre-scan: gather name+year for all AL-format storms so we skip their IBTrACS SID duplicates
    for storm_id, snapshots in storms_raw.items():
        if storm_id.startswith("AL"):
            meta = STORM_META.get(storm_id) or _auto_detect_meta(storm_id, snapshots)
            nyk = f"{meta['name'].upper()}_{meta['year']}"
            al_name_years.add(nyk)

    compiled_storms = {}

    for storm_id, snapshots in storms_raw.items():
        # Skip known IBTrACS duplicates of existing AL storms
        if storm_id in IBTRACS_TO_AL_DUPLICATES:
            al_id = IBTRACS_TO_AL_DUPLICATES[storm_id]
            print(f"  Skipping {storm_id} (duplicate of {al_id})")
            continue

        # Get metadata — hardcoded for presets, auto-detected for others
        if storm_id in STORM_META:
            meta = STORM_META[storm_id]
        else:
            meta = _auto_detect_meta(storm_id, snapshots)
            # Skip IBTrACS SID entries if the same name+year exists from an AL entry
            # (Only applies to non-AL IDs like "2022266N12294")
            if not storm_id.startswith("AL") and not storm_id.startswith("EP"):
                nyk = f"{meta['name'].upper()}_{meta['year']}"
                if nyk in al_name_years:
                    print(f"  Skipping {storm_id} ({meta['name']} {meta['year']}) — already have AL version")
                    continue
        name = meta["name"]
        year = meta["year"]
        cat = meta["cat"]

        print(f"  Computing {name} ({year})...", end=" ", flush=True)

        # Delegate the entire per-storm DPS computation to the unified engine.
        # This is THE single source of truth — also called by api/routes.py for
        # ad-hoc storms, guaranteeing hero/accordion/map all see identical numbers.
        from core.dps_engine import compute_storm_dps
        bundle = compute_storm_dps(
            storm_id=storm_id,
            snapshots=snapshots,
            storm_name=name,
            storm_year=year,
            category_hint=cat,
        )
        compiled_storms[storm_id] = bundle

        # Log line (pull values from the returned bundle)
        rain_flag = " RAIN-WARN" if bundle["rainfall_anomalous"] else ""
        adj_details = f" [{bundle['adjustment_notes']}]" if bundle.get("adjustment_notes") else ""
        adj_note = f" (was {bundle['dps_original']:.0f})" if abs(bundle["dps"] - bundle["dps_original"]) > 1 else ""
        exp_note = f" EXP:+{bundle['exposure_factor']:.1%}({bundle['exposure_region']})" if bundle["exposure_factor"] > 0 else ""
        perp_note = f" PERP:+{bundle['perp_factor']:.1%}(LF:{bundle['us_landfall_count']})" if bundle["perp_factor"] > 0 else ""
        stall_note = f" STALL:+{bundle['stall_bonus']:.1%}({bundle['stall_hours']:.0f}h)" if bundle["stall_bonus"] > 0 else ""
        rain_inland_note = f" RAIN-INLAND:+{bundle['rain_inland_factor']:.1%}(warn:{bundle['rainfall_warning']:.0f},mm:{bundle['rainfall_est_mm']:.0f})" if bundle["rain_inland_factor"] > 0 else ""
        inland_note = f" INLAND:+{bundle['inland_pen_factor']:.1%}" if bundle["inland_pen_factor"] > 0 else ""
        lf_events = bundle["landfalls"]
        lf_info = f" LF:{len(lf_events)}({', '.join(e['region'] for e in lf_events)})" if lf_events else " LF:0(fish)"
        cat_note = f" Cat:{bundle['category']}" + (f"(life:{bundle['category_lifetime']})" if bundle['category'] != bundle['category_lifetime'] else "")
        print(f"DPS {bundle['dps']:.1f} (peak {bundle['peak_dps']:.0f}){adj_note}{adj_details}{exp_note}{perp_note}{stall_note}{rain_inland_note}{inland_note}{rain_flag}{cat_note}{lf_info}")

    # Build output
    output = {
        "version": "v4-basin-specific",
        "compiled_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "description": "Includes basin-specific DPS adjustments for Atlantic, Eastern Pacific, Western Pacific, North Indian, South Indian, and South Pacific basins",
        "storm_count": len(compiled_storms),
        "storms": compiled_storms,
        # Raw snapshots still included for full visualization (map, scrubber, charts)
        "raw_snapshots": storms_raw,
    }

    # Write output
    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = os.path.getsize(output_path) / 1024
    elapsed = time.time() - t0

    print(f"\nCompiled {len(compiled_storms)} storms in {elapsed:.1f}s")
    print(f"Output: {output_path} ({size_kb:.0f} KB)")

    # Summary table
    print(f"\n{'Storm':<12} {'Basin':<14} {'DPS':>5} {'Peak':>5} {'Rain':>5} {'Level':<10}")
    print("-" * 65)
    ranked = sorted(compiled_storms.items(), key=lambda x: -x[1]["dps"])
    for sid, s in ranked:
        rain_mark = "*" if s["rainfall_anomalous"] else " "
        basin_display = s.get("basin_name", "Atlantic")
        print(f"{s['name']:<12} {basin_display:<14} {s['dps']:>5.0f} {s['peak_dps']:>5.1f} {s['rainfall_warning']:>5.1f}{rain_mark} {s['rainfall_level']:<10}")


if __name__ == "__main__":
    compile()
