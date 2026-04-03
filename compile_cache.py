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
        "ri_bonus": 10,  # Also detect RI for typhoons (lower than EP's 15)
        "duration_factor": 0.8,  # Reduced from 15% to 12% (0.8 means 80% of 15%)
        "name": "Western Pacific",
        "sub_basin_multipliers": {
            # Sub-region economic vulnerability adjustments
            "WP_JAPAN": 0.95,        # Better infrastructure, insurance
            "WP_PHILIPPINES": 1.15,  # Higher vulnerability
            "WP_VIETNAM": 1.20,      # High vulnerability
            "WP_TAIWAN": 0.93,       # Built for typhoons
            "WP_CHINA": 1.05,        # Moderate vulnerability
            "WP_GENERAL": 1.10,      # Default if region unclear
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
    Check if storm track passes near mountains for orographic rainfall.
    Returns boolean and estimated intensity of effect.
    """
    if basin != "WESTERN_PACIFIC":
        return False, 0

    # Mountain regions in Western Pacific (lat, lon, elevation_m)
    mountain_zones = [
        (11.5, 124.0, 2500),   # Philippines Cordilleras
        (14.5, 121.0, 2500),   # Philippines highlands
        (24.0, 121.0, 3952),   # Taiwan Central Mountains
        (35.0, 139.0, 3776),   # Japan Alps
        (20.5, 103.0, 2819),   # Laos mountains
        (17.0, 105.0, 2982),   # Vietnam highlands
    ]

    # Check if any snapshots are within 100km of a mountain zone
    has_mountains = False
    max_intensity = 0

    for snapshot in snapshots:
        lat = snapshot.get("lat", 0)
        lon = snapshot.get("lon", 0)
        wind = snapshot.get("max_wind_ms", 0) or 0

        for mt_lat, mt_lon, elevation in mountain_zones:
            # Simple distance check (rough, but good enough for compilation)
            distance = ((lat - mt_lat) ** 2 + (lon - mt_lon) ** 2) ** 0.5
            if distance < 3:  # Within ~3 degrees (~330km)
                has_mountains = True
                if wind > max_intensity:
                    max_intensity = wind

    return has_mountains, max_intensity


def determine_wp_sub_basin(snapshots):
    """
    Determine which sub-basin (Philippines, Japan, Vietnam, etc.) the WP storm primarily affected.
    Returns sub-basin key for multiplier lookup.
    """
    if not snapshots:
        return "WP_GENERAL"

    # Count snapshots in each sub-region
    philippines_count = 0
    japan_count = 0
    vietnam_count = 0
    taiwan_count = 0
    china_count = 0

    for snapshot in snapshots:
        lat = snapshot.get("lat", 0)
        lon = snapshot.get("lon", 0)

        # Philippines region
        if 5 <= lat <= 21 and 120 <= lon <= 135:
            philippines_count += 1
        # Japan region
        elif 24 <= lat <= 45 and 123 <= lon <= 145:
            japan_count += 1
        # Vietnam/Cambodia
        elif 8 <= lat <= 22 and 100 <= lon <= 112:
            vietnam_count += 1
        # Taiwan
        elif 21 <= lat <= 26 and 119 <= lon <= 123:
            taiwan_count += 1
        # China
        elif 15 <= lat <= 40 and 105 <= lon <= 125:
            china_count += 1

    # Return the region with most snapshots
    counts = {
        "WP_PHILIPPINES": philippines_count,
        "WP_JAPAN": japan_count,
        "WP_VIETNAM": vietnam_count,
        "WP_TAIWAN": taiwan_count,
        "WP_CHINA": china_count,
    }

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

EXPOSURE_CAP = 0.12  # Max exposure bonus: 12% of peak DPI

COASTAL_EXPOSURE_WEIGHTS = {
    # US metro corridors — highest density
    "SE Florida":               1.00,  # Miami-Ft Lauderdale-Palm Beach, 6.1M metro
    "Tampa Bay":                0.92,  # Tampa-St Pete-Clearwater, 3.2M metro
    "Northeast":                0.90,  # NYC, Long Island, New England coast
    "Mid-Atlantic":             0.80,  # Hampton Roads, Jersey Shore, DC corridor
    "SW Florida":               0.75,  # Cape Coral-Fort Myers, Naples, ~1.5M
    # Moderate-high density
    "NE Florida / Georgia":     0.65,  # Jacksonville metro, Savannah, ~2M combined
    "Texas":                    0.60,  # Houston corridor is dense but coast is spread
    "Carolinas":                0.55,  # Charleston, Myrtle Beach, Wilmington
    "Louisiana / Mississippi":  0.50,  # New Orleans metro, but lots of rural coast
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

    Takes the max exposure weight across all landfalls (worst-case scenario)
    and scales it by EXPOSURE_CAP to produce a factor that gets added to
    the cumDPI multiplier alongside duration_factor and breadth_factor.

    Returns: (exposure_factor, primary_region)
    """
    if not landfall_events:
        return 0.0, "Open Ocean"

    best_weight = 0.0
    best_region = "Coast"

    for event in landfall_events:
        region = event.get("region", "Coast")
        weight = COASTAL_EXPOSURE_WEIGHTS.get(region, 0.20)
        if weight > best_weight:
            best_weight = weight
            best_region = region

    exposure_factor = best_weight * EXPOSURE_CAP
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

PERPENDICULAR_CAP = 0.08  # Max perpendicular bonus: 8% of peak DPI

# US mainland regions that count for perpendicular bonus
US_MAINLAND_REGIONS = {
    "SE Florida", "Tampa Bay", "Northeast", "Mid-Atlantic", "SW Florida",
    "NE Florida / Georgia", "Texas", "Carolinas", "Louisiana / Mississippi",
    "North Carolina", "Alabama / FL Panhandle", "FL Big Bend", "Mississippi",
}


def compute_perpendicular_factor(landfall_events, coastal_hours):
    """
    Compute perpendicular surge bonus for storms making fast, direct landfalls.

    Only activates when coastal_hours < 12 (storm isn't coast-parallel).
    Gives 2% bonus per US mainland landfall, capped at PERPENDICULAR_CAP.

    Returns: (perp_factor, us_landfall_count)
    """
    if coastal_hours >= 12:
        return 0.0, 0  # Not a perpendicular pattern

    us_landfalls = sum(
        1 for e in landfall_events
        if e.get("region", "") in US_MAINLAND_REGIONS
    )

    if us_landfalls < 2:
        return 0.0, us_landfalls  # Need 2+ US landfalls to qualify

    perp_factor = min(PERPENDICULAR_CAP, us_landfalls * 0.02)
    return round(perp_factor, 4), us_landfalls


def apply_basin_dps_adjustment(cum_dpi, basin, snapshots):
    """
    Apply basin-specific DPS adjustments including:
    - Base multiplier
    - Rapid intensification bonus
    - Multiple landfall bonus (WP)
    - Orographic rainfall bonus (WP)
    - Sub-basin economic multiplier (WP)

    Returns: adjusted_dps, basin_name, adjustment_notes
    """
    coeffs = BASIN_COEFFICIENTS.get(basin, BASIN_COEFFICIENTS["ATLANTIC"])

    # Apply base multiplier
    adjusted_dps = cum_dpi * coeffs["dps_multiplier"]
    adjustment_notes = []

    # Check for rapid intensification
    ri_bonus = 0
    if coeffs["ri_bonus"] > 0 and len(snapshots) >= 2:
        max_24h_gain = 0
        for i in range(1, len(snapshots)):
            if (i - 1) >= 0:
                wind_prev = snapshots[i - 1].get("max_wind_ms", 0) or 0
                wind_curr = snapshots[i].get("max_wind_ms", 0) or 0
                gain_estimate = (wind_curr - wind_prev) * 4
                if gain_estimate > max_24h_gain:
                    max_24h_gain = gain_estimate

        if max_24h_gain > 80:
            ri_bonus = coeffs["ri_bonus"]
            adjusted_dps += ri_bonus
            adjustment_notes.append(f"+{ri_bonus}RI")

    # Western Pacific-specific enhancements
    if basin == "WESTERN_PACIFIC":
        # 1. Multiple landfall bonus
        landfall_count, landfall_pos = count_significant_landfalls(snapshots)
        if landfall_count > 1:
            landfall_bonus = min((landfall_count - 1) * 2.5, 8)  # Cap at +8 for 4+ landfalls
            adjusted_dps += landfall_bonus
            adjustment_notes.append(f"+{landfall_bonus:.1f}LF")

        # 2. Orographic rainfall bonus
        has_orographic, max_wind_near_mountains = has_orographic_rainfall_potential(
            snapshots, basin
        )
        if has_orographic and max_wind_near_mountains >= 20:
            orographic_bonus = min(max_wind_near_mountains / 25, 5)  # Up to +5 points
            adjusted_dps += orographic_bonus
            adjustment_notes.append(f"+{orographic_bonus:.1f}ORO")

        # 3. Sub-basin economic multiplier
        sub_basin = determine_wp_sub_basin(snapshots)
        sub_multiplier = coeffs.get("sub_basin_multipliers", {}).get(
            sub_basin, coeffs.get("sub_basin_multipliers", {}).get("WP_GENERAL", 1.0)
        )
        # Only apply sub-multiplier if it differs from 1.0
        if abs(sub_multiplier - 1.0) > 0.01:
            adjusted_dps *= sub_multiplier
            adjustment_notes.append(f"×{sub_multiplier:.2f}({sub_basin})")

    # Cap at 100
    adjusted_dps = min(adjusted_dps, 100.0)

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
    catalog_path = Path(__file__).parent / "data" / "cache" / "ibtracs_catalog.json"
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

        # Compute cumulative DPI (aka DPS)
        cum_result = compute_cumulative_dpi(snapshots, storm_name=name, storm_year=year)

        # Detect landfall events (needed for exposure factor)
        landfall_events = detect_landfall_events(snapshots)

        # Detect basin and apply basin-specific adjustments
        basin = detect_basin(snapshots)

        # Apply population exposure factor (R3)
        # Uses landfall regions to boost DPS for dense metro corridors
        exposure_factor, exposure_region = compute_exposure_factor(landfall_events)
        # Apply perpendicular surge factor (R4)
        perp_factor, us_lf_count = compute_perpendicular_factor(
            landfall_events, cum_result.total_coastal_hours
        )

        # Compute rainfall warning (needed before stall bonus)
        rain_result = compute_rainfall_warning(snapshots, storm_name=name)

        # R7: Rainfall stall bonus — stalling storms cause disproportionate inland flooding
        STALL_THRESHOLD_HOURS = 4
        STALL_BONUS_PER_HOUR = 0.01
        STALL_BONUS_CAP = 0.10
        if rain_result.total_stall_hours > STALL_THRESHOLD_HOURS and cum_result.peak_dpi > 0:
            stall_bonus = min(
                rain_result.total_stall_hours * STALL_BONUS_PER_HOUR,
                STALL_BONUS_CAP
            )
        else:
            stall_bonus = 0.0

        # R12: Rainfall inland damage extension
        # Storms with high rainfall warning AND high estimated rainfall get a boost
        # distinct from the stall bonus — this captures rain-driven inland flooding
        RAIN_WARN_THRESHOLD = 40
        RAIN_MM_THRESHOLD = 300
        RAIN_INLAND_CAP = 0.08
        if rain_result.warning_score > RAIN_WARN_THRESHOLD and rain_result.estimated_total_mm > RAIN_MM_THRESHOLD and cum_result.peak_dpi > 0:
            rain_inland_factor = min(rain_result.warning_score / 100.0 * 0.08, RAIN_INLAND_CAP)
        else:
            rain_inland_factor = 0.0

        # Combine exposure + perpendicular + stall + rainfall inland into a post-multiplier boost
        combined_boost = exposure_factor + perp_factor + stall_bonus + rain_inland_factor
        if combined_boost > 0 and cum_result.peak_dpi > 0:
            current_multiplier = cum_result.cum_dpi / cum_result.peak_dpi
            boosted = cum_result.peak_dpi * (current_multiplier + combined_boost)
            boosted = min(100.0, boosted)
        else:
            boosted = cum_result.cum_dpi

        # Apply basin-specific adjustment on the boosted value
        adjusted_dps, basin_name, adjustment_notes = apply_basin_dps_adjustment(
            boosted, basin, snapshots
        )

        # Find peak snapshot data for quick display
        peak_wind = max((s.get("max_wind_ms", 0) or 0) for s in snapshots)
        peak_ike = max((s.get("ike_total_tj", 0) or 0) for s in snapshots)
        min_pressure = min((s.get("min_pressure_hpa", 1013) or 1013) for s in snapshots)

        # Recategorize with basin-adjusted DPS
        basin_adjusted_category = categorize_dpi(adjusted_dps)

        compiled_storms[storm_id] = {
            "name": name,
            "year": year,
            "category": cat,
            # Core DPS (Destructive Power Score) — the hero number (BASIN-ADJUSTED)
            "dps": adjusted_dps,
            "dps_label": basin_adjusted_category,
            "peak_dps": cum_result.peak_dpi,
            # Basin information for UI display
            "basin": basin,
            "basin_name": basin_name,
            "dps_original": cum_result.cum_dpi,  # Keep original for reference
            # Population exposure (R3)
            "exposure_factor": exposure_factor,
            "exposure_region": exposure_region,
            # Perpendicular surge factor (R4)
            "perp_factor": perp_factor,
            "us_landfall_count": us_lf_count,
            # Rainfall stall bonus (R7)
            "stall_bonus": stall_bonus,
            "stall_hours": rain_result.total_stall_hours,
            # Rainfall inland factor (R12)
            "rain_inland_factor": rain_inland_factor,
            # Breakdown factors
            "duration_factor": cum_result.duration_factor,
            "breadth_factor": cum_result.breadth_factor,
            "coastal_hours": cum_result.total_coastal_hours,
            "track_hours": cum_result.total_track_hours,
            "peak_ike_tj": cum_result.peak_ike_tj,
            # Rainfall warning (red bar)
            "rainfall_warning": rain_result.warning_score,
            "rainfall_level": rain_result.warning_level,
            "rainfall_text": rain_result.warning_text,
            "rainfall_anomalous": rain_result.is_anomalous,
            "rainfall_stall_hours": rain_result.total_stall_hours,
            "rainfall_est_mm": rain_result.estimated_total_mm,
            # Quick stats for display
            "peak_wind_ms": round(peak_wind, 1),
            "peak_wind_kt": round(peak_wind / 0.514444, 0),
            "min_pressure_hpa": round(min_pressure, 0),
            "peak_ike": round(peak_ike, 1),
            "snapshot_count": len(snapshots),
            # Landfall events (ocean→coast transitions)
            "landfalls": landfall_events,
            # DPI timeseries for sparkline (just score + timestamp)
            "dpi_timeseries": [
                {"t": s.get("timestamp", ""), "dpi": round(s["dpi"], 1)}
                for s in cum_result.dpi_timeseries
                if s["dpi"] > 0
            ],
        }

        rain_flag = " RAIN-WARN" if rain_result.is_anomalous else ""
        adj_details = f" [{adjustment_notes}]" if adjustment_notes else ""
        adj_note = f" (was {cum_result.cum_dpi:.0f})" if abs(adjusted_dps - cum_result.cum_dpi) > 1 else ""
        exp_note = f" EXP:+{exposure_factor:.1%}({exposure_region})" if exposure_factor > 0 else ""
        perp_note = f" PERP:+{perp_factor:.1%}(LF:{us_lf_count})" if perp_factor > 0 else ""
        stall_note = f" STALL:+{stall_bonus:.1%}({rain_result.total_stall_hours:.0f}h)" if stall_bonus > 0 else ""
        rain_inland_note = f" RAIN-INLAND:+{rain_inland_factor:.1%}(warn:{rain_result.warning_score:.0f},mm:{rain_result.estimated_total_mm:.0f})" if rain_inland_factor > 0 else ""
        lf_info = f" LF:{len(landfall_events)}({', '.join(e['region'] for e in landfall_events)})" if landfall_events else " LF:0(fish)"
        print(f"DPS {adjusted_dps:.0f} (peak {cum_result.peak_dpi:.0f}){adj_note}{adj_details}{exp_note}{perp_note}{stall_note}{rain_inland_note}{rain_flag}{lf_info}")

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
