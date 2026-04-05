"""
SurgeDPS FastAPI Router
Ported from SurgeDPS/scripts/api_server.py (HTTPServer) → FastAPI.

Mounted at /surgedps/api in main.py.
All heavy work (cell load, activate) runs in a thread pool so the
async event loop stays unblocked.

Module search path: StormDPS/surgedps/ (copied from SurgeDPS/src/)
Data path:         StormDPS/surgedps_data/
Cache path:        StormDPS/surgedps_data/cells/
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

# ── Module path setup ───────────────────────────────────────────────────────
# StormDPS/api/surgedps_routes.py → StormDPS/ is one level up
_STORMDS_ROOT = Path(__file__).resolve().parent.parent  # StormDPS/
_SURGEDPS_SRC = str(_STORMDS_ROOT / "surgedps")
if _SURGEDPS_SRC not in sys.path:
    sys.path.insert(0, _SURGEDPS_SRC)

# ── SurgeDPS module imports ──────────────────────────────────────────────────
import numpy as np  # noqa: F401  (must come before rasterio on some platforms)

from damage_model.depth_damage import estimate_damage_from_raster  # type: ignore
from data_ingest.building_fetcher import fetch_buildings  # type: ignore
from tile_gen.pmtiles_builder import raster_to_geojson  # type: ignore
from storm_catalog.catalog import (  # type: ignore
    StormEntry,
    CELL_WIDTH,
    CELL_HEIGHT,
    fetch_active_storms,
    HISTORICAL_STORMS,
)
from storm_catalog.hurdat2_parser import (  # type: ignore
    get_seasons,
    get_storms_for_year,
    search_storms,
    get_storm_by_id,
)
from storm_catalog.surge_model import generate_surge_raster  # type: ignore

# ── Data / cache paths ───────────────────────────────────────────────────────
_DATA_DIR = _STORMDS_ROOT / "surgedps_data"
CACHE_DIR = str(_DATA_DIR / "cells")
os.makedirs(CACHE_DIR, exist_ok=True)

SEASON_MIN_YEAR = 2015

# ── DPS Score Lookup ─────────────────────────────────────────────────────────
_DPS_SCORES: dict = {}
_dps_path = _DATA_DIR / "dps_scores.json"
if _dps_path.exists():
    with open(_dps_path) as _f:
        _DPS_SCORES = json.load(_f)

# ── Active Storm State (module-level, thread-safe via GIL for reads) ─────────
_active_storm: Optional[StormEntry] = None
_active_exposure_region: str = ""

# ── Router ───────────────────────────────────────────────────────────────────
router = APIRouter()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helper functions (ported 1:1 from api_server.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_REGIONAL_BLDG_BASELINE = {
    "Tampa Bay": 10000, "Mid-Atlantic": 8000, "Carolinas": 5000,
    "SE Florida": 8000, "NE Florida / Georgia": 4000, "SW Florida": 3000,
    "Texas": 2000, "Louisiana / Mississippi": 1500, "Alabama / FL Panhandle": 2000,
    "FL Big Bend": 800, "Northeast": 6000, "North Carolina": 3000,
    "Mississippi": 1000, "Leeward Islands": 500, "Puerto Rico / USVI": 2000,
    "Windward Islands": 300, "Bahamas": 400, "Cuba / Jamaica": 500,
    "Mexico / Central America": 300,
}


def _compute_confidence(storm_id: str) -> dict:
    sdir = os.path.join(CACHE_DIR, storm_id)
    if not os.path.isdir(sdir):
        return {"confidence": "unvalidated", "building_count": 0}
    total = 0
    for fname in os.listdir(sdir):
        if fname.endswith("_buildings.json"):
            try:
                with open(os.path.join(sdir, fname)) as f:
                    data = json.load(f)
                total += len(data.get("features", []))
            except Exception:
                pass
    level = "high" if total > 500 else ("medium" if total >= 50 else "low")
    return {"confidence": level, "building_count": total}


def _compute_eli(dps_score: float, building_count: int) -> dict:
    if dps_score <= 0 or building_count <= 0:
        return {"eli": 0.0, "eli_tier": "unavailable"}
    eli = math.sqrt(dps_score) * math.sqrt(building_count)
    if eli >= 400:
        tier = "extreme"
    elif eli >= 250:
        tier = "very_high"
    elif eli >= 100:
        tier = "high"
    elif eli >= 50:
        tier = "moderate"
    else:
        tier = "low"
    return {"eli": round(eli, 1), "eli_tier": tier}


def _compute_validated_dps(dps_score: float, building_count: int, exposure_region: str) -> dict:
    if dps_score <= 0 or building_count <= 0:
        return {"validated_dps": dps_score, "dps_adjustment": 0.0, "dps_adj_reason": ""}
    baseline = _REGIONAL_BLDG_BASELINE.get(exposure_region, 2000)
    ratio = building_count / baseline
    if ratio > 3.0:
        adj = min(math.log2(ratio) * 0.03, 0.15)
        validated = min(100.0, dps_score * (1 + adj))
        reason = f"+{adj:.0%} ({building_count:,} bldgs vs {baseline:,} baseline)"
    elif ratio < 0.33:
        adj = -min(math.log2(1 / ratio) * 0.03, 0.10)
        validated = max(0.0, dps_score * (1 + adj))
        reason = f"{adj:.0%} ({building_count:,} bldgs vs {baseline:,} baseline)"
    else:
        return {"validated_dps": round(dps_score, 1), "dps_adjustment": 0.0, "dps_adj_reason": ""}
    return {"validated_dps": round(validated, 1), "dps_adjustment": round(adj, 3), "dps_adj_reason": reason}


def _inject_dps(storm_dict: dict) -> dict:
    if storm_dict.get("dps_score", 0) > 0:
        return storm_dict
    sid = storm_dict.get("storm_id", "")
    score = _DPS_SCORES.get(sid, 0)
    if score == 0:
        name = storm_dict.get("name", "").lower()
        name = (
            name.replace("hurricane ", "")
            .replace("tropical storm ", "")
            .replace("tropical depression ", "")
            .strip()
        )
        year = storm_dict.get("year", 0)
        score = _DPS_SCORES.get(f"{name}_{year}", 0)
    storm_dict["dps_score"] = score
    return storm_dict


def _empty_fc() -> dict:
    return {"type": "FeatureCollection", "features": []}


def _storm_cache_dir(storm: StormEntry) -> str:
    d = os.path.join(CACHE_DIR, storm.storm_id)
    os.makedirs(d, exist_ok=True)
    return d


def _cell_bbox(col: int, row: int, storm: StormEntry):
    lon_min = storm.grid_origin_lon + col * CELL_WIDTH
    lat_min = storm.grid_origin_lat + row * CELL_HEIGHT
    return lon_min, lat_min, lon_min + CELL_WIDTH, lat_min + CELL_HEIGHT


def _load_cell_sync(col: int, row: int) -> dict:
    """Blocking cell-load logic — called in a thread pool from async routes."""
    storm = _active_storm
    if storm is None:
        return {"buildings": _empty_fc(), "flood": _empty_fc()}

    sdir = _storm_cache_dir(storm)
    damage_path = os.path.join(sdir, f"cell_{col}_{row}_damage.geojson")
    flood_path = os.path.join(sdir, f"cell_{col}_{row}_flood.geojson")

    # Cache hit
    if os.path.exists(damage_path) and os.path.exists(flood_path):
        with open(damage_path) as f:
            damage_data = json.load(f)
        with open(flood_path) as f:
            flood_data = json.load(f)
        print(f"  [cache hit] cell ({col},{row}) for {storm.storm_id}")
        return {"buildings": damage_data, "flood": flood_data}

    lon_min, lat_min, lon_max, lat_max = _cell_bbox(col, row, storm)
    print(
        f"[{storm.storm_id} cell {col},{row}] "
        f"bbox=({lon_min:.2f},{lat_min:.2f})->({lon_max:.2f},{lat_max:.2f})"
    )

    # 1. Parametric surge raster
    raster_path = os.path.join(sdir, f"cell_{col}_{row}_depth.tif")
    if not os.path.exists(raster_path):
        generate_surge_raster(
            lon_min=lon_min, lat_min=lat_min,
            lon_max=lon_max, lat_max=lat_max,
            output_path=raster_path,
            landfall_lon=storm.landfall_lon,
            landfall_lat=storm.landfall_lat,
            max_wind_kt=storm.max_wind_kt,
            min_pressure_mb=storm.min_pressure_mb,
            heading_deg=storm.heading_deg,
            speed_kt=storm.speed_kt,
        )

    # 2. Flood polygons
    if not os.path.exists(flood_path):
        raster_to_geojson(raster_path, flood_path)
    with open(flood_path) as f:
        flood_data = json.load(f)

    # 3. OSM buildings + HAZUS damage model
    buildings_path = os.path.join(sdir, f"cell_{col}_{row}_buildings.json")
    if not os.path.exists(buildings_path):
        osm_buildings = fetch_buildings(lon_min, lat_min, lon_max, lat_max)
        with open(buildings_path, "w") as f:
            json.dump(osm_buildings, f)
    else:
        with open(buildings_path) as f:
            osm_buildings = json.load(f)

    # 4. Run damage model
    if not os.path.exists(damage_path):
        damaged_buildings = estimate_damage_from_raster(
            raster_path, osm_buildings
        )
        with open(damage_path, "w") as f:
            json.dump(damaged_buildings, f)
    else:
        with open(damage_path) as f:
            damaged_buildings = json.load(f)

    return {"buildings": damaged_buildings, "flood": flood_data}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Routes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.get("/health")
async def surgedps_health():
    return {
        "status": "ok",
        "active_storm": _active_storm.storm_id if _active_storm else None,
    }


@router.get("/seasons")
async def surgedps_seasons():
    """Return available seasons (2015+) for the storm browser."""
    try:
        seasons = await asyncio.to_thread(get_seasons)
        return [s for s in seasons if s >= SEASON_MIN_YEAR]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storms/active")
async def surgedps_active_storms():
    """Fetch currently active NHC storms."""
    try:
        active = await asyncio.to_thread(fetch_active_storms)
        return [_inject_dps(s.to_dict()) for s in active]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storms/historic")
async def surgedps_historic_storms():
    """Return the curated list of historic storms with DPS scores."""
    return [_inject_dps(s.to_dict()) for s in HISTORICAL_STORMS]


@router.get("/storms/search")
async def surgedps_search_storms(q: str = Query(..., description="Storm name or ID")):
    """Search storms by name or ID."""
    try:
        ql = q.lower().strip()
        seen_ids: set = set()
        results = []
        # Curated list first
        for s in HISTORICAL_STORMS:
            if ql in s.name.lower() or ql in s.storm_id.lower():
                results.append(_inject_dps(s.to_dict()))
                seen_ids.add(s.storm_id)
        # Fill from HURDAT2
        hurdat_matches = await asyncio.to_thread(search_storms, q)
        for s in hurdat_matches:
            if s.storm_id not in seen_ids:
                results.append(_inject_dps(s.to_dict()))
                seen_ids.add(s.storm_id)
            if len(results) >= 20:
                break
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/season/{year}")
async def surgedps_season(year: int):
    """Return all storms for a given season year."""
    try:
        storms = await asyncio.to_thread(get_storms_for_year, year)
        return [_inject_dps(s.to_dict()) for s in storms]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid year")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storm/{storm_id}/activate")
async def surgedps_activate_storm(storm_id: str):
    """Select a storm for analysis and load the center grid cell."""
    global _active_storm, _active_exposure_region

    # HURDAT2 lookup first, then curated historic list
    storm = await asyncio.to_thread(get_storm_by_id, storm_id)
    if storm is None:
        for hs in HISTORICAL_STORMS:
            if hs.storm_id == storm_id:
                storm = hs
                break
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm '{storm_id}' not found")

    _active_storm = storm
    print(f"\n{'='*60}")
    print(f"ACTIVATED: {storm.name} ({storm.year}) — Cat {storm.category}")
    print(f"  Landfall: ({storm.landfall_lon}, {storm.landfall_lat})")
    print(f"  Wind: {storm.max_wind_kt} kt  Pressure: {storm.min_pressure_mb} mb")
    print(f"  Grid origin: ({storm.grid_origin_lon}, {storm.grid_origin_lat})")
    print(f"{'='*60}\n")

    # Load center cell (0,0) in a thread — heavy I/O + compute
    print("Loading eye cell (0,0)...")
    center_data = await asyncio.to_thread(_load_cell_sync, 0, 0)

    # Build enriched storm dict
    conf = _compute_confidence(storm.storm_id)
    storm_data = _inject_dps(storm.to_dict())
    storm_data["confidence"] = conf["confidence"]
    storm_data["building_count"] = conf["building_count"]

    eli = _compute_eli(storm_data.get("dps_score", 0), conf["building_count"])
    storm_data["eli"] = eli["eli"]
    storm_data["eli_tier"] = eli["eli_tier"]

    _active_exposure_region = storm_data.get("exposure_region", "")
    vdps = _compute_validated_dps(
        storm_data.get("dps_score", 0), conf["building_count"], _active_exposure_region
    )
    storm_data["validated_dps"] = vdps["validated_dps"]
    storm_data["dps_adjustment"] = vdps["dps_adjustment"]
    storm_data["dps_adj_reason"] = vdps["dps_adj_reason"]

    return {"storm": storm_data, "center_cell": center_data}


@router.get("/cell")
async def surgedps_cell(col: int = Query(...), row: int = Query(...)):
    """Load a grid cell for the active storm."""
    if _active_storm is None:
        raise HTTPException(status_code=400, detail="No storm active")

    try:
        print(f"\n--- Loading cell ({col}, {row}) for {_active_storm.name} ---")
        data = await asyncio.to_thread(_load_cell_sync, col, row)

        # Attach updated confidence + ELI after cell load
        conf = _compute_confidence(_active_storm.storm_id)
        data["confidence"] = conf["confidence"]
        data["building_count"] = conf["building_count"]

        dps_val = _DPS_SCORES.get(_active_storm.storm_id, 0) or _DPS_SCORES.get(
            _active_storm.storm_id.lower(), 0
        )
        eli = _compute_eli(dps_val, conf["building_count"])
        data["eli"] = eli["eli"]
        data["eli_tier"] = eli["eli_tier"]

        vdps = _compute_validated_dps(dps_val, conf["building_count"], _active_exposure_region)
        data["validated_dps"] = vdps["validated_dps"]
        data["dps_adjustment"] = vdps["dps_adjustment"]
        data["dps_adj_reason"] = vdps["dps_adj_reason"]

        n = len(data.get("buildings", {}).get("features", []))
        print(
            f"--- Cell ({col},{row}): {n} buildings | "
            f"Confidence: {conf['confidence']} ({conf['building_count']} total) ---"
        )
        return data
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
