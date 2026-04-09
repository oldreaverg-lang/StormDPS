"""
SurgeDPS FastAPI Router
Ported from SurgeDPS/scripts/api_server.py (HTTPServer) → FastAPI.

Mounted at /surgedps/api in main.py.
All heavy work (cell load, activate) runs in a thread pool so the
async event loop stays unblocked.

Performance design
──────────────────
Cached cell responses are served via raw byte concatenation — the
damage.geojson and flood.geojson files are read as bytes and spliced
directly into the response body without a json.load → json.dumps
roundtrip. This drops cached-cell latency from ~5s → ~0.5s for a
typical 13 MB payload.

Non-cached cells still run the full pipeline (surge raster → OSM fetch
→ HAZUS model) which takes 30-120 s depending on network / OSM density.

Startup pre-warming
───────────────────
The top 5 historic storms are pre-warmed in background threads so the
first user click on any of them hits the cache immediately.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import sys
import time
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

logger = logging.getLogger("surgedps")

# ── Module path setup ───────────────────────────────────────────────────────
# Must happen BEFORE any surgedps-internal imports
_STORMDS_ROOT = Path(__file__).resolve().parent.parent  # StormDPS/
_SURGEDPS_SRC = str(_STORMDS_ROOT / "surgedps")
if _SURGEDPS_SRC not in sys.path:
    sys.path.insert(0, _SURGEDPS_SRC)

from data_ingest.duckdb_cache import building_cache  # type: ignore  # DuckDB in-session cache
from storage.r2_client import r2                     # type: ignore  # Cloudflare R2 (no-op if unconfigured)
from damage_model.depth_damage import estimate_damage_from_raster  # type: ignore
from data_ingest.building_fetcher import fetch_buildings  # type: ignore
from tile_gen.pmtiles_builder import raster_to_geojson, build_vector_pmtiles  # type: ignore
from tile_gen.cog_builder import build_cog  # type: ignore
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
from storm_catalog.surge_model import generate_surge_raster, SURGE_MODEL_VERSION, validate_surge_model  # type: ignore

# ── Data / cache paths ───────────────────────────────────────────────────────
# Use Railway persistent volume when PERSISTENT_DATA_DIR is set; otherwise
# fall back to the local surgedps_data/ directory.  This ensures cell caches
# survive across deploys instead of being wiped with the ephemeral filesystem.
_PERSISTENT_ROOT = Path(os.environ.get("PERSISTENT_DATA_DIR", str(_STORMDS_ROOT / "surgedps_data")))
_DATA_DIR = _PERSISTENT_ROOT / "surgedps"
CACHE_DIR = str(_DATA_DIR / "cells")
os.makedirs(CACHE_DIR, exist_ok=True)

SEASON_MIN_YEAR = 2015

# ── DPS Score Lookup ─────────────────────────────────────────────────────────
_DPS_SCORES: dict = {}
_dps_path = _DATA_DIR / "dps_scores.json"
if _dps_path.exists():
    try:
        with open(_dps_path) as _f:
            _DPS_SCORES = json.load(_f)
        logger.info("Loaded %d DPS scores from %s", len(_DPS_SCORES), _dps_path)
    except (json.JSONDecodeError, IOError) as _e:
        logger.error("Failed to load DPS scores from %s: %s", _dps_path, _e)
else:
    logger.warning("DPS scores file not found: %s", _dps_path)

# ── Active Storm State ────────────────────────────────────────────────────────
# Keyed by storm_id so multiple concurrent users can work with different storms.
# Each entry stores (StormEntry, exposure_region) tuple.
# Bounded to 50 entries (LRU eviction) to prevent unbounded memory growth.
_MAX_ACTIVE_STORMS = 50
_active_storms: OrderedDict[str, tuple[StormEntry, str]] = OrderedDict()
_active_storms_lock = threading.Lock()

# Legacy global for backward compat with health endpoint
_last_activated_storm_id: Optional[str] = None


def _register_active_storm(storm_id: str, storm: StormEntry, region: str) -> None:
    """Thread-safe registration with LRU eviction."""
    with _active_storms_lock:
        _active_storms.pop(storm_id, None)  # Move to end if exists
        _active_storms[storm_id] = (storm, region)
        while len(_active_storms) > _MAX_ACTIVE_STORMS:
            _active_storms.popitem(last=False)  # Evict oldest


# ── Per-cell generation locks ─────────────────────────────────────────────────
# Ensures only one coroutine generates a given cell at a time.
# When multiple users request the same uncached cell simultaneously (common
# during live storms), the second request waits for the first to finish and
# then reads the cached result instead of launching a duplicate pipeline.
# Bounded to 200 entries to prevent unbounded memory growth.
_MAX_CELL_LOCKS = 200
_cell_locks: OrderedDict[str, asyncio.Lock] = OrderedDict()
_cell_locks_guard = threading.Lock()  # plain lock — used only during lock creation


def _get_cell_lock(storm_id: str, col: int, row: int) -> asyncio.Lock:
    """Return (creating if needed) the per-cell asyncio.Lock for (storm, col, row)."""
    key = f"{storm_id}:{col},{row}"
    with _cell_locks_guard:
        if key in _cell_locks:
            _cell_locks.move_to_end(key)
        else:
            _cell_locks[key] = asyncio.Lock()
            while len(_cell_locks) > _MAX_CELL_LOCKS:
                _cell_locks.popitem(last=False)
        return _cell_locks[key]


# ── Router ───────────────────────────────────────────────────────────────────
router = APIRouter()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helper functions
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


# Confidence cache: {storm_id: (result_dict, timestamp)}
_confidence_cache: dict[str, tuple[dict, float]] = {}
_CONFIDENCE_TTL = 60.0  # seconds — rescan at most once per minute per storm


def _compute_confidence(storm_id: str) -> dict:
    now = time.monotonic()
    cached = _confidence_cache.get(storm_id)
    if cached and (now - cached[1]) < _CONFIDENCE_TTL:
        return cached[0]

    # Ask DuckDB instead of scanning the filesystem — O(1) vs O(n files)
    total = building_cache.building_count()
    if total == 0:
        # DuckDB has nothing yet (cold start); fall back to filesystem scan.
        # Scan _damage.geojson files (always present after cell generation)
        # rather than _buildings.json (intermediate file that may be cleaned up).
        sdir = os.path.join(CACHE_DIR, storm_id)
        if not os.path.isdir(sdir):
            result = {"confidence": "unvalidated", "building_count": 0}
            _confidence_cache[storm_id] = (result, now)
            return result
        for fname in os.listdir(sdir):
            if fname.endswith("_damage.geojson"):
                try:
                    with open(os.path.join(sdir, fname)) as f:
                        data = json.load(f)
                    total += len(data.get("features", []))
                except (json.JSONDecodeError, IOError, OSError) as exc:
                    logger.warning("Skipping %s during confidence calc: %s", fname, exc)
    level = "high" if total > 500 else ("medium" if total >= 50 else "low")
    result = {"confidence": level, "building_count": total}
    _confidence_cache[storm_id] = (result, now)
    return result


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
        name = (
            storm_dict.get("name", "").lower()
            .replace("hurricane ", "").replace("tropical storm ", "")
            .replace("tropical depression ", "").strip()
        )
        year = storm_dict.get("year", 0)
        score = _DPS_SCORES.get(f"{name}_{year}", 0)
    storm_dict["dps_score"] = score
    return storm_dict


def _empty_fc() -> dict:
    return {"type": "FeatureCollection", "features": []}


_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def _sanitize_storm_id(storm_id: str) -> str:
    """Strip anything except alphanumeric, underscore, and hyphen."""
    if _SAFE_ID_RE.match(storm_id):
        return storm_id
    return re.sub(r"[^A-Za-z0-9_\-]", "", storm_id)


def _storm_cache_dir(storm: StormEntry) -> str:
    safe_id = _sanitize_storm_id(storm.storm_id)
    d = os.path.join(CACHE_DIR, safe_id)
    os.makedirs(d, exist_ok=True)
    return d


def _cell_paths(storm: StormEntry, col: int, row: int) -> tuple[str, str]:
    sdir = _storm_cache_dir(storm)
    return (
        os.path.join(sdir, f"cell_{col}_{row}_damage.geojson"),
        os.path.join(sdir, f"cell_{col}_{row}_flood.geojson"),
    )


def _cell_bbox(col: int, row: int, storm: StormEntry) -> tuple[float, float, float, float]:
    lon_min = storm.grid_origin_lon + col * CELL_WIDTH
    lat_min = storm.grid_origin_lat + row * CELL_HEIGHT
    return lon_min, lat_min, lon_min + CELL_WIDTH, lat_min + CELL_HEIGHT


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Fast response helpers — no JSON parse for large files
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _r2_cell_key(storm: StormEntry, col: int, row: int, suffix: str) -> str:
    """R2 object key for a cell file, e.g. surgedps/cells/ian2022/2_3_damage.geojson"""
    safe_id = _sanitize_storm_id(storm.storm_id)
    return f"surgedps/cells/{safe_id}/cell_{col}_{row}_{suffix}.geojson"


def _read_cell_raw(storm: StormEntry, col: int, row: int) -> tuple[bytes, bytes] | None:
    """
    Read cached damage + flood files as raw bytes.
    Returns (damage_bytes, flood_bytes) or None if not cached.

    Read order: local filesystem first (fastest), then R2 (survives deploys).
    Avoids json.load → json.dumps for the large building payload.
    """
    damage_path, flood_path = _cell_paths(storm, col, row)

    # 1. Local filesystem (hot cache — present within the same Railway instance)
    if os.path.exists(damage_path) and os.path.exists(flood_path):
        with open(damage_path, "rb") as f:
            damage = f.read()
        with open(flood_path, "rb") as f:
            flood = f.read()
        return damage, flood

    # 2. R2 (cold cache — survives redeploys; populate local copy for subsequent reads)
    if r2.available:
        damage = r2.download_bytes(_r2_cell_key(storm, col, row, "damage"))
        flood  = r2.download_bytes(_r2_cell_key(storm, col, row, "flood"))
        if damage and flood:
            logger.info("[R2] Cache hit for cell %s:%d,%d — writing to local fs", storm.storm_id, col, row)
            os.makedirs(os.path.dirname(damage_path), exist_ok=True)
            with open(damage_path, "wb") as f:
                f.write(damage)
            with open(flood_path, "wb") as f:
                f.write(flood)
            return damage, flood

    return None


def _write_cell_to_r2(storm: StormEntry, col: int, row: int) -> None:
    """
    After a cell is generated locally, push it to R2 for persistence.
    Runs in the background — does not block the response.
    """
    if not r2.available:
        return
    damage_path, flood_path = _cell_paths(storm, col, row)
    try:
        with open(damage_path, "rb") as f:
            r2.upload_bytes(_r2_cell_key(storm, col, row, "damage"), f.read(), "application/geo+json")
        with open(flood_path, "rb") as f:
            r2.upload_bytes(_r2_cell_key(storm, col, row, "flood"), f.read(), "application/geo+json")
        logger.info("[R2] Persisted cell %s:%d,%d", storm.storm_id, col, row)
    except Exception as exc:
        logger.warning("[R2] Failed to persist cell %s:%d,%d: %s", storm.storm_id, col, row, exc)


def _build_cell_json(damage_bytes: bytes, flood_bytes: bytes, meta: dict) -> bytes:
    """
    Splice raw file bytes + small metadata dict into a valid JSON response
    without parsing the large GeoJSON files.

    Result shape: {"buildings":{...},"flood":{...},"confidence":"high",...}
    meta_bytes = {"confidence":"high",...}  →  strip leading { and prepend comma
    """
    meta_bytes = orjson.dumps(meta)
    # meta_bytes = b'{"confidence":"high",...}'
    # We splice: {"buildings":<damage>,"flood":<flood>,<meta fields>}
    return b'{"buildings":' + damage_bytes + b',"flood":' + flood_bytes + b"," + meta_bytes[1:]


def _build_activate_json(storm_data: dict, damage_bytes: bytes, flood_bytes: bytes) -> bytes:
    """
    Build activate response using raw cell bytes.
    Shape: {"storm":{...},"center_cell":{"buildings":{...},"flood":{...}}}
    """
    storm_bytes = orjson.dumps(storm_data)
    return (
        b'{"storm":' + storm_bytes
        + b',"center_cell":{"buildings":' + damage_bytes
        + b',"flood":' + flood_bytes + b"}}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Cell generation (blocking — runs in thread pool)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _generate_cell_files(storm: StormEntry, col: int, row: int) -> bool:
    """
    Ensure damage + flood files exist for (col, row) under storm.
    Returns True if files are present after the call.
    Does NOT load the files into memory — callers read raw bytes themselves.
    """
    damage_path, flood_path = _cell_paths(storm, col, row)

    # Already cached — nothing to do
    if os.path.exists(damage_path) and os.path.exists(flood_path):
        logger.debug("[cache hit] cell (%d,%d) for %s", col, row, storm.storm_id)
        return True

    sdir = _storm_cache_dir(storm)
    lon_min, lat_min, lon_max, lat_max = _cell_bbox(col, row, storm)
    logger.info("[%s cell %d,%d] bbox=(%.2f,%.2f)->(%.2f,%.2f)",
                storm.storm_id, col, row, lon_min, lat_min, lon_max, lat_max)

    # 1. Surge raster
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

    # ── Early exit: skip cells with negligible surge ──────────────────────
    # If the max depth in the raster is < 0.1 m the cell has no meaningful
    # impact.  Write empty GeoJSON files and return immediately — avoids
    # the expensive building fetch + HAZUS pipeline entirely.
    _MIN_SURGE_M = 0.1
    try:
        import rasterio as _rio
        with _rio.open(raster_path) as _ds:
            _max_depth = float(_ds.read(1).max())
        if _max_depth < _MIN_SURGE_M:
            logger.info("[%s cell %d,%d] max surge %.3f m < %.1f m — skipping (no impact)",
                        storm.storm_id, col, row, _max_depth, _MIN_SURGE_M)
            with open(damage_path, "w") as f:
                json.dump(_empty_fc(), f)
            with open(flood_path, "w") as f:
                json.dump(_empty_fc(), f)
            return True
    except Exception as exc:
        logger.warning("[%s cell %d,%d] surge threshold check failed (continuing): %s",
                       storm.storm_id, col, row, exc)

    # 1b. Cloud-Optimized GeoTIFF for range-request streaming from R2
    cog_path = os.path.join(sdir, f"cell_{col}_{row}_depth_cog.tif")
    if not os.path.exists(cog_path) and os.path.exists(raster_path):
        try:
            build_cog(raster_path, cog_path)
            logger.info("[%s cell %d,%d] COG built: %s", storm.storm_id, col, row, cog_path)
            if r2.available:
                safe_id = _sanitize_storm_id(storm.storm_id)
                r2.upload_file(
                    f"surgedps/cogs/{safe_id}/cell_{col}_{row}.tif",
                    cog_path,
                )
        except Exception as exc:
            logger.warning("[%s cell %d,%d] COG build failed (non-fatal): %s",
                           storm.storm_id, col, row, exc)

    # 2. Flood polygons
    if not os.path.exists(flood_path):
        raster_to_geojson(raster_path, flood_path)

    # 2b. Vector PMTiles for efficient tile serving
    pmtiles_path = os.path.join(sdir, f"cell_{col}_{row}_flood.pmtiles")
    if not os.path.exists(pmtiles_path) and os.path.exists(flood_path):
        try:
            build_vector_pmtiles(flood_path, pmtiles_path)
            logger.info("[%s cell %d,%d] PMTiles built: %s", storm.storm_id, col, row, pmtiles_path)
            if r2.available:
                safe_id = _sanitize_storm_id(storm.storm_id)
                r2.upload_file(
                    f"surgedps/pmtiles/{safe_id}/cell_{col}_{row}.pmtiles",
                    pmtiles_path,
                )
        except Exception as exc:
            logger.warning("[%s cell %d,%d] PMTiles build failed (non-fatal): %s",
                           storm.storm_id, col, row, exc)

    # 3. OSM buildings (fetch_buildings handles its own caching)
    buildings_path = os.path.join(sdir, f"cell_{col}_{row}_buildings.json")
    fetch_buildings(lon_min, lat_min, lon_max, lat_max, buildings_path, cache=True)

    with open(buildings_path) as f:
        buildings_data = json.load(f)

    if not buildings_data.get("features"):
        # No buildings — write empty damage file with version stamp
        empty = _empty_fc()
        empty["surge_model_version"] = SURGE_MODEL_VERSION
        with open(damage_path, "w") as f:
            json.dump(empty, f)
    else:
        # 4. HAZUS damage model — writes damage_path itself
        estimate_damage_from_raster(raster_path, buildings_path, damage_path)
        # Stamp surge model version into the damage file so stale cache is detectable
        try:
            with open(damage_path) as f:
                dmg = json.load(f)
            dmg["surge_model_version"] = SURGE_MODEL_VERSION
            with open(damage_path, "w") as f:
                json.dump(dmg, f)
        except Exception:
            pass  # version stamp is best-effort

    n = len(buildings_data.get("features", []))
    logger.info("[%s cell %d,%d] %d buildings processed", storm.storm_id, col, row, n)

    # 5. Clean up intermediate files to conserve volume space.
    #    depth.tif and buildings.json can be 30-100 MB each in dense urban areas.
    #    Only damage.geojson + flood.geojson are needed for cached responses.
    for tmp in (raster_path, buildings_path):
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
                logger.debug("[%s cell %d,%d] Removed intermediate file: %s",
                             storm.storm_id, col, row, os.path.basename(tmp))
        except OSError as exc:
            logger.warning("[%s cell %d,%d] Could not remove %s: %s",
                           storm.storm_id, col, row, os.path.basename(tmp), exc)

    # Persist to R2 so the cell survives Railway redeploys
    if os.path.exists(damage_path) and os.path.exists(flood_path):
        threading.Thread(
            target=_write_cell_to_r2,
            args=(storm, col, row),
            daemon=True,
        ).start()
        # Update coverage manifest (data catalog)
        threading.Thread(
            target=_update_manifest,
            args=(storm.storm_id, col, row, n),
            daemon=True,
        ).start()

    return os.path.exists(damage_path) and os.path.exists(flood_path)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Startup pre-warming
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Pre-warm only Harvey at startup to keep Railway costs low.
# All other storms remain available on-demand — cells generate when users
# activate them and are cached on the persistent volume for future hits.
_PREWARM_STORM_IDS = ["harvey_2017"]

# 3×3 grid cells to pre-warm per storm (matches frontend activation pattern)
_PREWARM_CELLS = [(c, r) for r in range(-1, 2) for c in range(-1, 2)]


def _cell_is_current(damage_path: str) -> bool:
    """Return True if the cached damage file was built with the current surge model version."""
    try:
        with open(damage_path) as f:
            data = json.load(f)
        return data.get("surge_model_version") == SURGE_MODEL_VERSION
    except Exception:
        return False  # unreadable or missing → regenerate


def _prewarm_storm(storm_id: str) -> None:
    """Pre-generate the full 3×3 grid for a historic storm."""
    storm = None
    for hs in HISTORICAL_STORMS:
        if hs.storm_id == storm_id:
            storm = hs
            break
    if storm is None:
        return

    cached = 0
    generated = 0
    failed = 0
    stale = 0
    for col, row in _PREWARM_CELLS:
        damage_path, flood_path = _cell_paths(storm, col, row)
        if os.path.exists(damage_path) and os.path.exists(flood_path):
            if _cell_is_current(damage_path):
                cached += 1
                continue
            # Stale — built with old surge formula. Delete and regenerate.
            stale += 1
            logger.info("[prewarm] %s cell (%d,%d) has stale surge model — regenerating",
                        storm_id, col, row)
            for f in (damage_path, flood_path):
                try:
                    os.remove(f)
                except OSError:
                    pass
        try:
            _generate_cell_files(storm, col, row)
            generated += 1
        except Exception as e:
            failed += 1
            logger.error("[prewarm] %s cell (%d,%d) failed: %s", storm_id, col, row, e)
        # Small pause between cells to avoid overloading OSM/NSI APIs
        time.sleep(2)

    logger.info("[prewarm] %s: %d cached, %d generated, %d stale→regen, %d failed (of %d cells)",
                storm_id, cached, generated, stale, failed, len(_PREWARM_CELLS))


def _prewarm_worker(storm_id: str, delay: float) -> None:
    """Staggered pre-warm worker for a single storm."""
    time.sleep(delay)
    _prewarm_storm(storm_id)


def _start_prewarm() -> None:
    """Launch pre-warm threads at import time with a staggered start.

    Storms are warmed sequentially (one at a time) to stay within API rate
    limits and Railway memory constraints.  Each storm is delayed by 120s
    after the previous one starts, giving each ~2 min to finish its 9 cells
    before the next storm begins.
    """
    if os.environ.get("SURGEDPS_SKIP_PREWARM"):
        logger.info("Pre-warming skipped (SURGEDPS_SKIP_PREWARM set)")
        return
    logger.info("[prewarm] Queuing %d storms × %d cells = %d total cells",
                len(_PREWARM_STORM_IDS), len(_PREWARM_CELLS),
                len(_PREWARM_STORM_IDS) * len(_PREWARM_CELLS))
    for i, sid in enumerate(_PREWARM_STORM_IDS):
        t = threading.Thread(
            target=_prewarm_worker,
            args=(sid, i * 120),  # 2 min stagger between storms
            daemon=True,
            name=f"prewarm-{sid}",
        )
        t.start()


# ── Startup surge formula sanity check ───────────────────────────────────────
# Runs synchronously at import time (pure math, <1ms).  Logs calibration table
# and raises RuntimeError if any reference storm deviates >35% from observed.
# This catches formula regressions before any cells are generated or served.
def _validate_surge_on_startup() -> None:
    warnings = validate_surge_model()
    if warnings:
        for w in warnings:
            logger.error("[surge_model] %s", w)
        raise RuntimeError(
            "Surge formula failed calibration check — deploy aborted. "
            "Fix estimate_peak_surge_ft() in surge_model.py and redeploy."
        )

_validate_surge_on_startup()

# Kick off pre-warming when the router module is imported
_start_prewarm()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Routes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.get("/health")
async def surgedps_health():
    return {
        "status": "ok",
        "active_storms": list(_active_storms.keys()),
        "last_activated": _last_activated_storm_id,
        "prewarm_storms": len(_PREWARM_STORM_IDS),
        "prewarm_cells_per_storm": len(_PREWARM_CELLS),
    }


@router.get("/seasons")
async def surgedps_seasons():
    try:
        seasons = await asyncio.to_thread(get_seasons)
        return [s for s in seasons if s["year"] >= SEASON_MIN_YEAR]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storms/active")
async def surgedps_active_storms():
    try:
        active = await asyncio.to_thread(fetch_active_storms)
        return [_inject_dps(s.to_dict()) for s in active]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storms/historic")
async def surgedps_historic_storms():
    return [_inject_dps(s.to_dict()) for s in HISTORICAL_STORMS]


@router.get("/storms/search")
async def surgedps_search_storms(q: str = Query(...)):
    try:
        ql = q.lower().strip()
        seen_ids: set = set()
        seen_name_year: set = set()  # deduplicate across ID formats (e.g. katrina_2005 vs AL122005)
        results = []
        for s in HISTORICAL_STORMS:
            if ql in s.name.lower() or ql in s.storm_id.lower():
                results.append(_inject_dps(s.to_dict()))
                seen_ids.add(s.storm_id)
                seen_name_year.add((s.name.lower().strip(), s.year))
        hurdat_matches = await asyncio.to_thread(search_storms, q)
        for s in hurdat_matches:
            key = (s.name.lower().strip(), s.year)
            if s.storm_id not in seen_ids and key not in seen_name_year:
                results.append(_inject_dps(s.to_dict()))
                seen_ids.add(s.storm_id)
                seen_name_year.add(key)
            if len(results) >= 20:
                break
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/season/{year}")
async def surgedps_season(year: int):
    try:
        storms = await asyncio.to_thread(get_storms_for_year, year)
        return [_inject_dps(s.to_dict()) for s in storms]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid year")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/storm/{storm_id}/activate")
async def surgedps_activate_storm(storm_id: str):
    global _last_activated_storm_id

    # Resolve storm
    storm = await asyncio.to_thread(get_storm_by_id, storm_id)
    if storm is None:
        for hs in HISTORICAL_STORMS:
            if hs.storm_id == storm_id:
                storm = hs
                break
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm '{storm_id}' not found")

    logger.info("ACTIVATED: %s (%d) — Cat %d  Landfall: (%.2f, %.2f)",
                storm.name, storm.year, storm.category,
                storm.landfall_lon, storm.landfall_lat)

    # Generate center cell if not cached (heavy)
    ok = await asyncio.to_thread(_generate_cell_files, storm, 0, 0)

    # Build storm metadata
    conf = _compute_confidence(storm.storm_id)
    storm_data = _inject_dps(storm.to_dict())
    storm_data["confidence"] = conf["confidence"]
    storm_data["building_count"] = conf["building_count"]
    eli = _compute_eli(storm_data.get("dps_score", 0), conf["building_count"])
    storm_data["eli"] = eli["eli"]
    storm_data["eli_tier"] = eli["eli_tier"]
    exposure_region = storm_data.get("exposure_region", "")
    vdps = _compute_validated_dps(
        storm_data.get("dps_score", 0), conf["building_count"], exposure_region
    )
    storm_data["validated_dps"] = vdps["validated_dps"]
    storm_data["dps_adjustment"] = vdps["dps_adjustment"]
    storm_data["dps_adj_reason"] = vdps["dps_adj_reason"]

    # Register in the per-storm state dict (thread-safe, with LRU eviction)
    _register_active_storm(storm_id, storm, exposure_region)
    _last_activated_storm_id = storm_id

    if ok:
        # ── FAST PATH: splice raw bytes — no json.load / json.dumps ──────────
        raw = await asyncio.to_thread(_read_cell_raw, storm, 0, 0)
        if raw:
            damage_bytes, flood_bytes = raw
            body = _build_activate_json(storm_data, damage_bytes, flood_bytes)
            return Response(content=body, media_type="application/json")

    # Cell generation failed — return storm metadata with empty cells + error flag
    # so the map still flies to the landfall location and user can retry cells manually
    logger.warning("Center cell generation failed for %s — returning empty data", storm.name)
    storm_data["cell_error"] = "Center cell data unavailable — try loading cells manually."
    return {"storm": storm_data, "center_cell": {"buildings": _empty_fc(), "flood": _empty_fc()}}


@router.get("/cell")
async def surgedps_cell(
    col: int = Query(...),
    row: int = Query(...),
    storm_id: Optional[str] = Query(None),
):
    # Resolve storm: prefer explicit storm_id param, fall back to last-activated
    storm: Optional[StormEntry] = None
    exposure_region = ""

    if storm_id:
        with _active_storms_lock:
            entry = _active_storms.get(storm_id)
        if entry:
            storm, exposure_region = entry

    # Backward compat: if no storm_id param or not found, use last activated
    if storm is None and _last_activated_storm_id:
        with _active_storms_lock:
            entry = _active_storms.get(_last_activated_storm_id)
        if entry:
            storm, exposure_region = entry

    if storm is None:
        raise HTTPException(status_code=400, detail="No storm active — activate a storm first")

    try:
        logger.info("Loading cell (%d, %d) for %s", col, row, storm.name)

        # Acquire the per-cell lock so only one coroutine runs the generation
        # pipeline at a time. A second request for the same cell will wait here
        # and then immediately hit the file cache once the first finishes.
        cell_lock = _get_cell_lock(storm.storm_id, col, row)
        async with cell_lock:
            ok = await asyncio.to_thread(_generate_cell_files, storm, col, row)

        # Metadata (small — fast regardless)
        conf = _compute_confidence(storm.storm_id)
        dps_val = _DPS_SCORES.get(storm.storm_id, 0) or _DPS_SCORES.get(storm.storm_id.lower(), 0)
        eli = _compute_eli(dps_val, conf["building_count"])
        vdps = _compute_validated_dps(dps_val, conf["building_count"], exposure_region)
        meta = {
            "confidence": conf["confidence"],
            "building_count": conf["building_count"],
            "eli": eli["eli"],
            "eli_tier": eli["eli_tier"],
            "validated_dps": vdps["validated_dps"],
            "dps_adjustment": vdps["dps_adjustment"],
            "dps_adj_reason": vdps["dps_adj_reason"],
            "provenance": {
                "computed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "model_version": "hazus-fia-v1",
                "nsi_api": "nsi.sec.usace.army.mil/nsiapi/structures",
                "damage_curves": "FEMA HAZUS Flood Technical Manual Ch.5",
                "adjustments": "building_adjuster_v1 (found_ht, med_yr_blt, num_story)",
            },
        }

        if ok:
            # ── FAST PATH: splice raw bytes ───────────────────────────────────
            raw = await asyncio.to_thread(_read_cell_raw, storm, col, row)
            if raw:
                damage_bytes, flood_bytes = raw
                body = _build_cell_json(damage_bytes, flood_bytes, meta)
                n_approx = damage_bytes.count(b'"type":"Feature"')
                logger.info("Cell (%d,%d): ~%d buildings (fast path)", col, row, n_approx)
                return Response(content=body, media_type="application/json")

        raise HTTPException(status_code=500, detail="Cell files not generated")

    except HTTPException:
        raise
    except RuntimeError as e:
        # Building data sources (NSI + Overpass) both unavailable
        logger.warning("Cell (%d,%d) building data unavailable: %s", col, row, e)
        raise HTTPException(
            status_code=503,
            detail="Building data temporarily unavailable — please try again in a moment.",
        )
    except Exception as e:
        logger.exception("Unexpected error in cell (%d,%d): %s", col, row, e)
        raise HTTPException(status_code=500, detail="Internal error generating cell")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Aggregate endpoint — Spatial SQL GROUP BY for emergency managers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@router.post("/aggregate")
async def surgedps_aggregate(body: dict):
    """
    Aggregate building/damage stats within a GeoJSON polygon.

    POST body: { "polygon": <GeoJSON Polygon geometry> }
    Optional:  { "cell_key": "ian2022:2,3" }

    Returns total buildings, total structure value, breakdowns by
    occupancy type and HAZUS code.  Uses DuckDB spatial SQL (ST_Within)
    when the spatial extension is available, otherwise filters by cell_key.
    """
    from shapely.geometry import shape

    polygon_geojson = body.get("polygon")
    cell_key = body.get("cell_key")
    wkt = None

    if polygon_geojson:
        try:
            geom = shape(polygon_geojson)
            wkt = geom.wkt
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid polygon geometry: {exc}")

    result = building_cache.aggregate(wkt_polygon=wkt, cell_key=cell_key)
    return result


@router.post("/near-miss")
async def surgedps_near_miss(body: dict):
    """
    Find buildings in the "near miss" buffer zone — properties just outside
    the flood boundary that would have flooded with slightly higher surge.

    POST body:
        polygon: GeoJSON Polygon geometry (the flood extent)
        buffer_m: buffer distance in meters (default 100)

    Returns a GeoJSON FeatureCollection of near-miss buildings,
    each tagged with "near_miss": true in properties.
    """
    from shapely.geometry import shape

    polygon_geojson = body.get("polygon")
    buffer_m = float(body.get("buffer_m", 100))

    if not polygon_geojson:
        raise HTTPException(status_code=400, detail="polygon is required")

    try:
        geom = shape(polygon_geojson)
        wkt = geom.wkt
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid polygon: {exc}")

    features = building_cache.query_near_miss(wkt, buffer_meters=buffer_m)
    return {"type": "FeatureCollection", "features": features}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Coverage manifest — data catalog for frontend
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_MANIFEST_R2_KEY = "surgedps/manifest.json"


def _update_manifest(storm_id: str, col: int, row: int, building_count: int) -> None:
    """
    Append a cell entry to the coverage manifest in R2.
    The manifest is a JSON dict: { storm_id: { "col,row": { ... } } }
    Read-modify-write with local fallback.
    """
    import time as _time

    # Load existing manifest
    manifest: dict = {}
    if r2.available:
        raw = r2.download_bytes(_MANIFEST_R2_KEY)
        if raw:
            try:
                manifest = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                manifest = {}

    safe_id = _sanitize_storm_id(storm_id)
    if safe_id not in manifest:
        manifest[safe_id] = {}

    manifest[safe_id][f"{col},{row}"] = {
        "building_count": building_count,
        "computed_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
    }

    # Also write locally for filesystem-only mode
    local_manifest = os.path.join(str(_DATA_DIR), "manifest.json")
    with open(local_manifest, "w") as f:
        json.dump(manifest, f)

    if r2.available:
        r2.upload_bytes(
            _MANIFEST_R2_KEY,
            json.dumps(manifest).encode(),
            content_type="application/json",
        )


@router.get("/manifest")
async def surgedps_manifest():
    """
    Return the coverage manifest: which cells have been computed for each storm.
    Frontend uses this to shade pre-computed grid cells differently.
    """
    # Try R2 first, then local file
    if r2.available:
        raw = await asyncio.to_thread(r2.download_bytes, _MANIFEST_R2_KEY)
        if raw:
            return Response(content=raw, media_type="application/json")

    local_manifest = os.path.join(str(_DATA_DIR), "manifest.json")
    if os.path.exists(local_manifest):
        with open(local_manifest, "rb") as f:
            return Response(content=f.read(), media_type="application/json")

    return {}