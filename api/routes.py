"""
FastAPI route definitions for the Hurricane IKE API.

Endpoints:
  - /storms/active — list active tropical cyclones from NHC
  - /storms/{storm_id}/forecast — forecast for a specific storm
  - /sst/track — compute SST anomaly track
  - /storms/{storm_id}/ike — compute IKE for a specific storm
  - /ike/compute — compute IKE from custom parameters
  - /storms/{storm_id}/value — full destructive valuation
  - /storms/{storm_id}/history — historical IKE timeline from HURDAT2
  - /storms/catalog — get storm catalog without computing IKE
  - /storms/{storm_id}/track — fetch storm track and compute IKE
  - /cache/stats — get cache statistics
  - /cache/ike/{storm_id} — delete IKE cache for a specific storm
  - /cache/ike — clear all IKE cache
  - /preload — initiate preload of all storms
  - /preload/generate — generate preload manifest
  - /storms/catalog/global — list storms from global catalog (IBTrACS/HURDAT2)
  - /storms/catalog/custom — list storms from custom catalog
  - /ibtracs/track/{sid} — fetch storm track from IBTrACS
  - /ibtracs/search — search IBTrACS by name/year/basin
  - /health/sources — check health of data sources
  - /storms/{storm_id}/ai-comparison — AI forecast comparison
  - /validation/season — season-wide accuracy validation
  - /validation/storm/{storm_id}/accuracy — per-storm accuracy metrics
  - /validation/outcome — record validation outcome
  - /audit/radii/{storm_id} — submit wind radii audit
  - /audit/radii/{storm_id}/history — get radii audit history
  - /audit/radii/{storm_id}/confidence — get radii confidence score
  - /audit/radii/summary — get radii audit summary
"""

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import httpx
from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from api.schemas import (
    StormSummary,
    IKEResponse,
    ValuationResponse,
    SnapshotInput,
    IBTrACSSearchInput,
)
from models.hurricane import HurricaneSnapshot
from services.noaa_client import NOAAClient, NOAAClientError
from core.ike import (
    compute_ike_from_snapshot,
    knots_to_ms,
    nm_to_meters,
    ms_to_knots,
    meters_to_nm,
)
from core.valuation import compute_valuation


router = APIRouter()
logger = logging.getLogger(__name__)

# FIX 2: Custom ThreadPoolExecutor for CPU-bound IKE computations
_IKE_EXECUTOR = ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 4) * 2),
    thread_name_prefix="ike_compute",
)

# FIX 3: In-memory cache for /storms/active endpoint
_active_storms_cache = None
_active_storms_cache_time = None
_ACTIVE_STORMS_TTL = timedelta(minutes=2)

# FIX 5: Valuation response cache (prevents dogpile on same storm)
_valuation_cache: dict[str, tuple] = {}  # {storm_id: (response_dict, timestamp)}
_valuation_cache_lock = asyncio.Lock()
_VALUATION_CACHE_TTL = timedelta(seconds=30)  # Short TTL — storm data changes

# FIX 6: Lock for active storms stale-while-revalidate pattern
_active_storms_lock = asyncio.Lock()

# FIX 4: Lock for protecting global catalog builds
_catalog_lock = asyncio.Lock()

# FIX 1: In-memory cache for /preload endpoint (non-blocking with 5-min TTL)
_preload_cache = None  # Cached preload response dict
_preload_cache_time = None
_preload_lock = asyncio.Lock()
_PRELOAD_CACHE_TTL = timedelta(minutes=5)

# Persistent data directory — use Railway volume when PERSISTENT_DATA_DIR is set
_PERSISTENT_DATA = Path(os.environ.get("PERSISTENT_DATA_DIR", str(Path(__file__).parent.parent / "data")))

# Cache for global IBTrACS catalog to avoid repeated large downloads/parses.
# We also persist a json cache file so restarts can reuse the catalog quickly.
_GLOBAL_IBTRACS_CATALOG_CACHE = None
_GLOBAL_IBTRACS_CATALOG_TIMESTAMP = None
_GLOBAL_IBTRACS_CATALOG_TTL = timedelta(hours=6)
_GLOBAL_IBTRACS_CACHE_FILE = _PERSISTENT_DATA / "cache" / "ibtracs_catalog.json"

# ------------------------------------------------------------------
# Per-storm IKE result cache
# ------------------------------------------------------------------
# Caches the full IKEResponse list for each storm+params combination.
# IKE depends on the wind model (grid resolution, quadrant method) but
# NOT on the DPS formula — DPS is computed client-side from cached IKE.
# Cache key: storm_id + grid_resolution + skip_points
# ------------------------------------------------------------------
_IKE_CACHE_DIR = _PERSISTENT_DATA / "cache" / "ike"
_IKE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Bump this when the IKE wind model changes (e.g., Holland profile, quadrant method)
# DPS formula changes do NOT require a version bump — DPS is client-side.
_IKE_CACHE_VERSION = "v2"

# Eviction policy: keep at most this many cache files.  When exceeded, the
# oldest files by mtime are purged.  A typical hurricane season has ~20
# named storms; each storm generates a handful of parameter combos, so
# 500 is generous headroom while preventing unbounded growth from years
# of accumulated batch runs.
_IKE_CACHE_MAX_FILES = 500
_IKE_CACHE_MAX_SIZE_MB = 200  # soft cap — triggers eviction when exceeded


def _evict_ike_cache():
    """
    Evict oldest IKE cache files when count or total size exceeds limits.

    Called after each cache write.  Uses mtime to determine age and removes
    the oldest 25% of files to amortize eviction overhead.
    """
    try:
        files = list(_IKE_CACHE_DIR.glob("*.json"))
        if not files:
            return

        total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
        needs_eviction = (
            len(files) > _IKE_CACHE_MAX_FILES
            or total_size_mb > _IKE_CACHE_MAX_SIZE_MB
        )
        if not needs_eviction:
            return

        # Sort by modification time (oldest first)
        files.sort(key=lambda f: f.stat().st_mtime)
        evict_count = max(1, len(files) // 4)
        evicted = 0
        for f in files[:evict_count]:
            try:
                f.unlink()
                evicted += 1
            except OSError:
                pass
        logger.info(
            f"IKE cache eviction: removed {evicted} files "
            f"(was {len(files)} files / {total_size_mb:.1f} MB)"
        )
    except Exception as e:
        logger.warning(f"IKE cache eviction error: {e}")


def _ike_cache_key(storm_id: str, grid_res_km: float, skip: int) -> str:
    """Generate cache filename for a storm+params combo."""
    raw = f"{storm_id}_{grid_res_km}_{skip}_{_IKE_CACHE_VERSION}"
    h = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{storm_id}_{h}.json"


def _load_ike_cache(storm_id: str, grid_res_km: float, skip: int) -> list[dict] | None:
    """Load cached IKE results if available and valid."""
    fname = _ike_cache_key(storm_id, grid_res_km, skip)
    path = _IKE_CACHE_DIR / fname
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        # Verify version and storm_id match
        if data.get("_version") != _IKE_CACHE_VERSION:
            return None
        if data.get("_storm_id") != storm_id:
            return None
        return data.get("results")
    except json.JSONDecodeError as e:
        logger.warning(f"Corrupted IKE cache file for {storm_id}: {e}")
        return None
    except (KeyError, Exception) as e:
        logger.warning(f"Invalid IKE cache data for {storm_id}: {e}")
        return None


def _save_ike_cache(storm_id: str, grid_res_km: float, skip: int,
                    results: list[dict], source: str, compute_ms: float):
    """Save IKE results to disk cache with atomic writes."""
    import tempfile
    fname = _ike_cache_key(storm_id, grid_res_km, skip)
    path = _IKE_CACHE_DIR / fname
    payload = {
        "_version": _IKE_CACHE_VERSION,
        "_storm_id": storm_id,
        "_source": source,
        "_grid_res_km": grid_res_km,
        "_skip_points": skip,
        "_compute_ms": round(compute_ms, 1),
        "_cached_at": datetime.utcnow().isoformat(),
        "_obs_count": len(results),
        "results": results,
    }
    tmp_path = path.with_suffix('.tmp')
    try:
        tmp_path.write_text(json.dumps(payload, default=str))
        tmp_path.replace(path)  # atomic rename on POSIX
    except Exception as e:
        logger.warning(f"Failed to write IKE cache for {storm_id}: {e}")
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)

    # Run eviction check after every write
    _evict_ike_cache()


def _ike_response_to_dict(r: "IKEResponse") -> dict:
    """Serialize an IKEResponse to a JSON-safe dict."""
    d = r.dict()
    # Convert datetime to ISO string
    if d.get("timestamp"):
        d["timestamp"] = d["timestamp"].isoformat() if hasattr(d["timestamp"], "isoformat") else str(d["timestamp"])
    return d


def _dict_to_ike_response(d: dict) -> "IKEResponse":
    """Deserialize a dict back to IKEResponse."""
    # Parse timestamp back from ISO string
    if d.get("timestamp") and isinstance(d["timestamp"], str):
        try:
            d["timestamp"] = datetime.fromisoformat(d["timestamp"])
        except ValueError:
            d["timestamp"] = None
    return IKEResponse(**d)


def _search_ibtracs_by_atcf_id(
    client: NOAAClient, csv_text: str, atcf_id: str
) -> list:
    """
    Search IBTrACS CSV for a storm matching a given ATCF ID (e.g., AL142024).

    IBTrACS includes a USA_ATCF_ID column that maps to NHC ATCF identifiers,
    allowing us to find storms like Milton (AL142024) even if they're not
    yet in the HURDAT2 file.
    """
    snapshots = []
    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        row_atcf = row.get("USA_ATCF_ID", "").strip()
        if row_atcf == atcf_id:
            snap = client._ibtracs_row_to_snapshot(row)
            if snap is not None:
                snapshots.append(snap)

    return snapshots


def _ike_to_response(result, snapshot=None) -> IKEResponse:
    """Helper to convert IKEResult to API response, including wind field params."""
    from api.schemas import QuadrantRadii

    # Build quadrant radii if available
    r34_quads = None
    if snapshot and snapshot.r34_quadrants_m:
        r34_quads = QuadrantRadii(
            NE=round(meters_to_nm(snapshot.r34_quadrants_m.get("NE", 0) or 0), 1),
            SE=round(meters_to_nm(snapshot.r34_quadrants_m.get("SE", 0) or 0), 1),
            SW=round(meters_to_nm(snapshot.r34_quadrants_m.get("SW", 0) or 0), 1),
            NW=round(meters_to_nm(snapshot.r34_quadrants_m.get("NW", 0) or 0), 1),
        )

    # Look up latest radii audit confidence for this storm
    radii_confidence = None
    if snapshot:
        try:
            from services.wind_radii_audit import WindRadiiAuditor
            radii_confidence = WindRadiiAuditor.instance().get_latest_confidence(
                snapshot.storm_id
            )
        except Exception:
            pass  # Audit DB not yet populated is normal

    return IKEResponse(
        storm_id=result.storm_id,
        timestamp=result.timestamp,
        ike_total_tj=round(result.ike_total_tj, 2),
        ike_hurricane_tj=round(result.ike_hurricane_tj, 2),
        ike_tropical_storm_tj=round(result.ike_tropical_storm_tj, 2),
        ike_pretty=result.ike_total_pretty,
        lat=snapshot.lat if snapshot else None,
        lon=snapshot.lon if snapshot else None,
        wind_field_source=result.wind_field_source,
        max_wind_ms=round(snapshot.max_wind_ms, 1) if snapshot else None,
        min_pressure_hpa=round(snapshot.min_pressure_hpa, 1) if snapshot and snapshot.min_pressure_hpa else None,
        rmw_nm=round(meters_to_nm(snapshot.rmw_m), 1) if snapshot and snapshot.rmw_m else None,
        r34_nm=round(meters_to_nm(snapshot.r34_m), 1) if snapshot and snapshot.r34_m else None,
        r64_nm=round(meters_to_nm(snapshot.r64_m), 1) if snapshot and snapshot.r64_m else None,
        r34_quadrants=r34_quads,
        forward_speed_knots=round(ms_to_knots(snapshot.forward_speed_ms), 1) if snapshot and snapshot.forward_speed_ms else None,
        forward_direction_deg=round(snapshot.forward_direction_deg, 1) if snapshot and snapshot.forward_direction_deg is not None else None,
        radii_confidence=radii_confidence,
    )


async def _compute_ike_batch(
    snapshots: list[HurricaneSnapshot],
    grid_resolution_m: float,
    max_workers: int = 4,
) -> list[tuple]:
    """
    Compute IKE for multiple snapshots in parallel using asyncio.
    
    Args:
        snapshots: list of HurricaneSnapshot objects
        grid_resolution_m: grid resolution in meters
        max_workers: max concurrent computations (default 4)
    
    Returns:
        list of (ike_result, snapshot) tuples
    """
    results = []
    semaphore = asyncio.Semaphore(max_workers)
    
    async def compute_single(snap):
        """Compute IKE for one snapshot with concurrency limit."""
        async with semaphore:
            # Run CPU-intensive computation in dedicated thread pool
            loop = asyncio.get_event_loop()
            try:
                ike = await loop.run_in_executor(
                    _IKE_EXECUTOR,
                    compute_ike_from_snapshot,
                    snap,
                    grid_resolution_m
                )
                return (ike, snap)
            except Exception as e:
                logger.warning(f"IKE computation failed for snapshot: {e}")
                return None
    
    # Create tasks for all snapshots
    tasks = [compute_single(snap) for snap in snapshots]
    
    # Gather results in order
    batch_results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Filter out None results (failures)
    return [r for r in batch_results if r is not None]


def _load_custom_storms(min_year: int = 2015, max_year: int = 2099) -> list[dict]:
    """Load custom storms from local CSV file (for future years like 2025/2026)."""
    custom_path = Path(__file__).parent.parent / "data" / "custom_storms.csv"
    
    if not custom_path.exists():
        return []
    
    custom_storms = []
    try:
        with open(custom_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    year = int(row.get("YEAR", "0"))
                    if year < min_year or year > max_year:
                        continue
                    
                    wind_kt = int(row.get("PEAK_WIND_KT", "0"))
                    if wind_kt < 34:  # Skip sub-TS
                        continue
                    
                    custom_storms.append({
                        "id": row.get("ID", "").strip(),
                        "name": row.get("NAME", "").strip().title(),
                        "year": year,
                        "basin": row.get("BASIN", "").strip(),
                        "peak_wind_kt": wind_kt,
                        "category": int(row.get("CATEGORY", "0")),
                        "source": "custom",
                    })
                except (ValueError, KeyError):
                    continue
    except Exception as e:
        logger.warning(f"Failed to load custom storms: {e}")
        return []
    
    return custom_storms


def _load_custom_track(storm_id: str) -> list[HurricaneSnapshot]:
    """Load track data for a custom storm from local CSV file."""
    custom_path = Path(__file__).parent.parent / "data" / "custom_tracks.csv"
    
    if not custom_path.exists():
        return []
    
    snapshots = []
    try:
        # Look up storm name ONCE before the loop (was being called per-row)
        name = storm_id
        custom_catalog = _load_custom_storms(0, 9999)
        for entry in custom_catalog:
            if entry.get("id") == storm_id:
                name = entry.get("name", storm_id)
                break

        with open(custom_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("STORM_ID", "").strip() != storm_id:
                    continue

                try:
                    # Parse timestamp
                    timestamp_str = row.get("TIMESTAMP", "").strip()
                    timestamp = datetime.fromisoformat(timestamp_str)

                    # Convert forward speed from knots -> m/s (if present)
                    fwd_speed_knots = float(row.get("FORWARD_SPEED_KNOTS", "0") or 0)
                    forward_speed_ms = fwd_speed_knots * 0.514444

                    snap = HurricaneSnapshot(
                        storm_id=storm_id,
                        name=name,
                        timestamp=timestamp,
                        lat=float(row.get("LATITUDE", "0")),
                        lon=float(row.get("LONGITUDE", "0")),
                        max_wind_ms=float(row.get("MAX_WIND_MS", "0")),
                        min_pressure_hpa=float(row.get("MIN_PRESSURE_HPA", "1013")),
                        forward_speed_ms=forward_speed_ms,
                        rmw_m=float(row.get("RMW_NM", "20")) * 1852.0,
                    )
                    snapshots.append(snap)
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"Failed to parse custom track row: {e}")
                    continue
    except Exception as e:
        logger.warning(f"Failed to load custom track for {storm_id}: {e}")
        return []
    
    return snapshots


# ------------------------------------------------------------------
# Active storms
# ------------------------------------------------------------------

async def _refresh_active_storms(request: Request):
    """
    Background task to refresh active storms cache.
    Acquires lock to ensure only ONE refresh happens at a time (prevents dogpile).
    """
    global _active_storms_cache, _active_storms_cache_time
    async with _active_storms_lock:
        # Double-check: another task may have refreshed while we waited for the lock
        now = datetime.utcnow()
        if (_active_storms_cache_time and
                (now - _active_storms_cache_time) < _ACTIVE_STORMS_TTL):
            logger.debug("[ACTIVE_STORMS] Another task refreshed cache while we waited")
            return

        shared_client = getattr(request.app.state, "http_client", None)
        try:
            async with NOAAClient(http_client=shared_client) as client:
                storms = await asyncio.wait_for(
                    client.get_active_storms(), timeout=3.0
                )
                _active_storms_cache = storms
                _active_storms_cache_time = datetime.utcnow()
                logger.info(f"[ACTIVE_STORMS] Background refresh complete: {len(storms)} storms")
        except asyncio.TimeoutError:
            logger.warning("[ACTIVE_STORMS] Background refresh timed out, keeping stale cache")
        except httpx.PoolTimeout:
            logger.warning("[ACTIVE_STORMS] Connection pool exhausted, keeping stale cache")
        except Exception as e:
            logger.warning(f"[ACTIVE_STORMS] Background refresh failed: {e}, keeping stale cache")


@router.get("/storms/active", response_model=list[StormSummary])
async def list_active_storms(request: Request):
    """
    List all currently active tropical cyclones from NHC.

    Uses stale-while-revalidate caching pattern:
    - Fresh cache (< 2 min): return immediately, sub-millisecond
    - Stale cache exists: return immediately, kick off background refresh
    - Cold start: wait for first fetch, then cache

    This eliminates thundering herd and ensures sub-millisecond responses
    for all but the very first request.
    """
    global _active_storms_cache, _active_storms_cache_time

    now = datetime.utcnow()

    # Fast path: fresh cache exists
    if (_active_storms_cache is not None and _active_storms_cache_time
            and (now - _active_storms_cache_time) < _ACTIVE_STORMS_TTL):
        logger.debug(f"[ACTIVE_STORMS] Fresh cache hit — {len(_active_storms_cache)} storms")
        return [StormSummary(**s) for s in _active_storms_cache]

    # Stale cache exists? Return it immediately, refresh in background (non-blocking)
    if _active_storms_cache is not None:
        logger.debug(f"[ACTIVE_STORMS] Returning stale cache ({len(_active_storms_cache)} storms), refreshing in background")
        # Only kick off background refresh if not already refreshing
        if not _active_storms_lock.locked():
            asyncio.create_task(_refresh_active_storms(request))
        return [StormSummary(**s) for s in _active_storms_cache]

    # Cold start: must wait for first fetch
    logger.info("[ACTIVE_STORMS] Cold start, fetching from NOAA")
    await _refresh_active_storms(request)
    return [StormSummary(**s) for s in _active_storms_cache] if _active_storms_cache else []


@router.get("/storms/search", response_model=list[StormSummary])
async def search_storms(
    query: str = Query("", description="Storm name to search for"),
    basin: Optional[str] = Query(None, description="Basin code: NA, EP, WP, NI, SI, SP, SA"),
    year: Optional[int] = Query(None, description="Season year"),
    limit: int = Query(50, ge=1, le=1000, description="Max results to return"),
):
    """
    Search historical storm catalog by name, basin, and year.

    Returns list of storms matching the search criteria from IBTrACS/HURDAT2.
    If query is empty, returns an empty list.
    """
    if not query or query.strip() == "":
        return []

    async with NOAAClient() as client:
        try:
            # Search by name, year, and optional basin
            snapshots = await asyncio.wait_for(
                client.get_ibtracs_by_name_year(query.upper(), year, basin),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"IBTrACS search timeout for {query}")
            return []
        except httpx.PoolTimeout:
            logger.warning(f"Connection pool exhausted for IBTrACS search {query}")
            raise HTTPException(status_code=503, detail="Server under heavy load, try again")
        except NOAAClientError as e:
            logger.warning(f"IBTrACS search failed for {query}: {e}")
            return []
        except Exception as e:
            logger.warning(f"Unexpected error searching IBTrACS for {query}: {e}")
            return []

    if not snapshots:
        return []

    # Convert HurricaneSnapshot objects to StormSummary objects
    # Use the last snapshot (most recent) for each storm as representative
    results = []
    seen_ids = set()
    for snap in snapshots[-limit:]:  # Get the most recent observations
        if snap.storm_id not in seen_ids:
            seen_ids.add(snap.storm_id)
            # Convert wind speed from m/s to knots for display
            intensity_kt = ms_to_knots(snap.max_wind_ms) if snap.max_wind_ms else None
            # Derive movement direction/speed from forward motion parameters
            movement_str = None
            if snap.forward_direction_deg is not None and snap.forward_speed_ms is not None:
                movement_str = f"{ms_to_knots(snap.forward_speed_ms):.1f} kt towards {snap.forward_direction_deg:.0f}°"

            results.append(StormSummary(
                id=snap.storm_id,
                name=snap.name,
                classification=snap.category.name.replace("_", " ").title(),
                lat=snap.lat,
                lon=snap.lon,
                intensity_knots=intensity_kt,
                pressure_mb=snap.min_pressure_hpa,
                movement=movement_str,
                movement_speed_knots=ms_to_knots(snap.forward_speed_ms) if snap.forward_speed_ms else None,
                movement_direction_deg=snap.forward_direction_deg,
            ))

    return results


@router.get("/storms/{storm_id}/forecast")
async def get_storm_forecast(storm_id: str):
    """
    Fetch NHC forecast track and cone for an active storm.

    Returns GeoJSON-like data with forecast positions, the
    uncertainty cone polygon, and stall risk analysis computed
    from implied forward speeds between forecast positions.
    """
    async with NOAAClient() as client:
        try:
            forecast = await client.get_forecast_track(storm_id)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    # ── Stall Risk Analysis ──
    # Compute implied forward speed between consecutive forecast positions
    # using Haversine distance / time delta. Flag stall risk when forecast
    # speeds drop below thresholds.
    forecast["stall_risk"] = _compute_stall_risk(forecast.get("forecast_track", []))

    return forecast


def _compute_stall_risk(forecast_track: list[dict]) -> dict:
    """
    Analyze forecast positions for stall risk.

    Uses Haversine distance between consecutive NHC forecast positions
    to compute implied forward speed (knots). A stall is flagged when:
      - Any forecast segment < 5 kt (near-stall)
      - Mean forecast speed over 48h < 8 kt (slow-mover)
      - Consecutive segments < 8 kt for 24+ hours (persistent slow motion)

    Returns dict with:
      - risk_level: "none" | "low" | "moderate" | "high" | "extreme"
      - risk_score: 0-100
      - min_forecast_speed_kt: slowest implied segment speed
      - mean_forecast_speed_kt: average over all segments
      - slow_hours: total forecast hours below 8 kt
      - stall_hours: total forecast hours below 5 kt
      - segments: list of {hour_start, hour_end, speed_kt, lat, lon}
      - description: human-readable stall risk summary
    """
    import math

    result = {
        "risk_level": "none",
        "risk_score": 0,
        "min_forecast_speed_kt": None,
        "mean_forecast_speed_kt": None,
        "slow_hours": 0,
        "stall_hours": 0,
        "segments": [],
        "description": "Insufficient forecast data"
    }

    if not forecast_track or len(forecast_track) < 2:
        return result

    # Sort by forecast hour (TAU)
    pts = sorted(forecast_track, key=lambda p: p.get("hour", 0))

    def haversine_nm(lat1, lon1, lat2, lon2):
        """Great-circle distance in nautical miles."""
        R_nm = 3440.065  # Earth radius in nm
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) ** 2)
        return 2 * R_nm * math.asin(math.sqrt(a))

    segments = []
    speeds = []
    slow_hours = 0
    stall_hours = 0

    for i in range(len(pts) - 1):
        p1, p2 = pts[i], pts[i + 1]
        h1 = p1.get("hour", 0)
        h2 = p2.get("hour", 0)
        dt_hours = h2 - h1
        if dt_hours <= 0:
            continue

        lat1, lon1 = p1.get("lat", 0), p1.get("lon", 0)
        lat2, lon2 = p2.get("lat", 0), p2.get("lon", 0)
        if not all([lat1, lon1, lat2, lon2]):
            continue

        dist_nm = haversine_nm(lat1, lon1, lat2, lon2)
        speed_kt = dist_nm / dt_hours

        segments.append({
            "hour_start": h1,
            "hour_end": h2,
            "speed_kt": round(speed_kt, 1),
            "lat": lat2,
            "lon": lon2,
        })
        speeds.append(speed_kt)

        if speed_kt < 8:
            slow_hours += dt_hours
        if speed_kt < 5:
            stall_hours += dt_hours

    if not speeds:
        return result

    min_speed = min(speeds)
    mean_speed = sum(speeds) / len(speeds)

    # ── Risk Scoring ──
    # Combines: minimum forecast speed, slow-motion persistence, and near-stall duration
    #
    # Risk = 40 × speed_factor + 35 × persistence_factor + 25 × stall_factor
    #
    # speed_factor: how slow the slowest forecast segment is
    #   < 3 kt → 1.0, 3-6 kt → 0.5-1.0, 6-10 kt → 0.1-0.5, > 10 kt → 0
    speed_factor = max(0, 1.0 - min_speed / 10.0)

    # persistence_factor: how many hours below 8 kt (Harvey had 72+ hours)
    persistence_factor = min(1.0, slow_hours / 48.0)

    # stall_factor: how many hours of near-stall (< 5 kt)
    stall_factor = min(1.0, stall_hours / 24.0)

    risk_score = round(40 * speed_factor + 35 * persistence_factor + 25 * stall_factor)
    risk_score = min(100, max(0, risk_score))

    # Risk level thresholds
    if risk_score >= 70:
        risk_level = "extreme"
    elif risk_score >= 50:
        risk_level = "high"
    elif risk_score >= 30:
        risk_level = "moderate"
    elif risk_score >= 15:
        risk_level = "low"
    else:
        risk_level = "none"

    # Human-readable description
    if risk_level == "extreme":
        desc = f"EXTREME stall risk — forecast shows near-stall ({min_speed:.0f} kt) for {stall_hours:.0f}+ hours. Catastrophic rainfall potential (Harvey-like scenario)."
    elif risk_level == "high":
        desc = f"HIGH stall risk — forecast shows slow motion ({min_speed:.0f} kt min) with {slow_hours:.0f}h below 8 kt. Significant rainfall flooding threat."
    elif risk_level == "moderate":
        desc = f"Moderate stall risk — slowing forecast ({min_speed:.0f} kt min, {slow_hours:.0f}h slow). Enhanced rainfall expected near landfall."
    elif risk_level == "low":
        desc = f"Low stall risk — some deceleration forecast ({min_speed:.0f} kt min). Minor rainfall enhancement possible."
    else:
        desc = f"No stall risk — storm maintaining forward speed ({mean_speed:.0f} kt avg). Standard rainfall expected."

    result.update({
        "risk_level": risk_level,
        "risk_score": risk_score,
        "min_forecast_speed_kt": round(min_speed, 1),
        "mean_forecast_speed_kt": round(mean_speed, 1),
        "slow_hours": slow_hours,
        "stall_hours": stall_hours,
        "segments": segments,
        "description": desc,
    })
    return result


# ------------------------------------------------------------------
# Sea Surface Temperature along track
# ------------------------------------------------------------------

@router.post("/sst/track")
async def get_sst_along_track(points: list[dict] = Body(...)):
    """
    Fetch sea surface temperature from ERDDAP for a list of track points.

    Expects a JSON array of {lat, lon, timestamp} objects.
    Returns SST (°C) at each point from the NOAA Geo-polar Blended SST dataset.
    """
    from services.source_health import SourceHealthMonitor
    monitor = SourceHealthMonitor.instance()

    logger.info(f"[SST] Request received: {len(points)} track points")
    if points:
        logger.debug(f"[SST] First point: {points[0]}")
        logger.debug(f"[SST] Last point:  {points[-1]}")
    t0 = time.time()
    async with NOAAClient() as client:
        try:
            sst_data = await client.get_sst_along_track(points)
            valid_count = sum(1 for s in sst_data if s.get("sst_c") is not None)
            elapsed_ms = (time.time() - t0) * 1000
            logger.info(f"[SST] Response: {valid_count}/{len(sst_data)} points have valid SST data")
            if valid_count > 0:
                monitor.record_success("erddap_sst", latency_ms=elapsed_ms)
            else:
                monitor.record_failure("erddap_sst", error="All SST values null", latency_ms=elapsed_ms)
                if sst_data:
                    logger.warning(f"[SST] WARNING: All SST values null! First result: {sst_data[0]}")
        except Exception as e:
            elapsed_ms = (time.time() - t0) * 1000
            monitor.record_failure("erddap_sst", error=str(e), latency_ms=elapsed_ms)
            logger.error(f"[SST] ERROR: {type(e).__name__}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    return sst_data


# ------------------------------------------------------------------
# IKE computation
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/ike", response_model=IKEResponse)
async def get_storm_ike(
    request: Request,
    storm_id: str,
    grid_resolution_km: float = Query(5.0, ge=1.0, le=50.0),
):
    """
    Compute IKE for an active or recent storm by ATCF ID.

    Uses the best available wind field source:
      1. Real gridded data from GFS if available
      2. Asymmetric parametric model if quadrant radii are present
      3. Symmetric Holland model as fallback
    """
    # FIX 1: Use shared http_client from app.state
    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
        try:
            snapshot = await client.get_storm_snapshot(storm_id)
        except httpx.PoolTimeout:
            logger.warning(f"Connection pool exhausted for {storm_id}")
            raise HTTPException(status_code=503, detail="Server under heavy load, try again")
        except NOAAClientError as e:
            raise HTTPException(status_code=404, detail=str(e))

        # Try to get gridded data
        try:
            grid = await client.get_gridded_wind_field(
                storm_id, snapshot.lat, snapshot.lon
            )
            if grid is not None:
                snapshot.wind_field = grid
        except httpx.PoolTimeout:
            logger.warning(f"Connection pool exhausted for gridded wind field {storm_id}")
            # Log but don't fail — fall back to parametric model

    try:
        result = compute_ike_from_snapshot(
            snapshot, grid_resolution_m=grid_resolution_km * 1000
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return _ike_to_response(result, snapshot)


@router.post("/ike/compute", response_model=IKEResponse)
async def compute_custom_ike(input_data: SnapshotInput):
    """
    Compute IKE from manually provided storm parameters.

    If quadrant radii (r34_ne_nm, etc.) are provided, uses the asymmetric
    wind field model. Otherwise falls back to symmetric Holland model.
    """
    # Build quadrant dicts if provided
    r34_quadrants = None
    has_quads = any([
        input_data.r34_ne_nm, input_data.r34_se_nm,
        input_data.r34_sw_nm, input_data.r34_nw_nm,
    ])
    if has_quads:
        r34_quadrants = {
            "NE": nm_to_meters(input_data.r34_ne_nm) if input_data.r34_ne_nm else None,
            "SE": nm_to_meters(input_data.r34_se_nm) if input_data.r34_se_nm else None,
            "SW": nm_to_meters(input_data.r34_sw_nm) if input_data.r34_sw_nm else None,
            "NW": nm_to_meters(input_data.r34_nw_nm) if input_data.r34_nw_nm else None,
        }

    # Compute max r34 from quadrants or use the scalar
    r34_m = None
    if r34_quadrants:
        r34_vals = [v for v in r34_quadrants.values() if v]
        r34_m = max(r34_vals) if r34_vals else None
    elif input_data.r34_nm:
        r34_m = nm_to_meters(input_data.r34_nm)

    snapshot = HurricaneSnapshot(
        storm_id=input_data.storm_id,
        name=input_data.name,
        timestamp=datetime.utcnow(),
        lat=input_data.lat,
        lon=input_data.lon,
        max_wind_ms=knots_to_ms(input_data.max_wind_knots),
        min_pressure_hpa=input_data.min_pressure_hpa,
        rmw_m=nm_to_meters(input_data.rmw_nm) if input_data.rmw_nm else None,
        r34_m=r34_m,
        r34_quadrants_m=r34_quadrants,
        forward_speed_ms=knots_to_ms(input_data.forward_speed_knots) if input_data.forward_speed_knots else None,
        forward_direction_deg=input_data.forward_direction_deg,
    )

    try:
        result = compute_ike_from_snapshot(
            snapshot, grid_resolution_m=input_data.grid_resolution_km * 1000
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return _ike_to_response(result, snapshot)


# ------------------------------------------------------------------
# Full valuation
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/value", response_model=ValuationResponse)
async def get_storm_valuation(
    request: Request,
    storm_id: str,
    grid_resolution_km: float = Query(5.0, ge=1.0, le=50.0),
):
    """
    Compute full destructive value for a storm.

    Combines IKE, surge threat, and intensification rate into
    a composite 0-100 score.
    """
    global _valuation_cache, _valuation_cache_lock

    # Check cache (no lock needed for reads in asyncio single-threaded event loop)
    now = datetime.utcnow()
    cache_key = storm_id
    cached = _valuation_cache.get(cache_key)
    if cached:
        cached_response, cached_time = cached
        if (now - cached_time) < _VALUATION_CACHE_TTL:
            logger.debug(f"[VALUATION] Cache hit for {storm_id}")
            return cached_response

    # Cache miss — acquire lock to prevent dogpile
    async with _valuation_cache_lock:
        # Double-check after acquiring lock (another request may have filled cache)
        cached = _valuation_cache.get(cache_key)
        if cached:
            cached_response, cached_time = cached
            if (now - cached_time) < _VALUATION_CACHE_TTL:
                logger.debug(f"[VALUATION] Cache hit for {storm_id} (after lock acquisition)")
                return cached_response

        # Actually compute (only ONE request does this)
        # Use shared http_client from app.state
        shared_client = getattr(request.app.state, "http_client", None)
        async with NOAAClient(http_client=shared_client) as client:
            try:
                snapshot = await asyncio.wait_for(
                    client.get_storm_snapshot(storm_id), timeout=8.0
                )
            except asyncio.TimeoutError:
                raise HTTPException(status_code=504, detail="Snapshot retrieval timed out")
            except httpx.PoolTimeout:
                logger.warning(f"Connection pool exhausted for {storm_id}")
                raise HTTPException(status_code=503, detail="Server under heavy load, try again")
            except NOAAClientError as e:
                raise HTTPException(status_code=404, detail=str(e))

            grid = None
            try:
                grid = await client.get_gridded_wind_field(
                    storm_id, snapshot.lat, snapshot.lon
                )
            except asyncio.TimeoutError:
                logger.warning(f"Wind field grid retrieval timed out for {storm_id}, falling back to parametric wind")
            except httpx.PoolTimeout:
                logger.warning(f"Connection pool exhausted for wind field {storm_id}, falling back to parametric wind")
            except Exception as e:
                logger.warning(f"Failed to retrieve wind field grid for {storm_id}: {e}, falling back to parametric wind")

            if grid is not None:
                snapshot.wind_field = grid

        try:
            valuation = compute_valuation(
                snapshot, grid_resolution_m=grid_resolution_km * 1000
            )
        except (ValueError, TypeError, RuntimeError, Exception) as e:
            logger.error(f"Valuation computation failed for {storm_id}: {e}")
            raise HTTPException(status_code=422, detail=str(e))

        response = ValuationResponse(
            storm_id=valuation.storm_id,
            name=valuation.name,
            timestamp=valuation.ike_result.timestamp,
            ike=_ike_to_response(valuation.ike_result, snapshot),
            destructive_potential=round(valuation.destructive_potential, 1),
            surge_threat=round(valuation.surge_threat, 1) if valuation.surge_threat is not None else None,
            overall_value=round(valuation.overall_value, 1) if valuation.overall_value is not None else None,
            category=snapshot.category.name.replace("_", " ").title(),
        )

        # Cache the result
        _valuation_cache[cache_key] = (response, datetime.utcnow())
        logger.info(f"[VALUATION] Cached result for {storm_id}")
        return response



# ------------------------------------------------------------------
# Historical track with IKE timeline (HURDAT2)
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/history", response_model=list[IKEResponse])
async def get_historical_ike_track(
    storm_id: str,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0),
):
    """
    Compute IKE at every 6-hour best-track observation for a historical storm.

    Uses HURDAT2 extended format with quadrant wind radii when available,
    enabling asymmetric IKE computation for storms after ~2004.
    """
    async with NOAAClient() as client:
        try:
            snapshots = await client.get_historical_track(storm_id)
        except httpx.PoolTimeout:
            logger.warning(f"Connection pool exhausted for historical track {storm_id}")
            raise HTTPException(status_code=503, detail="Server under heavy load, try again")
        except NOAAClientError as e:
            raise HTTPException(status_code=404, detail=str(e))

    if not snapshots:
        raise HTTPException(status_code=404, detail=f"No data for {storm_id}")

    # Compute IKE in parallel (not serial) for all snapshots
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)
    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    return results


# ------------------------------------------------------------------
# Storm catalog (lightweight listing from HURDAT2)
# ------------------------------------------------------------------

@router.get("/storms/catalog")
async def get_storm_catalog(
    min_year: int = Query(2015, ge=1851, le=2099),
    max_year: int = Query(2099, ge=1851, le=2099),
):
    """
    Get a lightweight catalog of all named storms.

    Primary source: IBTrACS (global, quality-controlled, updated frequently).
    Fallback: HURDAT2 (Atlantic/East Pacific only, annual reanalysis).

    Returns storm ID, name, year, peak wind (kt), and Saffir-Simpson category
    without computing IKE. Fast enough for populating UI storm pickers.
    """
    catalog = []
    async with NOAAClient() as client:
        # Primary: IBTrACS (more reliable servers, global coverage)
        try:
            catalog = await client.get_ibtracs_catalog(min_year, max_year)
            logger.info(f"[CATALOG] IBTrACS loaded: {len(catalog)} storms ({min_year}-{max_year})")
        except Exception as e:
            logger.error(f"[CATALOG] IBTrACS failed: {type(e).__name__}: {str(e)}")

        # Fallback: HURDAT2 (if IBTrACS returned nothing)
        if not catalog:
            try:
                catalog = await client.get_storm_catalog(min_year, max_year)
                logger.info(f"[CATALOG] HURDAT2 fallback: {len(catalog)} storms ({min_year}-{max_year})")
            except Exception as e:
                logger.error(f"[CATALOG] HURDAT2 also failed: {type(e).__name__}: {str(e)}")
                logger.warning("Both IBTrACS and HURDAT2 failed — returning empty catalog")
                catalog = []

    return catalog


# ------------------------------------------------------------------
# Unified track endpoint (auto-routes HURDAT2 vs IBTrACS)
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/track", response_model=list[IKEResponse])
async def get_storm_track(
    storm_id: str,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0, description="Wind field grid spacing"),
    skip_points: int = Query(0, ge=0, le=10, description="Skip N points between calculations (0=all points)"),
):
    """
    Unified storm track endpoint that auto-detects the data source.

    - Custom storms (SI/NI/SP prefixes from data/custom_storms.csv) -> Custom tracks
    - ATCF IDs starting with AL or EP (e.g., AL092008) -> HURDAT2
    - IBTrACS SIDs (e.g., 2008245N17323) -> IBTrACS by SID
    - Other basin prefixes (SH, WP, IO, CP, etc.) -> IBTrACS name search

    Falls back to IBTrACS search if HURDAT2 lookup fails.
    
    Query parameters:
    - grid_resolution_km: Wind field grid spacing (1-50 km, default 10)
    - skip_points: Skip N points between IKE calculations (0-10, default 0 for all)
      Setting to 1 means calculate every other point (2x faster), 2 = every third (3x faster)
    """
    # --- Check IKE cache FIRST — skip all network I/O if already computed ---
    cached = _load_ike_cache(storm_id, grid_resolution_km, skip_points)
    if cached:
        logger.info(f"[CACHE HIT] {storm_id} — returning {len(cached)} cached IKE results")
        return JSONResponse(content=cached)

    prefix = storm_id[:2].upper()
    snapshots = []
    source = None

    # 1) Try custom storms first (local CSV, no network needed)
    if prefix in ("SI", "NI", "SP", "WP"):
        snapshots = _load_custom_track(storm_id)
        if snapshots:
            source = "custom"

    # 2-5) All remote lookups share a SINGLE NOAAClient (one HTTP session)
    #
    # Priority order (optimized for reliability and data quality):
    #   IBTrACS (NCEI) — global, quality-controlled, ingests HURDAT2 + all agencies
    #   HURDAT2/EBTRK (NHC) — Atlantic-only, flaky servers, annual reanalysis only
    #
    # IBTrACS is primary because:
    #   - NCEI servers are more reliable than NHC
    #   - Includes wind radii from USA agency (same data as HURDAT2)
    #   - "Last 3 years" file updates regularly during active seasons
    #   - Global coverage (not just Atlantic/East Pacific)
    #   - HURDAT2 only does annual reanalysis; IBTrACS updates more frequently
    if not snapshots:
        cache_dir = _PERSISTENT_DATA / "cache"
        async with NOAAClient(cache_dir=str(cache_dir)) as client:
            from services.source_health import SourceHealthMonitor
            _monitor = SourceHealthMonitor.instance()

            # 2) IBTrACS by SID (format: YYYYDDDNxxyyy — starts with digit)
            if not snapshots and storm_id[0].isdigit():
                t0 = time.time()
                try:
                    snapshots = await client.get_ibtracs_track(storm_id)
                    source = "ibtracs"
                    _monitor.record_success("ibtracs", latency_ms=(time.time() - t0) * 1000)
                except (NOAAClientError, Exception):
                    _monitor.record_failure("ibtracs", error="SID lookup failed", latency_ms=(time.time() - t0) * 1000)
                    snapshots = []

            # 3) IBTrACS by ATCF ID for AL/EP storms (e.g. AL092017)
            if not snapshots and prefix in ("AL", "EP") and len(storm_id) == 8:
                t0 = time.time()
                try:
                    csv_text = await client._fetch_ibtracs(use_recent=True)
                    snapshots = _search_ibtracs_by_atcf_id(client, csv_text, storm_id)
                    if not snapshots:
                        csv_text = await client._fetch_ibtracs(use_recent=False)
                        snapshots = _search_ibtracs_by_atcf_id(client, csv_text, storm_id)
                    if snapshots:
                        source = "ibtracs"
                        _monitor.record_success("ibtracs", latency_ms=(time.time() - t0) * 1000)
                    else:
                        _monitor.record_failure("ibtracs", error="ATCF ID not found", latency_ms=(time.time() - t0) * 1000)
                except (NOAAClientError, Exception):
                    _monitor.record_failure("ibtracs", error="ATCF lookup failed", latency_ms=(time.time() - t0) * 1000)
                    snapshots = []

            # 4) Fallback: HURDAT2/EBTRK for Atlantic/East Pacific ATCF IDs
            #    Only try if IBTrACS didn't have it (e.g. very recent advisory data)
            if not snapshots and prefix in ("AL", "EP") and len(storm_id) == 8:
                if NOAAClient.nhc_is_down():
                    logger.info(f"[TRACK] Skipping HURDAT2 for {storm_id} — NHC recently unreachable")
                else:
                    t0 = time.time()
                    try:
                        snapshots = await client.get_historical_track(storm_id)
                        source = "hurdat2"
                        _monitor.record_success("hurdat2", latency_ms=(time.time() - t0) * 1000)
                    except (NOAAClientError, Exception):
                        _monitor.record_failure("hurdat2", error="HURDAT2 lookup failed", latency_ms=(time.time() - t0) * 1000)
                        snapshots = []

            # 5) Last resort: IBTrACS full archive by SID
            if not snapshots:
                try:
                    snapshots = await client.get_ibtracs_track(storm_id, use_recent=False)
                    source = "ibtracs"
                except (NOAAClientError, Exception):
                    snapshots = []

            # 6) Name-based fallback: if storm_id looks like a plain name
            #    (e.g. "KATRINA", "MICHAEL"), search IBTrACS by name.
            #    Handles requests like /storms/KATRINA/track that don't match
            #    any ATCF ID or SID pattern.
            if not snapshots and storm_id.isalpha():
                try:
                    snapshots = await client.get_ibtracs_by_name(
                        storm_id.upper(), basin=None
                    )
                    if snapshots:
                        source = "ibtracs"
                        logger.info(f"[TRACK] Name-based fallback matched {storm_id} → {len(snapshots)} points")
                except (NOAAClientError, Exception):
                    snapshots = []

    if not snapshots:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for \"{storm_id}\". "
            f"Try an ATCF ID (AL092008), IBTrACS SID, or storm name + year."
        )

    t0 = time.time()

    # Sample snapshots if skip_points > 0 (every Nth point)
    if skip_points > 0:
        sampled = []
        for i, snap in enumerate(snapshots):
            if i % (skip_points + 1) == 0:
                sampled.append(snap)
        snapshots = sampled

    # Compute IKE in parallel for all snapshots
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)

    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    # --- Save to cache for future requests ---
    compute_ms = (time.time() - t0) * 1000
    _save_ike_cache(
        storm_id, grid_resolution_km, skip_points,
        [_ike_response_to_dict(r) for r in results],
        source=source or "unknown",
        compute_ms=compute_ms,
    )
    logger.info(f"[CACHE MISS] {storm_id} — computed {len(results)} IKE results in {compute_ms:.0f}ms, saved to cache")

    return results


# ------------------------------------------------------------------
# IKE Cache management endpoints
# ------------------------------------------------------------------

@router.get("/cache/stats")
async def get_cache_stats():
    """Return IKE cache statistics."""
    files = list(_IKE_CACHE_DIR.glob("*.json"))
    total_size = sum(f.stat().st_size for f in files)
    storms = set()
    for f in files:
        # Extract storm_id from filename (everything before the hash)
        parts = f.stem.rsplit("_", 1)
        if len(parts) == 2:
            storms.add(parts[0])
    return {
        "cached_storms": len(storms),
        "cache_files": len(files),
        "total_size_mb": round(total_size / 1024 / 1024, 2),
        "max_files": _IKE_CACHE_MAX_FILES,
        "max_size_mb": _IKE_CACHE_MAX_SIZE_MB,
        "cache_version": _IKE_CACHE_VERSION,
        "cache_dir": str(_IKE_CACHE_DIR),
    }


@router.delete("/cache/ike/{storm_id}")
async def clear_storm_cache(storm_id: str):
    """Clear cached IKE results for a specific storm."""
    cleared = 0
    for f in _IKE_CACHE_DIR.glob(f"{storm_id}_*.json"):
        f.unlink()
        cleared += 1
    return {"cleared": cleared, "storm_id": storm_id}


@router.delete("/cache/ike")
async def clear_all_ike_cache():
    """Clear all cached IKE results (forces full recomputation)."""
    cleared = 0
    for f in _IKE_CACHE_DIR.glob("*.json"):
        f.unlink()
        cleared += 1
    return {"cleared": cleared, "message": "All IKE cache cleared"}


# ------------------------------------------------------------------
# Preset storm IDs for preloading (matches frontend PRESETS array)
# ------------------------------------------------------------------
PRESET_STORM_IDS = [
    "AL122005",  # Katrina
    "AL092024",  # Helene
    "AL152017",  # Maria
    "AL112017",  # Irma
    "AL092022",  # Ian
    "AL142018",  # Michael
    "AL142024",  # Milton
    "AL182012",  # Sandy
    "AL092008",  # Ike
    "AL092017",  # Harvey
    "AL052019",  # Dorian
    "AL062018",  # Florence
    "AL102023",  # Idalia
    "AL022024",  # Beryl
]

_PRELOAD_BUNDLE_PATH = _PERSISTENT_DATA / "cache" / "preload_bundle.json"


def _build_preload_bundle_sync() -> dict:
    """
    Build preload bundle synchronously (runs in thread pool via run_in_executor).
    Performs all blocking file I/O without holding the event loop.
    """
    bundle = {}

    # 1) Load preset storms from individual IKE cache files
    default_grid_res = 15.0
    default_skip = 1
    for storm_id in PRESET_STORM_IDS:
        cached = _load_ike_cache(storm_id, default_grid_res, default_skip)
        if cached:
            bundle[storm_id] = cached

    # 2) Also check for active storms that may have been cached
    try:
        for f in _IKE_CACHE_DIR.glob("*.json"):
            parts = f.stem.rsplit("_", 1)
            if len(parts) == 2:
                sid = parts[0]
                if sid not in bundle:
                    try:
                        data = json.loads(f.read_text())
                        if data.get("_version") == _IKE_CACHE_VERSION and data.get("results"):
                            bundle[sid] = data["results"]
                    except (json.JSONDecodeError, KeyError):
                        pass
    except Exception:
        pass

    return {
        "version": _IKE_CACHE_VERSION,
        "storm_count": len(bundle),
        "storms": bundle,
    }


@router.get("/preload")
async def get_preload_bundle():
    """
    Return a preloaded data bundle containing all formula inputs for preset
    storms. The frontend loads this on startup so DPS/IKE formulas always
    have complete data without waiting for per-storm API calls.

    FIX 1: Response is cached in-memory with 5-minute TTL, and the bundle
    is built in a background thread to avoid blocking the event loop.

    Returns a dict of storm_id -> list of IKEResponse dicts.
    Also includes any active storms that have cached data.
    """
    global _preload_cache, _preload_cache_time
    now = datetime.utcnow()

    # Return cached if fresh
    if (_preload_cache is not None and _preload_cache_time
            and (now - _preload_cache_time) < _PRELOAD_CACHE_TTL):
        return JSONResponse(content=_preload_cache)

    # Build in background thread to avoid blocking event loop
    async with _preload_lock:
        # Double-check after lock to prevent dogpile
        if (_preload_cache is not None and _preload_cache_time
                and (datetime.utcnow() - _preload_cache_time) < _PRELOAD_CACHE_TTL):
            return JSONResponse(content=_preload_cache)

        # Move ALL file I/O to a thread pool
        loop = asyncio.get_event_loop()
        bundle = await loop.run_in_executor(_IKE_EXECUTOR, _build_preload_bundle_sync)

        _preload_cache = bundle
        _preload_cache_time = datetime.utcnow()
        return JSONResponse(content=bundle)


@router.post("/preload/generate")
async def generate_preload_bundle(
    grid_resolution_km: float = Query(15.0, ge=1.0, le=50.0),
    skip_points: int = Query(1, ge=0, le=10),
):
    """
    Pre-compute and cache IKE data for all preset storms that are not
    already cached. This ensures the preload bundle has complete data.

    Returns stats on what was computed vs. already cached.
    """
    results = {"already_cached": [], "computed": [], "failed": []}

    # Separate cached from uncached storms
    to_compute = []
    for storm_id in PRESET_STORM_IDS:
        cached = _load_ike_cache(storm_id, grid_resolution_km, skip_points)
        if cached:
            results["already_cached"].append(storm_id)
        else:
            to_compute.append(storm_id)

    # Compute all uncached storms in parallel (not serial)
    if to_compute:
        async def _compute_one(sid):
            try:
                await get_storm_track(sid, grid_resolution_km, skip_points)
                results["computed"].append(sid)
                logger.info(f"[PRELOAD] Computed and cached {sid}")
            except Exception as e:
                results["failed"].append({"storm_id": sid, "error": str(e)})
                logger.warning(f"[PRELOAD] Failed to compute {sid}: {e}")

        # Run up to 3 at a time to avoid overloading NOAA
        sem = asyncio.Semaphore(3)
        async def _throttled(sid):
            async with sem:
                await _compute_one(sid)

        await asyncio.gather(*[_throttled(sid) for sid in to_compute])

    return {
        "total_presets": len(PRESET_STORM_IDS),
        "already_cached": len(results["already_cached"]),
        "newly_computed": len(results["computed"]),
        "failed": len(results["failed"]),
        "details": results,
    }


# ------------------------------------------------------------------
# Global storm catalog (IBTrACS — all basins)
# ------------------------------------------------------------------

async def _build_global_catalog() -> list[dict]:
    """Build or load a cached global storm catalog (IBTrACS + custom storms)."""

    global _GLOBAL_IBTRACS_CATALOG_CACHE, _GLOBAL_IBTRACS_CATALOG_TIMESTAMP

    now = datetime.utcnow()

    # In-memory cache (fast)
    if (
        _GLOBAL_IBTRACS_CATALOG_CACHE
        and _GLOBAL_IBTRACS_CATALOG_TIMESTAMP
        and (now - _GLOBAL_IBTRACS_CATALOG_TIMESTAMP) < _GLOBAL_IBTRACS_CATALOG_TTL
    ):
        logger.info("Using in-memory global catalog cache")
        return _GLOBAL_IBTRACS_CATALOG_CACHE

    # Disk cache (helps across restarts)
    try:
        if _GLOBAL_IBTRACS_CACHE_FILE.exists():
            mtime = datetime.fromtimestamp(_GLOBAL_IBTRACS_CACHE_FILE.stat().st_mtime)
            if (now - mtime) < _GLOBAL_IBTRACS_CATALOG_TTL:
                with open(_GLOBAL_IBTRACS_CACHE_FILE, "r", encoding="utf-8") as f:
                    catalog = json.load(f)
                logger.info("Loaded global catalog from disk cache")
                _GLOBAL_IBTRACS_CATALOG_CACHE = catalog
                _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = now
                return catalog
    except Exception as e:
        logger.debug(f"Failed to read global catalog cache: {e}")

    # FIX 4: Protect catalog building with lock — only one request fetches from IBTrACS
    async with _catalog_lock:
        # Double-check cache after acquiring lock (another request may have built it)
        if (
            _GLOBAL_IBTRACS_CATALOG_CACHE
            and _GLOBAL_IBTRACS_CATALOG_TIMESTAMP
            and (datetime.utcnow() - _GLOBAL_IBTRACS_CATALOG_TIMESTAMP) < _GLOBAL_IBTRACS_CATALOG_TTL
        ):
            logger.info("Using in-memory global catalog cache (loaded while waiting for lock)")
            return _GLOBAL_IBTRACS_CATALOG_CACHE

        # Fetch from IBTrACS (may be slow on first run) but time out if it takes too long
        catalog = []
        cache_dir = _PERSISTENT_DATA / "cache"
        async with NOAAClient(timeout=60.0, cache_dir=str(cache_dir)) as client:
            try:
                catalog = await asyncio.wait_for(
                    client.get_ibtracs_catalog(1851, 2099), timeout=45.0
                )
                logger.info(f"Fetched IBTrACS catalog: {len(catalog)} storms")
            except asyncio.TimeoutError:
                logger.warning("IBTrACS catalog fetch timed out; returning custom storms only")
            except Exception as e:
                logger.warning(f"IBTrACS catalog fetch failed: {type(e).__name__}: {e}")

        # Merge custom storms (future years, etc.)
        try:
            custom = _load_custom_storms(1851, 2099)
            logger.info(f"Loaded {len(custom)} custom storms")
            existing_ids = {s.get("id", "") for s in catalog if s}
            for storm in custom:
                sid = storm.get("id", "")
                if sid and sid not in existing_ids:
                    catalog.append(storm)
                    existing_ids.add(sid)
        except Exception as e:
            logger.warning(f"Custom storms load failed: {e}")

        # Cache for quick future responses
        _GLOBAL_IBTRACS_CATALOG_CACHE = catalog
        _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = now
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            with open(_GLOBAL_IBTRACS_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(catalog, f)
        except Exception as e:
            logger.debug(f"Could not write global catalog cache: {e}")

        return catalog


@router.get("/storms/catalog/global")
async def get_global_storm_catalog(
    min_year: int = Query(2015, ge=1851, le=2099),
    max_year: int = Query(2099, ge=1851, le=2099),
):
    """
    Get a global catalog of all named storms from IBTrACS (all basins) + custom data.

    Returns storm ID (IBTrACS SID), name, year, basin, peak wind (kt),
    and Saffir-Simpson category. Includes custom storms for future years (2025+).
    """
    catalog = await _build_global_catalog()
    if not catalog:
        return []

    # Filter by requested years
    return [s for s in catalog if min_year <= s.get("year", 0) <= max_year]


@router.get("/storms/catalog/custom")
async def get_custom_storms_endpoint(
    min_year: int = Query(2015, ge=1851, le=2099),
    max_year: int = Query(2099, ge=1851, le=2099),
):
    """
    Get custom storms (for future years like 2025/2026).
    
    These storms are stored locally in data/custom_storms.csv and can be edited.
    They're also automatically merged into /storms/catalog/global.
    """
    return _load_custom_storms(min_year, max_year)


# ------------------------------------------------------------------
# IBTrACS endpoints (global coverage)
# ------------------------------------------------------------------

@router.get("/ibtracs/track/{sid}", response_model=list[IKEResponse])
async def get_ibtracs_track(
    sid: str,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0),
):
    """
    Fetch storm track from IBTrACS and compute IKE at each observation.

    IBTrACS covers all global basins (Atlantic, W. Pacific, Indian Ocean, etc.)
    and provides wind radii from multiple meteorological agencies.

    Args:
        sid: IBTrACS storm ID (e.g., '2005236N23285' for Katrina)
    """
    async with NOAAClient() as client:
        try:
            snapshots = await client.get_ibtracs_track(sid)
        except (NOAAClientError, Exception) as e:
            raise HTTPException(status_code=404, detail=str(e))

    if not snapshots:
        raise HTTPException(status_code=404, detail=f"No IBTrACS data for {sid}")

    # Compute IKE in parallel (not serial)
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)
    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    return results


@router.post("/ibtracs/search", response_model=list[IKEResponse])
async def search_ibtracs(
    search: IBTrACSSearchInput,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0),
):
    """
    Search IBTrACS by storm name and optional year, compute IKE for each observation.

    If year is omitted, searches for all storms with the given name and returns
    the most recent match (useful when users only know the storm name).
    """
    async with NOAAClient() as client:
        if search.year:
            # Exact name+year search (original behavior)
            try:
                snapshots = await client.get_ibtracs_by_name_year(
                    search.name, search.year, search.basin
                )
            except (NOAAClientError, Exception) as e:
                raise HTTPException(status_code=404, detail=str(e))
        else:
            # Name-only search: scan IBTrACS for all matches, pick most recent
            try:
                snapshots = await client.get_ibtracs_by_name(
                    search.name, search.basin
                )
            except (NOAAClientError, Exception) as e:
                raise HTTPException(status_code=404, detail=str(e))

    if not snapshots:
        year_str = f" ({search.year})" if search.year else ""
        raise HTTPException(
            status_code=404,
            detail=f"No IBTrACS data for {search.name}{year_str}"
        )

    # Compute IKE in parallel (not serial)
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)
    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    return results


# ==================================================================
# SOURCE HEALTH & WEATHERNEXT VALIDATION ENDPOINTS
# ==================================================================

@router.get("/health/sources")
async def get_source_health():
    """
    Dashboard view of all data source health: reliability, latency,
    composite rankings, and consecutive failure counts.

    Used by the mobile app Settings screen and for operational monitoring.
    """
    from services.source_health import SourceHealthMonitor
    monitor = SourceHealthMonitor.instance()
    return monitor.summary()


@router.get("/storms/{storm_id}/ai-comparison")
async def get_ai_vs_nhc_comparison(storm_id: str):
    """
    Side-by-side comparison of WeatherNext AI forecast vs NHC traditional
    advisory for a given storm. Logged for post-season 2026 validation.

    Returns 404 if WeatherNext is not configured or the storm is not active.
    """
    from services.weather_data_service import WeatherDataService

    async with WeatherDataService() as svc:
        # Attempt to get storm location from NHC active list
        async with NOAAClient() as client:
            try:
                active = await client.get_active_storms()
            except NOAAClientError:
                active = []

        storm = None
        for s in active:
            if s.get("id", "").upper() == storm_id.upper():
                storm = s
                break

        if not storm:
            raise HTTPException(
                status_code=404,
                detail=f"Storm {storm_id} not found in NHC active list"
            )

        lat = storm.get("lat", 25.0)
        lon = storm.get("lon", -80.0)

        comparison = await svc.get_weathernext_vs_nhc_comparison(storm_id, lat, lon)
        if not comparison:
            raise HTTPException(
                status_code=404,
                detail="WeatherNext not configured or no forecast data available"
            )

        return comparison


@router.get("/validation/season")
async def get_validation_season_summary(year: int = Query(None)):
    """
    Season-level summary of all validation data: how many comparisons logged,
    how many storms tracked, how many have post-season outcomes recorded.
    """
    from services.validation_log import ValidationLogger
    vlog = ValidationLogger.instance()
    return vlog.get_season_summary(year)


@router.get("/validation/storm/{storm_id}/accuracy")
async def get_storm_accuracy(storm_id: str):
    """
    After recording actual outcomes, returns NHC vs WeatherNext accuracy for
    a specific storm. Returns 404 if no outcome has been recorded yet.
    """
    from services.validation_log import ValidationLogger
    vlog = ValidationLogger.instance()
    result = vlog.get_storm_accuracy(storm_id)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"No outcome recorded yet for {storm_id}. Record outcomes via POST /validation/outcome."
        )
    return result


@router.post("/validation/outcome")
async def record_storm_outcome(
    storm_id: str = Body(...),
    peak_wind_kt: float = Body(None),
    min_pressure_mb: float = Body(None),
    landfall_lat: float = Body(None),
    landfall_lon: float = Body(None),
    category: str = Body(None),
    dpi: float = Body(None),
    notes: str = Body(""),
):
    """
    Record the actual observed outcome of a storm after it ends.

    This ground truth is compared against both NHC and WeatherNext predictions
    in the post-season accuracy analysis. Idempotent (upserts by storm_id).
    """
    from services.validation_log import ValidationLogger
    vlog = ValidationLogger.instance()
    ok = vlog.record_actual_outcome(
        storm_id=storm_id,
        peak_wind_kt=peak_wind_kt,
        min_pressure_mb=min_pressure_mb,
        landfall_lat=landfall_lat,
        landfall_lon=landfall_lon,
        category=category,
        dpi=dpi,
        notes=notes,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to record outcome")
    return {"status": "recorded", "storm_id": storm_id}


# ==================================================================
# WIND RADII AUDIT ENDPOINTS
# ==================================================================

@router.post("/audit/radii/{storm_id}")
async def run_radii_audit(storm_id: str):
    """
    Trigger a wind radii cross-validation audit for an active storm.

    Fetches the latest advisory data from all available sources (NHC, IBTrACS),
    cross-validates quadrant wind radii, runs plausibility checks, and returns
    a confidence score. Results are persisted to JSONL + SQLite.

    Designed to be called every ATCF advisory cycle (every 6 hours, offset +3h)
    but can be triggered manually at any time.
    """
    from services.wind_radii_audit import (
        WindRadiiAuditor, RadiiObservation, snapshot_to_observation,
    )
    from services.source_health import SourceHealthMonitor

    auditor = WindRadiiAuditor.instance()
    monitor = SourceHealthMonitor.instance()
    observations = []

    async with NOAAClient() as client:
        # Source 1: NHC operational advisory (highest authority)
        try:
            t0 = time.time()
            snapshot = await client.get_storm_snapshot(storm_id)
            monitor.record_success("nhc_active", latency_ms=(time.time() - t0) * 1000)
            obs = snapshot_to_observation(snapshot, source="nhc_advisory")
            observations.append(obs)
        except (NOAAClientError, Exception) as e:
            monitor.record_failure("nhc_active", error=str(e))
            logger.debug(f"NHC advisory unavailable for {storm_id}: {e}")

        # Source 2: IBTrACS (independent quality-controlled archive)
        try:
            t0 = time.time()
            # Try IBTrACS by ATCF ID
            csv_text = await client._fetch_ibtracs(use_recent=True)
            # Search for matching storm in IBTrACS and get latest observation
            import csv as _csv, io as _io
            reader = _csv.DictReader(_io.StringIO(csv_text))
            latest_ibtracs_snap = None
            for row in reader:
                row_atcf = row.get("USA_ATCF_ID", "").strip()
                if row_atcf.upper() == storm_id.upper():
                    snap = client._ibtracs_row_to_snapshot(row)
                    if snap:
                        latest_ibtracs_snap = snap

            if latest_ibtracs_snap:
                monitor.record_success("ibtracs", latency_ms=(time.time() - t0) * 1000)
                obs = snapshot_to_observation(latest_ibtracs_snap, source="ibtracs")
                observations.append(obs)
            else:
                monitor.record_failure("ibtracs", error=f"No IBTrACS data for {storm_id}")
        except (NOAAClientError, Exception) as e:
            monitor.record_failure("ibtracs", error=str(e))
            logger.debug(f"IBTrACS unavailable for {storm_id}: {e}")

    if not observations:
        raise HTTPException(
            status_code=404,
            detail=f"No wind radii data available for {storm_id} from any source"
        )

    # Get previous advisory for temporal continuity check
    prev = auditor.get_audit_history(storm_id)
    previous_obs = None
    if prev:
        last_radii = prev[-1].get("radii_used")
        if last_radii:
            previous_obs = RadiiObservation(
                source=last_radii.get("source", "previous"),
                timestamp=prev[-1].get("advisory_time", ""),
                storm_id=storm_id,
                max_wind_kt=last_radii.get("max_wind_kt"),
                r34_ne_nm=last_radii.get("r34_ne_nm"),
                r34_se_nm=last_radii.get("r34_se_nm"),
                r34_sw_nm=last_radii.get("r34_sw_nm"),
                r34_nw_nm=last_radii.get("r34_nw_nm"),
                r50_ne_nm=last_radii.get("r50_ne_nm"),
                r50_se_nm=last_radii.get("r50_se_nm"),
                r50_sw_nm=last_radii.get("r50_sw_nm"),
                r50_nw_nm=last_radii.get("r50_nw_nm"),
                r64_ne_nm=last_radii.get("r64_ne_nm"),
                r64_se_nm=last_radii.get("r64_se_nm"),
                r64_sw_nm=last_radii.get("r64_sw_nm"),
                r64_nw_nm=last_radii.get("r64_nw_nm"),
            )

    result = await auditor.audit_storm(storm_id, observations, previous_obs)

    return {
        "storm_id": result.storm_id,
        "advisory_time": result.advisory_time,
        "audit_time": result.audit_time,
        "confidence_score": result.confidence_score,
        "sources_checked": result.sources_checked,
        "source_names": result.source_names,
        "cross_source_agreement": result.cross_source_agreement,
        "error_count": sum(1 for f in result.flags if f.severity == "error"),
        "warning_count": sum(1 for f in result.flags if f.severity == "warning"),
        "flags": [
            {"severity": f.severity, "check": f.check_name, "message": f.message, "field": f.field}
            for f in result.flags
        ],
        "radii_used": result.radii_used,
    }


@router.get("/audit/radii/{storm_id}/history")
async def get_radii_audit_history(storm_id: str):
    """
    Retrieve the full audit trail for a storm's wind radii.

    Returns every advisory cycle audit: confidence scores, flags, which sources
    agreed/disagreed, and the radii values that were used.
    """
    from services.wind_radii_audit import WindRadiiAuditor
    auditor = WindRadiiAuditor.instance()
    history = auditor.get_audit_history(storm_id)
    if not history:
        raise HTTPException(status_code=404, detail=f"No audit history for {storm_id}")
    return history


@router.get("/audit/radii/{storm_id}/confidence")
async def get_latest_radii_confidence(storm_id: str):
    """Quick check: what's the current confidence in this storm's wind radii data?"""
    from services.wind_radii_audit import WindRadiiAuditor
    auditor = WindRadiiAuditor.instance()
    score = auditor.get_latest_confidence(storm_id)
    summary = auditor.get_storm_summary(storm_id)
    if score is None:
        raise HTTPException(status_code=404, detail=f"No audit data for {storm_id}")
    return {
        "storm_id": storm_id,
        "latest_confidence": score,
        "summary": summary,
    }


@router.get("/audit/radii/summary")
async def get_all_radii_audit_summaries():
    """Dashboard: audit summaries for all storms with active audits."""
    from services.wind_radii_audit import WindRadiiAuditor
    auditor = WindRadiiAuditor.instance()
    return auditor.get_all_summaries()
