"""
FastAPI route definitions for the Hurricane IKE API.

Endpoints:
  - /storms/active — list active tropical cyclones from NHC
  - /storms/{id}/ike — compute IKE for a specific storm
  - /storms/{id}/value — full destructive valuation
  - /storms/{id}/history — historical IKE timeline from HURDAT2
  - /ike/compute — compute IKE from custom parameters
  - /ibtracs/track/{sid} — fetch storm track from IBTrACS
  - /ibtracs/search — search IBTrACS by name/year/basin
"""

import asyncio
import csv
import hashlib
import io
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import APIRouter, Body, HTTPException, Query

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

# Cache for global IBTrACS catalog to avoid repeated large downloads/parses.
# We also persist a json cache file so restarts can reuse the catalog quickly.
_GLOBAL_IBTRACS_CATALOG_CACHE = None
_GLOBAL_IBTRACS_CATALOG_TIMESTAMP = None
_GLOBAL_IBTRACS_CATALOG_TTL = timedelta(hours=6)
_GLOBAL_IBTRACS_CACHE_FILE = Path(__file__).parent.parent / "data" / "cache" / "ibtracs_catalog.json"

# ------------------------------------------------------------------
# Per-storm IKE result cache
# ------------------------------------------------------------------
# Caches the full IKEResponse list for each storm+params combination.
# IKE depends on the wind model (grid resolution, quadrant method) but
# NOT on the DPS formula — DPS is computed client-side from cached IKE.
# Cache key: storm_id + grid_resolution + skip_points
# ------------------------------------------------------------------
_IKE_CACHE_DIR = Path(__file__).parent.parent / "data" / "cache" / "ike"
_IKE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Bump this when the IKE wind model changes (e.g., Holland profile, quadrant method)
# DPS formula changes do NOT require a version bump — DPS is client-side.
_IKE_CACHE_VERSION = "v2"


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
    except (json.JSONDecodeError, KeyError):
        return None


def _save_ike_cache(storm_id: str, grid_res_km: float, skip: int,
                    results: list[dict], source: str, compute_ms: float):
    """Save IKE results to disk cache."""
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
    try:
        path.write_text(json.dumps(payload, default=str))
    except Exception as e:
        logger.warning(f"Failed to write IKE cache for {storm_id}: {e}")


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
    import csv
    import io

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
            # Run CPU-intensive computation in thread pool
            loop = asyncio.get_event_loop()
            try:
                ike = await loop.run_in_executor(
                    None,
                    compute_ike_from_snapshot,
                    snap,
                    grid_resolution_m
                )
                return (ike, snap)
            except ValueError:
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
        import logging
        logging.warning(f"Failed to load custom storms: {e}")
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
                    import logging
                    logging.warning(f"Failed to parse custom track row: {e}")
                    continue
    except Exception as e:
        import logging
        logging.warning(f"Failed to load custom track for {storm_id}: {e}")
        return []
    
    return snapshots


# ------------------------------------------------------------------
# Active storms
# ------------------------------------------------------------------

@router.get("/storms/active", response_model=list[StormSummary])
async def list_active_storms():
    """List all currently active tropical cyclones from NHC."""
    async with NOAAClient() as client:
        try:
            storms = await asyncio.wait_for(client.get_active_storms(), timeout=3.0)
        except asyncio.TimeoutError:
            return []  # Return empty list if timeout
        except NOAAClientError as e:
            # Log but don't crash - return empty list
            import logging
            logging.warning(f"Failed to fetch active storms: {e}")
            return []
        except Exception as e:
            # Catch any other errors
            import logging
            logging.warning(f"Unexpected error fetching active storms: {e}")
            return []
    return [StormSummary(**s) for s in storms]


@router.get("/storms/{storm_id}/forecast")
async def get_storm_forecast(storm_id: str):
    """
    Fetch NHC forecast track and cone for an active storm.

    Returns GeoJSON-like data with forecast positions and the
    uncertainty cone polygon, sourced from NHC GIS archives.
    """
    async with NOAAClient() as client:
        try:
            forecast = await client.get_forecast_track(storm_id)
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))
    return forecast


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
    print(f"[SST] Request received: {len(points)} track points")
    if points:
        print(f"[SST] First point: {points[0]}")
        print(f"[SST] Last point:  {points[-1]}")
    async with NOAAClient() as client:
        try:
            sst_data = await client.get_sst_along_track(points)
            valid_count = sum(1 for s in sst_data if s.get("sst_c") is not None)
            print(f"[SST] Response: {valid_count}/{len(sst_data)} points have valid SST data")
            if valid_count == 0 and sst_data:
                print(f"[SST] WARNING: All SST values null! First result: {sst_data[0]}")
        except Exception as e:
            print(f"[SST] ERROR: {type(e).__name__}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    return sst_data


# ------------------------------------------------------------------
# IKE computation
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/ike", response_model=IKEResponse)
async def get_storm_ike(
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
    async with NOAAClient() as client:
        try:
            snapshot = await client.get_storm_snapshot(storm_id)
        except NOAAClientError as e:
            raise HTTPException(status_code=404, detail=str(e))

        # Try to get gridded data
        grid = await client.get_gridded_wind_field(
            storm_id, snapshot.lat, snapshot.lon
        )
        if grid is not None:
            snapshot.wind_field = grid

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
    storm_id: str,
    grid_resolution_km: float = Query(5.0, ge=1.0, le=50.0),
):
    """
    Compute full destructive value for a storm.

    Combines IKE, surge threat, and intensification rate into
    a composite 0-100 score.
    """
    async with NOAAClient() as client:
        try:
            snapshot = await client.get_storm_snapshot(storm_id)
        except NOAAClientError as e:
            raise HTTPException(status_code=404, detail=str(e))

        grid = await client.get_gridded_wind_field(
            storm_id, snapshot.lat, snapshot.lon
        )
        if grid is not None:
            snapshot.wind_field = grid

    try:
        valuation = compute_valuation(
            snapshot, grid_resolution_m=grid_resolution_km * 1000
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return ValuationResponse(
        storm_id=valuation.storm_id,
        name=valuation.name,
        timestamp=valuation.ike_result.timestamp,
        ike=_ike_to_response(valuation.ike_result, snapshot),
        destructive_potential=round(valuation.destructive_potential, 1),
        surge_threat=round(valuation.surge_threat, 1) if valuation.surge_threat is not None else None,
        overall_value=round(valuation.overall_value, 1) if valuation.overall_value is not None else None,
        category=snapshot.category.name.replace("_", " ").title(),
    )


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
    Get a lightweight catalog of all named storms from HURDAT2.

    Returns storm ID, name, year, peak wind (kt), and Saffir-Simpson category
    without computing IKE. Fast enough for populating UI storm pickers.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    async with NOAAClient() as client:
        try:
            catalog = await client.get_storm_catalog(min_year, max_year)
            print(f"[CATALOG] HURDAT2 loaded: {len(catalog)} storms ({min_year}-{max_year})")
        except Exception as e:
            print(f"[CATALOG] HURDAT2 FAILED: {type(e).__name__}: {str(e)}")
            logger.warning("Returning empty HURDAT2 catalog as fallback")
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

    - Custom storms (SI/NI/SP prefixes from data/custom_storms.csv) → Custom tracks
    - ATCF IDs starting with AL or EP (e.g., AL092008) → HURDAT2
    - IBTrACS SIDs (e.g., 2008245N17323) → IBTrACS by SID
    - Other basin prefixes (SH, WP, IO, CP, etc.) → IBTrACS name search

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
        from fastapi.responses import JSONResponse
        return JSONResponse(content=cached)

    prefix = storm_id[:2].upper()
    snapshots = []
    source = None

    # 1) Try custom storms first (local CSV, no network needed)
    if prefix in ("SI", "NI", "SP", "WP"):
        snapshots = _load_custom_track(storm_id)
        if snapshots:
            source = "custom"

    # 2-4) All remote lookups share a SINGLE NOAAClient (one HTTP session)
    if not snapshots:
        cache_dir = Path(__file__).parent.parent / "data" / "cache"
        async with NOAAClient(cache_dir=str(cache_dir)) as client:
            # 2) HURDAT2/EBTRK for Atlantic / East Pacific ATCF IDs
            if prefix in ("AL", "EP") and len(storm_id) == 8:
                if NOAAClient.nhc_is_down():
                    print(f"[TRACK] Skipping HURDAT2 for {storm_id} — NHC recently unreachable")
                else:
                    try:
                        snapshots = await client.get_historical_track(storm_id)
                        source = "hurdat2"
                    except (NOAAClientError, Exception):
                        snapshots = []

            # 3) IBTrACS by SID (format: YYYYDDDNxxyyy — starts with digit)
            if not snapshots and storm_id[0].isdigit():
                try:
                    snapshots = await client.get_ibtracs_track(storm_id)
                    source = "ibtracs"
                except (NOAAClientError, Exception):
                    snapshots = []

            # 4) Fallback: IBTrACS by ATCF ID for AL/EP storms not in HURDAT2
            if not snapshots and prefix in ("AL", "EP") and len(storm_id) == 8:
                try:
                    csv_text = await client._fetch_ibtracs(use_recent=True)
                    snapshots = _search_ibtracs_by_atcf_id(client, csv_text, storm_id)
                    if not snapshots:
                        # Reuse the same client — no new connection
                        csv_text = await client._fetch_ibtracs(use_recent=False)
                        snapshots = _search_ibtracs_by_atcf_id(client, csv_text, storm_id)
                    if snapshots:
                        source = "ibtracs"
                except (NOAAClientError, Exception):
                    snapshots = []

            # 5) Last resort: IBTrACS SID directly
            if not snapshots:
                try:
                    snapshots = await client.get_ibtracs_track(storm_id, use_recent=False)
                    source = "ibtracs"
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

_PRELOAD_BUNDLE_PATH = Path(__file__).parent.parent / "data" / "cache" / "preload_bundle.json"


@router.get("/preload")
async def get_preload_bundle():
    """
    Return a preloaded data bundle containing all formula inputs for preset
    storms. The frontend loads this on startup so DPS/IKE formulas always
    have complete data without waiting for per-storm API calls.

    Returns a dict of storm_id -> list of IKEResponse dicts.
    Also includes any active storms that have cached data.
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

    # Fetch from IBTrACS (may be slow on first run) but time out if it takes too long
    catalog = []
    cache_dir = Path(__file__).parent.parent / "data" / "cache"
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
    Search IBTrACS by storm name and year, compute IKE for each observation.

    Useful for finding storms by name when the IBTrACS SID isn't known.
    """
    async with NOAAClient() as client:
        try:
            snapshots = await client.get_ibtracs_by_name_year(
                search.name, search.year, search.basin
            )
        except (NOAAClientError, Exception) as e:
            raise HTTPException(status_code=404, detail=str(e))

    if not snapshots:
        raise HTTPException(
            status_code=404,
            detail=f"No IBTrACS data for {search.name} ({search.year})"
        )

    # Compute IKE in parallel (not serial)
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)
    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    return results
