"""
Wind field proxy backed by Open-Meteo, formatted for leaflet-velocity.

The frontend's wind layer (a leaflet-velocity overlay drawing animated
particles + a colored speed heatmap) needs a gridded U/V wind field. Open-Meteo
serves point-wise hourly forecasts for free; we sample a regular grid around
the storm, convert wind speed/direction to U/V components, and return the
two-record JSON array that leaflet-velocity expects:

    [
      { "header": { ..., "parameterNumber": 2 }, "data": [u00, u01, ...] },
      { "header": { ..., "parameterNumber": 3 }, "data": [v00, v01, ...] }
    ]

Endpoint
--------
    GET /wind/field?bbox=lat0,lon0,lat1,lon1&ts=YYYYMMDDTHH&res=1.0

    bbox    Two corners (south-west, north-east). Clamped to ±60° lat to keep
            the grid manageable.
    ts      Frame timestamp (hourly resolution). Past frames hit the ERA5
            archive endpoint; future frames hit the standard forecast endpoint.
            Defaults to the current top-of-the-hour.
    res     Grid step in degrees (default 1.0 → ~111 km at the equator).
            Smaller is prettier but more API calls.

Caching: each unique (bbox_key, ts) combo is persisted to
$PERSISTENT_DATA_DIR/cache/wind/<bbox_key>/<ts>.json. Repeat hits are served
from disk in microseconds.

Open-Meteo free tier allows up to ~10K calls/day and ~600 location-coords per
single request; for safety we cap the grid at 30×30 = 900 points per call.
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from api.open_meteo_limiter import open_meteo_get
from storage import WIND_CACHE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Upstream config ─────────────────────────────────────────────────────────

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/era5"
_HTTP_TIMEOUT = httpx.Timeout(25.0, connect=5.0)

_MAX_GRID_POINTS = 900   # 30 × 30 — safe under Open-Meteo's batch cap
_MIN_RES_DEG     = 0.5
_MAX_RES_DEG     = 2.0
_DEFAULT_RES_DEG = 1.0
_CACHE_TTL_HOURS = 48

# Throttle eviction scans: at most one per hour so a burst of cache writes
# doesn't repeatedly walk the cache directory.
_LAST_EVICT_AT: datetime | None = None
_EVICT_INTERVAL = timedelta(hours=1)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _bbox_key(lat0: float, lon0: float, lat1: float, lon1: float, res: float) -> str:
    """Stable directory key for a bbox at a given resolution.

    Normalizes corner order so callers passing swapped corners hit the same
    cache entry as the canonical (south,west,north,east) ordering.
    """
    s, n = sorted((float(lat0), float(lat1)))
    w, e = sorted((float(lon0), float(lon1)))
    return f"{s:+06.1f}_{w:+07.1f}_{n:+06.1f}_{e:+07.1f}_r{res:.1f}"


def _parse_ts(ts: str) -> datetime:
    """Parse YYYYMMDDTHH into a tz-aware UTC datetime."""
    if len(ts) != 11 or ts[8] != "T":
        raise HTTPException(400, "Invalid ts; expected YYYYMMDDTHH")
    try:
        return datetime.strptime(ts, "%Y%m%dT%H").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise HTTPException(400, f"Invalid ts: {e}")


def _build_grid(lat0: float, lon0: float, lat1: float, lon1: float, res: float
                ) -> tuple[list[float], list[float], int, int]:
    """Build a regular lat/lon grid covering the bbox at *res* degrees.

    Returns (lats_list_per_point, lons_list_per_point, ny, nx) in row-major
    order — top row first (highest lat), each row left-to-right (lowest lon
    first). This matches the layout leaflet-velocity expects.
    """
    # Snap corners to res, ensure lat0<lat1, lon0<lon1
    lat_min, lat_max = sorted([lat0, lat1])
    lon_min, lon_max = sorted([lon0, lon1])
    nx = max(2, int(math.floor((lon_max - lon_min) / res)) + 1)
    ny = max(2, int(math.floor((lat_max - lat_min) / res)) + 1)
    # Cap to keep API calls in budget
    while nx * ny > _MAX_GRID_POINTS:
        res *= 1.25
        nx = max(2, int(math.floor((lon_max - lon_min) / res)) + 1)
        ny = max(2, int(math.floor((lat_max - lat_min) / res)) + 1)
    lats: list[float] = []
    lons: list[float] = []
    # leaflet-velocity expects row-major from TOP-LEFT (la1, lo1) downward.
    for j in range(ny):
        lat = lat_max - j * res
        for i in range(nx):
            lon = lon_min + i * res
            lats.append(round(lat, 4))
            lons.append(round(lon, 4))
    return lats, lons, ny, nx


def _wind_to_uv(speed_ms: float, dir_deg_from: float) -> tuple[float, float]:
    """Meteorological wind: dir is the direction the wind is coming FROM.
    Return (u, v) where u is east-positive, v is north-positive.
    """
    if speed_ms is None or dir_deg_from is None:
        return (0.0, 0.0)
    # Vector points TO direction = from + 180; standard formula:
    rad = math.radians(dir_deg_from)
    u = -speed_ms * math.sin(rad)
    v = -speed_ms * math.cos(rad)
    return (u, v)


async def _fetch_open_meteo(lats: list[float], lons: list[float],
                            ts_dt: datetime) -> tuple[list[float|None], list[float|None]]:
    """Hit Open-Meteo for wind_speed_10m + wind_direction_10m at every point.

    Returns (speeds_ms, dirs_deg_from) parallel to lats/lons. Returns
    (None, None) for any point that didn't come back.
    """
    now = datetime.now(timezone.utc)
    # ERA5 archive has ~5 day publication lag — only switch to it for
    # timestamps older than 7 days. Anything within the last week is served
    # from the forecast endpoint, which exposes a `past_days` window so we
    # can request historical hours without the archive lag.
    is_archive = ts_dt < now - timedelta(days=7)
    base = _ARCHIVE_URL if is_archive else _FORECAST_URL

    # Open-Meteo accepts comma-separated multi-location requests.
    params = {
        "latitude": ",".join(f"{x:.4f}" for x in lats),
        "longitude": ",".join(f"{x:.4f}" for x in lons),
        "hourly": "wind_speed_10m,wind_direction_10m",
        "wind_speed_unit": "ms",
        "timezone": "UTC",
    }
    if is_archive:
        params["start_date"] = ts_dt.strftime("%Y-%m-%d")
        params["end_date"]   = ts_dt.strftime("%Y-%m-%d")
    else:
        # Forecast endpoint: include enough past_days to cover the requested
        # timestamp (max 7 on the free tier), and a small forward window so
        # users can scrub into the immediate forecast.
        delta_days = (now.date() - ts_dt.date()).days
        params["past_days"] = max(0, min(7, delta_days + 1))
        # Only request forecast hours if the target ts is actually in the
        # future — otherwise we burn quota on 3 days of unused samples.
        if ts_dt.date() >= now.date():
            params["forecast_days"] = 3

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        r = await open_meteo_get(client, base, params, label="WIND")
    body = r.json()
    # Multi-location response is a list; single-location is a dict.
    if isinstance(body, dict):
        body = [body]

    target_iso = ts_dt.strftime("%Y-%m-%dT%H:00")
    speeds: list[float|None] = []
    dirs:   list[float|None] = []
    for entry in body:
        h = entry.get("hourly") or {}
        times = h.get("time") or []
        ws    = h.get("wind_speed_10m") or []
        wd    = h.get("wind_direction_10m") or []
        try:
            idx = times.index(target_iso)
            speeds.append(ws[idx] if idx < len(ws) else None)
            dirs.append(wd[idx] if idx < len(wd) else None)
        except (ValueError, IndexError):
            speeds.append(None)
            dirs.append(None)
    # Pad if Open-Meteo returned fewer entries than requested
    while len(speeds) < len(lats):
        speeds.append(None)
        dirs.append(None)
    return speeds, dirs


def _build_velocity_payload(lats: list[float], lons: list[float],
                            ny: int, nx: int,
                            speeds: list[float|None], dirs: list[float|None],
                            ts_dt: datetime) -> list[dict]:
    """Convert speed+direction grid into leaflet-velocity's two-record JSON."""
    u_data: list[float] = []
    v_data: list[float] = []
    for i in range(len(lats)):
        # Pass speed/dir through unmodified — _wind_to_uv returns (0,0) when
        # either is None. Don't substitute 0.0 here, or a missing direction
        # paired with a real speed would render as a fake northerly wind
        # (dir=0 → wind FROM north → southward particle flow over a data void).
        u, v = _wind_to_uv(speeds[i], dirs[i])
        u_data.append(round(u, 2))
        v_data.append(round(v, 2))

    la1 = max(lats); la2 = min(lats)
    lo1 = min(lons); lo2 = max(lons)
    dx = round((lo2 - lo1) / max(1, nx - 1), 4)
    dy = round((la1 - la2) / max(1, ny - 1), 4)
    ref_time = ts_dt.strftime("%Y-%m-%d %H:00:00")

    common_header = {
        "discipline": 0,
        "disciplineName": "Meteorological products",
        "gribEdition": 2,
        "gribLength": 0,
        "center": 7,
        "centerName": "Open-Meteo",
        "subcenter": 0,
        "refTime": ref_time,
        "significanceOfRT": 1,
        "significanceOfRTName": "Start of forecast",
        "productStatus": 0,
        "productStatusName": "Operational products",
        "productType": 1,
        "productTypeName": "Forecast products",
        "productDefinitionTemplate": 0,
        "productDefinitionTemplateName": "Analysis/forecast at horizontal level/in horizontal layer at a point in time",
        "parameterCategory": 2,
        "parameterCategoryName": "Momentum",
        "parameterUnit": "m s-1",
        "genProcessType": 2,
        "genProcessTypeName": "Forecast",
        "forecastTime": 0,
        "surface1Type": 103,
        "surface1TypeName": "Specified height level above ground",
        "surface1Value": 10.0,
        "numberPoints": nx * ny,
        "shape": 0,
        "shapeName": "Earth spherical with radius = 6,367,470 m",
        "scanMode": 0,
        "nx": nx, "ny": ny,
        "basicAngle": 0,
        "subDivisions": 0,
        "lo1": lo1, "la1": la1,
        "lo2": lo2, "la2": la2,
        "dx": dx, "dy": dy,
    }
    return [
        {"header": {**common_header, "parameterNumber": 2,
                    "parameterNumberName": "U-component_of_wind",
                    "parameterName": "U-component of wind"},
         "data": u_data},
        {"header": {**common_header, "parameterNumber": 3,
                    "parameterNumberName": "V-component_of_wind",
                    "parameterName": "V-component of wind"},
         "data": v_data},
    ]


# ── Endpoint ────────────────────────────────────────────────────────────────

@router.get("/wind/field")
async def wind_field(
    bbox: str = Query(..., description="lat0,lon0,lat1,lon1 (any corner order)"),
    ts: str | None = Query(None, description="YYYYMMDDTHH; defaults to current hour"),
    res: float = Query(_DEFAULT_RES_DEG, ge=_MIN_RES_DEG, le=_MAX_RES_DEG),
):
    """Return a leaflet-velocity-formatted U/V wind grid for the bbox + time.

    Cached on the persistent volume by (bbox_key, ts). First call for a
    given (bbox, ts) hits Open-Meteo; subsequent calls are instant.
    """
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be lat0,lon0,lat1,lon1")
    try:
        lat0, lon0, lat1, lon1 = (float(p) for p in parts)
    except ValueError:
        raise HTTPException(400, "bbox values must be numeric")

    # Clamp to keep things sane.
    lat0 = max(-60.0, min(60.0, lat0)); lat1 = max(-60.0, min(60.0, lat1))
    lon0 = max(-180.0, min(180.0, lon0)); lon1 = max(-180.0, min(180.0, lon1))

    if ts:
        ts_dt = _parse_ts(ts)
    else:
        ts_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        ts = ts_dt.strftime("%Y%m%dT%H")

    key = _bbox_key(lat0, lon0, lat1, lon1, res)
    cache_path = WIND_CACHE_DIR / key / f"{ts}.json"
    if cache_path.exists():
        try:
            return JSONResponse(content=json.loads(cache_path.read_text()),
                                headers={"Cache-Control": "public, max-age=3600",
                                         "X-Wind-Cache": "hit"})
        except Exception as e:
            logger.warning(f"[WIND] cache read failed for {cache_path}: {e}")

    lats, lons, ny, nx = _build_grid(lat0, lon0, lat1, lon1, res)
    try:
        speeds, dirs = await _fetch_open_meteo(lats, lons, ts_dt)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[WIND] Open-Meteo fetch failed: {type(e).__name__}: {e}")
        raise HTTPException(502, "wind upstream unavailable")

    payload = _build_velocity_payload(lats, lons, ny, nx, speeds, dirs, ts_dt)

    # Persist the full payload atomically so it survives a writer crash.
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp, cache_path)
    except OSError as e:
        logger.warning(f"[WIND] cache write failed for {cache_path}: {e}")

    _maybe_evict()

    return JSONResponse(content=payload,
                        headers={"Cache-Control": "public, max-age=3600",
                                 "X-Wind-Cache": "miss"})


def _maybe_evict() -> None:
    """Run eviction if we haven't in the last hour. Best-effort, never raises."""
    global _LAST_EVICT_AT
    now = datetime.now(timezone.utc)
    if _LAST_EVICT_AT is not None and (now - _LAST_EVICT_AT) < _EVICT_INTERVAL:
        return
    _LAST_EVICT_AT = now
    try:
        evict_old_wind_frames()
    except Exception as e:
        logger.warning(f"[WIND EVICT] failed: {e}")


# ── Eviction ────────────────────────────────────────────────────────────────

def evict_old_wind_frames(max_age_hours: int = _CACHE_TTL_HOURS) -> int:
    """Delete cached wind JSON files older than *max_age_hours*."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    if not WIND_CACHE_DIR.exists():
        return 0
    for bbox_dir in WIND_CACHE_DIR.iterdir():
        if not bbox_dir.is_dir():
            continue
        for f in bbox_dir.glob("*.json"):
            try:
                stem = f.stem  # YYYYMMDDTHH
                ft = datetime.strptime(stem, "%Y%m%dT%H").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if ft < cutoff:
                try:
                    f.unlink()
                    removed += 1
                except OSError:
                    pass
        # Drop empty bbox dirs
        try:
            if not any(bbox_dir.iterdir()):
                bbox_dir.rmdir()
        except OSError:
            pass
    if removed:
        logger.info(f"[WIND EVICT] removed {removed} cached frames older than {max_age_hours}h")
    return removed
