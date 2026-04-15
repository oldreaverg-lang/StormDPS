"""
Satellite imagery proxy + frame index for the StormDPS map.

Two endpoints power the live satellite layer:

    GET /satellite/frames/{satellite}?hours=24&cadence_min=30
        Returns a JSON list of available frame timestamps for the requested
        satellite. The frontend uses this to build the time slider.

    GET /satellite/tile/{satellite}/{ts}/{z}/{x}/{y}.png
        Web Mercator slippy tile (EPSG:3857) for the given satellite + frame
        timestamp + zoom/x/y. Tiles are fetched on demand from NASA GIBS WMTS
        (https://gibs.earthdata.nasa.gov) and cached on the Railway persistent
        volume so subsequent users (and time-slider scrubs) get an instant
        response.

Why proxy at all? Three reasons:
  1. Browser CORS — GIBS sets ``Access-Control-Allow-Origin: *`` so direct
     hot-linking does work, but a proxy lets us aggressively cache and lets
     us swap upstreams (NESDIS, RAMMB) without changing the frontend.
  2. Cost control — each cached tile is served straight off disk after the
     first hit, so users in the same hour don't all hammer NASA's CDN.
  3. Eviction — we keep only the last 48 hours of frames; older tile
     directories get pruned automatically.

Satellite mapping (basin → satellite name):
    GOES-East  : Atlantic, East Pacific, Caribbean              (lon -135 ..  -10)
    GOES-West  : Central / East Pacific, west of GOES-East      (lon -180 .. -135)
    Himawari   : West Pacific, eastern Indian Ocean, Australia  (lon   60 ..  180)
    Meteosat-IODC: western Indian Ocean                         (lon   20 ..   60)
    Meteosat-0 : Atlantic east of GOES-East / Africa            (lon  -10 ..   20)

The choose_satellite() helper below maps a (lat, lon) to one of these names.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import FileResponse

from storage import SATELLITE_CACHE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Upstream config ─────────────────────────────────────────────────────────

# NASA GIBS WMTS in EPSG:3857 (Web Mercator). GeoColor is a true-color daytime
# / IR-blended nighttime composite, the same look Zoom Earth uses.
GIBS_BASE = "https://gibs.earthdata.nasa.gov/wmts/epsg3857/best"

# Satellite → (GIBS layer name, max native zoom, cadence in minutes).
# GeoColor refresh cadences vary by satellite; we round up to 10-min frames.
_SATELLITES = {
    "goes-east": ("GOES-East_ABI_GeoColor", 7, 10),
    "goes-west": ("GOES-West_ABI_GeoColor", 7, 10),
    "himawari":  ("Himawari_AHI_Band3_Red_Visible_1km", 7, 10),  # GIBS GeoColor for Himawari
    "meteosat-iodc": ("Meteosat-IODC_IR_Brightness_Temperature", 6, 30),
    "meteosat-0":   ("Meteosat-Prime_IR_Brightness_Temperature", 6, 30),
}

_TILE_CACHE_TTL_HOURS = 48
_HTTP_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


# ── Helpers ─────────────────────────────────────────────────────────────────

def choose_satellite(lat: float, lon: float) -> str:
    """Pick the best geostationary satellite for a given storm position."""
    # Normalize lon to -180..180
    if lon > 180:
        lon -= 360
    if -135 <= lon < -10:
        return "goes-east"
    if -180 <= lon < -135:
        return "goes-west"
    if 60 <= lon <= 180:
        return "himawari"
    if 20 <= lon < 60:
        return "meteosat-iodc"
    # -10 .. 20 (Africa / E. Atlantic) → Meteosat-0
    return "meteosat-0"


def _round_to_cadence(dt: datetime, cadence_min: int) -> datetime:
    """Floor *dt* (UTC) to the nearest cadence_min boundary."""
    minute = (dt.minute // cadence_min) * cadence_min
    return dt.replace(minute=minute, second=0, microsecond=0)


def _ts_to_iso(ts: str) -> str:
    """Convert compact ts (YYYYMMDDTHHMM) to ISO 8601 (with Z)."""
    return f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}T{ts[9:11]}:{ts[11:13]}:00Z"


def _is_valid_ts(ts: str) -> bool:
    if len(ts) != 13 or ts[8] != "T":
        return False
    try:
        datetime.strptime(ts, "%Y%m%dT%H%M")
        return True
    except ValueError:
        return False


def _tile_path(satellite: str, ts: str, z: int, x: int, y: int) -> Path:
    return SATELLITE_CACHE_DIR / satellite / ts / str(z) / str(x) / f"{y}.png"


# ── Endpoints ───────────────────────────────────────────────────────────────

@router.get("/satellite/frames/{satellite}")
async def satellite_frames(
    satellite: str,
    hours: int = Query(24, ge=1, le=336),
    cadence_min: int = Query(30, ge=10, le=60),
):
    """List the most recent frame timestamps available for a satellite.

    The frontend uses this to populate the time slider. We don't actually
    verify each frame exists upstream — we trust GIBS to have published every
    cadence_min frame for the last *hours* hours. If a frame turns out to be
    missing the proxy returns a 1×1 transparent tile.
    """
    if satellite not in _SATELLITES:
        raise HTTPException(404, f"Unknown satellite '{satellite}'")
    _, _, native_cadence = _SATELLITES[satellite]
    cadence_min = max(cadence_min, native_cadence)

    now = datetime.now(timezone.utc)
    # GIBS publishes frames with ~10-15 min lag — back off 15 min from "now"
    end = _round_to_cadence(now - timedelta(minutes=15), cadence_min)
    start = end - timedelta(hours=hours)

    frames: list[str] = []
    cursor = start
    while cursor <= end:
        frames.append(cursor.strftime("%Y%m%dT%H%M"))
        cursor += timedelta(minutes=cadence_min)

    return {
        "satellite": satellite,
        "cadence_min": cadence_min,
        "hours": hours,
        "frames": frames,
        "latest": frames[-1] if frames else None,
    }


# 1×1 transparent PNG returned when an upstream tile is missing or upstream
# is unreachable. Generated once and cached in memory.
_BLANK_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c63000100000005000156a5d6680000000049454e44ae426082"
)


@router.get("/satellite/tile/{satellite}/{ts}/{z}/{x}/{y}.png")
async def satellite_tile(
    satellite: str,
    ts: str,
    z: int,
    x: int,
    y: int,
):
    """Serve a Web Mercator slippy tile for *satellite* at frame *ts*.

    *ts* must be in the compact form ``YYYYMMDDTHHMM`` (e.g. ``20260415T1530``).
    Cache hit → instant disk read. Cache miss → fetch from GIBS, persist,
    serve. Upstream failure → transparent 1×1 PNG so the map keeps working.
    """
    if satellite not in _SATELLITES:
        raise HTTPException(404, f"Unknown satellite '{satellite}'")
    if not _is_valid_ts(ts):
        raise HTTPException(400, "Invalid timestamp; expected YYYYMMDDTHHMM")
    layer, max_zoom, _ = _SATELLITES[satellite]
    if z < 0 or z > max_zoom:
        # Out-of-range zoom — return transparent so Leaflet doesn't 404-spam.
        return Response(content=_BLANK_PNG, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})

    path = _tile_path(satellite, ts, z, x, y)
    if path.exists():
        return FileResponse(path, media_type="image/png",
                            headers={"Cache-Control": "public, max-age=86400"})

    # GIBS WMTS REST URL. TileMatrixSet for GeoColor is GoogleMapsCompatible_Level7.
    matrix_set = "GoogleMapsCompatible_Level7" if max_zoom >= 7 else "GoogleMapsCompatible_Level6"
    iso_ts = _ts_to_iso(ts)
    upstream = (
        f"{GIBS_BASE}/{layer}/default/{iso_ts}/{matrix_set}/{z}/{y}/{x}.png"
    )

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            r = await client.get(upstream)
        if r.status_code == 200 and r.content[:8].startswith(b"\x89PNG"):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".png.tmp")
                tmp.write_bytes(r.content)
                os.replace(tmp, path)
            except OSError as e:
                logger.warning(f"[SATELLITE] cache write failed for {path}: {e}")
            return Response(content=r.content, media_type="image/png",
                            headers={"Cache-Control": "public, max-age=86400"})
        else:
            logger.debug(f"[SATELLITE] upstream {r.status_code} for {upstream}")
    except Exception as e:
        logger.warning(f"[SATELLITE] upstream fetch failed: {type(e).__name__}: {e}")

    # Fallback: blank tile (kept short-cache so a later refresh can fill it in).
    return Response(content=_BLANK_PNG, media_type="image/png",
                    headers={"Cache-Control": "public, max-age=300"})


# ── Eviction (called from a background task or manually) ────────────────────

def evict_old_satellite_frames(max_age_hours: int = _TILE_CACHE_TTL_HOURS) -> int:
    """Delete cached frame directories older than *max_age_hours*.

    Returns the number of frame directories removed.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    if not SATELLITE_CACHE_DIR.exists():
        return 0
    for sat_dir in SATELLITE_CACHE_DIR.iterdir():
        if not sat_dir.is_dir():
            continue
        for frame_dir in sat_dir.iterdir():
            if not frame_dir.is_dir():
                continue
            try:
                frame_dt = datetime.strptime(frame_dir.name, "%Y%m%dT%H%M").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if frame_dt < cutoff:
                try:
                    shutil.rmtree(frame_dir)
                    removed += 1
                except OSError as e:
                    logger.debug(f"[SATELLITE EVICT] {frame_dir}: {e}")
    if removed:
        logger.info(f"[SATELLITE EVICT] removed {removed} frame dirs older than {max_age_hours}h")
    return removed
