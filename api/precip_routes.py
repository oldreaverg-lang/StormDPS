"""
Precipitation + cloud-cover overlay backed by Open-Meteo.

A single endpoint powers the frontend Precipitation overlay:

    GET /precip/field?bbox=lat0,lon0,lat1,lon1&ts=YYYYMMDDTHH&res=0.5
        Returns two co-registered grids:
          - precip:    mm/hr (precipitation rate)
          - cloud:     % (total cloud cover, 0–100)
        Both at *res* degrees over the bbox. Frontend renders cloud as a
        greyscale alpha layer underneath a multi-stop color ramp for the
        precipitation rate (Light → Moderate → Heavy → Severe).

Responses are persisted to the Railway volume so we don't burn the
Open-Meteo free tier on repeat scrubs.
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from api.open_meteo_limiter import open_meteo_get
from storage import PRECIP_CACHE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Upstream config ─────────────────────────────────────────────────────────

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/era5"
_HTTP_TIMEOUT = httpx.Timeout(25.0, connect=5.0)

_MAX_GRID_POINTS = 350    # ~18×18 — keeps the Open-Meteo URL under ~6KB
                          # (each lat/lon pair is ~7 chars + comma; >~600
                          # points blows past Cloudflare's 8KB upstream cap)
_MIN_RES_DEG     = 0.25
_MAX_RES_DEG     = 2.0
_DEFAULT_RES_DEG = 0.5    # Precip varies sharply — finer default than pressure
_CACHE_TTL_HOURS = 48

# Throttle eviction scans: at most one per hour so a burst of cache writes
# doesn't repeatedly walk the cache directory.
_LAST_EVICT_AT: datetime | None = None
_EVICT_INTERVAL = timedelta(hours=1)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _bbox_key(lat0: float, lon0: float, lat1: float, lon1: float, res: float) -> str:
    # Preserve longitude order so a dateline-crossing bbox (w > e) keys to a
    # different cache entry than the same endpoints non-wrapping.
    s, n = sorted((float(lat0), float(lat1)))
    w, e = float(lon0), float(lon1)
    cross = "x" if w > e else "n"
    return f"{s:+06.1f}_{w:+07.1f}_{n:+06.1f}_{e:+07.1f}_{cross}_r{res:.2f}"


def _parse_ts(ts: str) -> datetime:
    if len(ts) != 11 or ts[8] != "T":
        raise HTTPException(400, "Invalid ts; expected YYYYMMDDTHH")
    try:
        return datetime.strptime(ts, "%Y%m%dT%H").replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise HTTPException(400, f"Invalid ts: {e}")


def _build_grid(lat0: float, lon0: float, lat1: float, lon1: float, res: float
                ) -> tuple[list[float], list[float], int, int, float]:
    """Build a regular row-major grid (top row = highest lat).

    Longitudes are dateline-aware: lon0 > lon1 signals a wrap east across
    180°; span becomes (lon1 + 360) - lon0 and emitted sample longitudes are
    normalized into [-180, 180] so Open-Meteo accepts them as ordinary points.
    """
    lat_min, lat_max = sorted([lat0, lat1])
    if lon0 <= lon1:
        lon_span = lon1 - lon0
    else:
        lon_span = (lon1 + 360.0) - lon0
    nx = max(2, int(math.floor(lon_span / res)) + 1)
    ny = max(2, int(math.floor((lat_max - lat_min) / res)) + 1)
    while nx * ny > _MAX_GRID_POINTS:
        res *= 1.25
        nx = max(2, int(math.floor(lon_span / res)) + 1)
        ny = max(2, int(math.floor((lat_max - lat_min) / res)) + 1)
    lats: list[float] = []
    lons: list[float] = []
    for j in range(ny):
        lat = lat_max - j * res
        for i in range(nx):
            lon_raw = lon0 + i * res
            lon = ((lon_raw + 540.0) % 360.0) - 180.0
            lats.append(round(lat, 4))
            lons.append(round(lon, 4))
    return lats, lons, ny, nx, res


async def _fetch_open_meteo_precip_cloud(lats: list[float], lons: list[float],
                                         ts_dt: datetime
                                         ) -> tuple[list[float | None], list[float | None]]:
    """Hit Open-Meteo for precipitation + cloudcover at every point.

    Returns (precip_mm_per_hr, cloud_pct) parallel to lats/lons.
    """
    now = datetime.now(timezone.utc)
    is_archive = ts_dt < now - timedelta(days=7)
    base = _ARCHIVE_URL if is_archive else _FORECAST_URL

    # `cloud_cover` is the current Open-Meteo parameter name; `cloudcover`
    # still works via back-compat today but has returned None for some time
    # ranges in practice. Use the canonical name and read either shape below.
    params = {
        "latitude":  ",".join(f"{x:.4f}" for x in lats),
        "longitude": ",".join(f"{x:.4f}" for x in lons),
        "hourly":    "precipitation,cloud_cover",
        "timezone":  "UTC",
    }
    if is_archive:
        params["start_date"] = ts_dt.strftime("%Y-%m-%d")
        params["end_date"]   = ts_dt.strftime("%Y-%m-%d")
    else:
        delta_days = (now.date() - ts_dt.date()).days
        params["past_days"] = max(0, min(7, delta_days + 1))
        # Only ask for forecast hours if the requested ts is actually in the
        # future — otherwise we burn quota on 3 days of unused samples.
        if ts_dt.date() >= now.date():
            params["forecast_days"] = 3

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        r = await open_meteo_get(client, base, params, label="PRECIP")
    body = r.json()
    if isinstance(body, dict):
        body = [body]

    target_iso = ts_dt.strftime("%Y-%m-%dT%H:00")
    precip: list[float | None] = []
    cloud:  list[float | None] = []
    for entry in body:
        h = entry.get("hourly") or {}
        times = h.get("time") or []
        ps    = h.get("precipitation") or []
        # Accept either naming — upstream has been transitioning from the
        # legacy `cloudcover` to the canonical `cloud_cover`.
        cs    = h.get("cloud_cover") or h.get("cloudcover") or []
        try:
            idx = times.index(target_iso)
            pv = ps[idx] if idx < len(ps) else None
            cv = cs[idx] if idx < len(cs) else None
        except (ValueError, IndexError):
            pv, cv = None, None
        precip.append(pv)
        cloud.append(cv)
    while len(precip) < len(lats):
        precip.append(None)
        cloud.append(None)
    return precip, cloud


def _build_field_payload(lats: list[float], lons: list[float],
                         ny: int, nx: int, res: float,
                         precip: list[float | None],
                         cloud:  list[float | None],
                         ts_dt: datetime,
                         lon_w: float, lon_e: float) -> dict:
    """Pack precip + cloud into co-registered grids the frontend can rasterize.

    lon_w / lon_e are the requested west/east edges. For a dateline-crossing
    bbox (lon_w > lon_e), lo2 is reported as lon_e + 360 so the frontend's
    imageOverlay sees a monotonically-increasing longitude axis.
    """
    pclean: list[float] = [round(float(v), 2) if v is not None else 0.0 for v in precip]
    cclean: list[float] = [round(float(v), 1) if v is not None else 0.0 for v in cloud]
    # p_min/p_max should reflect *measured* values, not the None→0 substitution
    # used for rendering. Compute over the original (non-None) precip series.
    pvalid = [round(float(v), 2) for v in precip if v is not None]

    # Reshape to ny rows × nx cols (top row = highest lat).
    p_rows: list[list[float]] = [pclean[j * nx:(j + 1) * nx] for j in range(ny)]
    c_rows: list[list[float]] = [cclean[j * nx:(j + 1) * nx] for j in range(ny)]

    la1 = max(lats); la2 = min(lats)
    lo1 = lon_w
    lo2 = lon_e if lon_e >= lon_w else lon_e + 360.0
    return {
        "ts":     ts_dt.strftime("%Y%m%dT%H"),
        "ny":     ny,
        "nx":     nx,
        "la1":    la1, "la2": la2,
        "lo1":    lo1, "lo2": lo2,
        "dx":     round((lo2 - lo1) / max(1, nx - 1), 4),
        "dy":     round((la1 - la2) / max(1, ny - 1), 4),
        "res":    round(res, 3),
        "precip": p_rows,           # mm/hr
        "cloud":  c_rows,           # 0–100 %
        "p_max":  round(max(pvalid) if pvalid else 0.0, 2),
        "p_min":  round(min(pvalid) if pvalid else 0.0, 2),
        "unit":   "mm/hr|%",
    }


# ── /precip/field ───────────────────────────────────────────────────────────

@router.get("/precip/field")
async def precip_field(
    bbox: str = Query(..., description="lat0,lon0,lat1,lon1"),
    ts: str | None = Query(None, description="YYYYMMDDTHH; defaults to current hour"),
    res: float = Query(_DEFAULT_RES_DEG, ge=_MIN_RES_DEG, le=_MAX_RES_DEG),
):
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be lat0,lon0,lat1,lon1")
    try:
        lat0, lon0, lat1, lon1 = (float(p) for p in parts)
    except ValueError:
        raise HTTPException(400, "bbox values must be numeric")

    lat0 = max(-60.0, min(60.0, lat0)); lat1 = max(-60.0, min(60.0, lat1))
    lon0 = max(-180.0, min(180.0, lon0)); lon1 = max(-180.0, min(180.0, lon1))

    if ts:
        ts_dt = _parse_ts(ts)
    else:
        ts_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        ts = ts_dt.strftime("%Y%m%dT%H")

    key = _bbox_key(lat0, lon0, lat1, lon1, res)
    cache_path = PRECIP_CACHE_DIR / key / f"{ts}.json"
    if cache_path.exists():
        try:
            return JSONResponse(content=json.loads(cache_path.read_text()),
                                headers={"Cache-Control": "public, max-age=3600",
                                         "X-Precip-Cache": "hit"})
        except Exception as e:
            logger.warning(f"[PRECIP] cache read failed for {cache_path}: {e}")

    lats, lons, ny, nx, eff_res = _build_grid(lat0, lon0, lat1, lon1, res)
    try:
        precip, cloud = await _fetch_open_meteo_precip_cloud(lats, lons, ts_dt)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[PRECIP] Open-Meteo fetch failed: {type(e).__name__}: {e}")
        raise HTTPException(502, "precip upstream unavailable")

    payload = _build_field_payload(lats, lons, ny, nx, eff_res, precip, cloud, ts_dt, lon0, lon1)

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp, cache_path)
    except OSError as e:
        logger.warning(f"[PRECIP] cache write failed for {cache_path}: {e}")

    _maybe_evict()

    return JSONResponse(content=payload,
                        headers={"Cache-Control": "public, max-age=3600",
                                 "X-Precip-Cache": "miss"})


def _maybe_evict() -> None:
    """Run precip eviction at most once per hour. Best-effort, never raises."""
    global _LAST_EVICT_AT
    now = datetime.now(timezone.utc)
    if _LAST_EVICT_AT is not None and (now - _LAST_EVICT_AT) < _EVICT_INTERVAL:
        return
    _LAST_EVICT_AT = now
    try:
        evict_old_precip_frames()
    except Exception as e:
        logger.warning(f"[PRECIP EVICT] failed: {e}")


# ── Eviction ────────────────────────────────────────────────────────────────

def evict_old_precip_frames(max_age_hours: int = _CACHE_TTL_HOURS) -> int:
    """Delete cached precip JSON files older than *max_age_hours*."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    if not PRECIP_CACHE_DIR.exists():
        return 0
    for bbox_dir in PRECIP_CACHE_DIR.iterdir():
        if not bbox_dir.is_dir():
            continue
        for f in bbox_dir.glob("*.json"):
            try:
                stem = f.stem
                ft = datetime.strptime(stem, "%Y%m%dT%H").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if ft < cutoff:
                try:
                    f.unlink()
                    removed += 1
                except OSError:
                    pass
        try:
            if not any(bbox_dir.iterdir()):
                bbox_dir.rmdir()
        except OSError:
            pass
    if removed:
        logger.info(f"[PRECIP EVICT] removed {removed} cached frames older than {max_age_hours}h")
    return removed
