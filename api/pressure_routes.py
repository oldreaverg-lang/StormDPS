"""
Mean Sea Level Pressure (MSLP) layer backed by Open-Meteo + METAR.

Two endpoints power the frontend Pressure overlay:

    GET /pressure/field?bbox=lat0,lon0,lat1,lon1&ts=YYYYMMDDTHH&res=1.0
        Returns a regular grid of pressure_msl values (hPa) covering the
        bbox at *res* degrees. Frontend uses this to:
          - Color-shade the field via a canvas overlay
          - Run d3-contour to draw isobars every 4 hPa

    GET /pressure/stations?bbox=lat0,lon0,lat1,lon1
        Returns recent METAR station-pressure observations inside the bbox
        from aviationweather.gov. Frontend renders these as small white
        pills on the map (the "1012 / 1009 / 1007" labels in the reference).

Both endpoints persist responses to the Railway volume so we don't burn the
Open-Meteo / METAR free tiers on repeat scrubs.
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
from storage import PRESSURE_CACHE_DIR, METAR_CACHE_DIR

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Upstream config ─────────────────────────────────────────────────────────

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_ARCHIVE_URL  = "https://archive-api.open-meteo.com/v1/era5"
_METAR_URL    = "https://aviationweather.gov/api/data/metar"
_HTTP_TIMEOUT = httpx.Timeout(25.0, connect=5.0)

_MAX_GRID_POINTS = 625   # 25×25 — frontend now uses a storm-centered 20°×20°
                         # bbox so the grid rarely auto-coarsens past 0.5°/cell.
                         # Each coord pair is ~7 chars; 625 pairs ≈ 4.4KB URL —
                         # safe under Cloudflare's 8KB upstream URL cap.
_MIN_RES_DEG     = 0.25
_MAX_RES_DEG     = 2.0
_DEFAULT_RES_DEG = 0.5
_CACHE_TTL_HOURS = 48
_METAR_TTL_MIN   = 15    # observations refresh hourly; 15-min cache is safe

# Throttle eviction scans: at most one per hour so a burst of cache writes
# doesn't repeatedly walk the cache directory.
_LAST_EVICT_AT: datetime | None = None
_EVICT_INTERVAL = timedelta(hours=1)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _bbox_key(lat0: float, lon0: float, lat1: float, lon1: float, res: float) -> str:
    # Normalize latitude order but *preserve* longitude order so a
    # dateline-crossing range (w > e) caches separately from a same-endpoints
    # non-wrapping range.
    s, n = sorted((float(lat0), float(lat1)))
    w, e = float(lon0), float(lon1)
    cross = "x" if w > e else "n"
    return f"{s:+06.1f}_{w:+07.1f}_{n:+06.1f}_{e:+07.1f}_{cross}_r{res:.1f}"


def _metar_key(lat0: float, lon0: float, lat1: float, lon1: float) -> str:
    s, n = sorted((float(lat0), float(lat1)))
    w, e = float(lon0), float(lon1)
    cross = "x" if w > e else "n"
    return f"{s:+06.1f}_{w:+07.1f}_{n:+06.1f}_{e:+07.1f}_{cross}"


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

    Returns (lats, lons, ny, nx, effective_res).

    Longitudes are interpreted with a dateline-aware convention: lon0 > lon1
    signals a bbox that wraps east across the dateline, so span is
    (lon1 + 360) - lon0. Emitted lon samples are normalized into [-180, 180]
    so Open-Meteo accepts them as ordinary point lookups.
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


async def _fetch_open_meteo_pressure(lats: list[float], lons: list[float],
                                     ts_dt: datetime) -> list[float | None]:
    """Hit Open-Meteo for pressure_msl at every point.

    Returns hPa values parallel to lats/lons (None for any point that
    didn't come back).
    """
    now = datetime.now(timezone.utc)
    is_archive = ts_dt < now - timedelta(days=7)
    base = _ARCHIVE_URL if is_archive else _FORECAST_URL

    params = {
        "latitude": ",".join(f"{x:.4f}" for x in lats),
        "longitude": ",".join(f"{x:.4f}" for x in lons),
        "hourly": "pressure_msl",
        "timezone": "UTC",
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
        r = await open_meteo_get(client, base, params, label="PRESSURE")
    body = r.json()
    if isinstance(body, dict):
        body = [body]

    target_iso = ts_dt.strftime("%Y-%m-%dT%H:00")
    out: list[float | None] = []
    for entry in body:
        h = entry.get("hourly") or {}
        times = h.get("time") or []
        ps    = h.get("pressure_msl") or []
        try:
            idx = times.index(target_iso)
            v = ps[idx] if idx < len(ps) else None
        except (ValueError, IndexError):
            v = None
        out.append(v)
    while len(out) < len(lats):
        out.append(None)
    return out


def _build_field_payload(lats: list[float], lons: list[float],
                         ny: int, nx: int, res: float,
                         pressures: list[float | None],
                         ts_dt: datetime,
                         lon_w: float, lon_e: float) -> dict:
    """Pack into a compact grid the frontend can color + contour.

    lon_w / lon_e are the requested western/eastern edges. For a
    dateline-crossing bbox (lon_w > lon_e numerically), we emit lo2 as
    lon_e + 360 so the frontend's imageOverlay + d3-contour math sees a
    monotonically-increasing longitude axis.
    """
    # Replace Nones with the row median so isobar contouring stays smooth.
    clean: list[float] = []
    valid = [p for p in pressures if p is not None]
    fallback = sum(valid) / len(valid) if valid else 1013.25
    for p in pressures:
        clean.append(round(float(p), 1) if p is not None else round(fallback, 1))

    # Reshape to ny rows × nx cols (top row = highest lat).
    rows: list[list[float]] = []
    for j in range(ny):
        rows.append(clean[j * nx:(j + 1) * nx])

    la1 = max(lats); la2 = min(lats)
    lo1 = lon_w
    lo2 = lon_e if lon_e >= lon_w else lon_e + 360.0
    return {
        "ts":   ts_dt.strftime("%Y%m%dT%H"),
        "ny":   ny,
        "nx":   nx,
        "la1":  la1, "la2": la2,
        "lo1":  lo1, "lo2": lo2,
        "dx":   round((lo2 - lo1) / max(1, nx - 1), 4),
        "dy":   round((la1 - la2) / max(1, ny - 1), 4),
        "res":  round(res, 3),
        "data": rows,
        "min":  round(min(clean), 1),
        "max":  round(max(clean), 1),
        "unit": "hPa",
    }


# ── /pressure/field ─────────────────────────────────────────────────────────

@router.get("/pressure/field")
async def pressure_field(
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

    # Clamp lats; preserve the lon0/lon1 ordering so a dateline-crossing
    # bbox (lon0 > lon1) is routed through the wrapping grid builder.
    lat0 = max(-60.0, min(60.0, lat0)); lat1 = max(-60.0, min(60.0, lat1))
    lon0 = max(-180.0, min(180.0, lon0)); lon1 = max(-180.0, min(180.0, lon1))

    if ts:
        ts_dt = _parse_ts(ts)
    else:
        ts_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        ts = ts_dt.strftime("%Y%m%dT%H")

    key = _bbox_key(lat0, lon0, lat1, lon1, res)
    cache_path = PRESSURE_CACHE_DIR / key / f"{ts}.json"
    if cache_path.exists():
        try:
            return JSONResponse(content=json.loads(cache_path.read_text()),
                                headers={"Cache-Control": "public, max-age=3600",
                                         "X-Pressure-Cache": "hit"})
        except Exception as e:
            logger.warning(f"[PRESSURE] cache read failed for {cache_path}: {e}")

    lats, lons, ny, nx, eff_res = _build_grid(lat0, lon0, lat1, lon1, res)
    try:
        pressures = await _fetch_open_meteo_pressure(lats, lons, ts_dt)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[PRESSURE] Open-Meteo fetch failed: {type(e).__name__}: {e}")
        raise HTTPException(502, "pressure upstream unavailable")

    payload = _build_field_payload(lats, lons, ny, nx, eff_res, pressures, ts_dt, lon0, lon1)

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp, cache_path)
    except OSError as e:
        logger.warning(f"[PRESSURE] cache write failed for {cache_path}: {e}")

    _maybe_evict()

    return JSONResponse(content=payload,
                        headers={"Cache-Control": "public, max-age=3600",
                                 "X-Pressure-Cache": "miss"})


def _maybe_evict() -> None:
    """Run pressure + METAR eviction at most once per hour. Best-effort."""
    global _LAST_EVICT_AT
    now = datetime.now(timezone.utc)
    if _LAST_EVICT_AT is not None and (now - _LAST_EVICT_AT) < _EVICT_INTERVAL:
        return
    _LAST_EVICT_AT = now
    try:
        evict_old_pressure_frames()
    except Exception as e:
        logger.warning(f"[PRESSURE EVICT] failed: {e}")
    try:
        evict_old_metar_files()
    except Exception as e:
        logger.warning(f"[METAR EVICT] failed: {e}")


def evict_old_metar_files(max_age_hours: int = 24) -> int:
    """Delete cached METAR JSON files whose mtime is older than *max_age_hours*.

    METAR observations refresh hourly, so stale bbox snapshots aren't useful.
    Returns the number of files removed.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    if not METAR_CACHE_DIR.exists():
        return 0
    for f in METAR_CACHE_DIR.glob("*.json"):
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mtime < cutoff:
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        logger.info(f"[METAR EVICT] removed {removed} cached files older than {max_age_hours}h")
    return removed


# ── /pressure/stations (METAR) ──────────────────────────────────────────────

def _altim_inhg_to_hpa(in_hg: float | None) -> float | None:
    """Convert an altimeter setting in inches Hg to hPa. Standard 33.8639."""
    if in_hg is None:
        return None
    try:
        return round(float(in_hg) * 33.8639, 1)
    except (TypeError, ValueError):
        return None


@router.get("/pressure/stations")
async def pressure_stations(
    bbox: str = Query(..., description="lat0,lon0,lat1,lon1"),
    max_stations: int = Query(60, ge=1, le=300),
):
    """Return METAR station pressure observations inside the bbox.

    Uses the AviationWeather.gov API. Stations without a usable pressure
    reading (no slp and no altim) are dropped. Results are thinned to
    *max_stations* by sampling on a grid so dense regions don't crowd
    the map.
    """
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(400, "bbox must be lat0,lon0,lat1,lon1")
    try:
        lat0, lon0, lat1, lon1 = (float(p) for p in parts)
    except ValueError:
        raise HTTPException(400, "bbox values must be numeric")

    lat_min, lat_max = sorted([lat0, lat1])
    # Preserve original lon0/lon1 ordering so dateline-crossing bboxes are
    # routed through the two-halves query path below. The cache key keys
    # on the original endpoints (with a wrap marker) via _metar_key.
    crosses_dateline = lon0 > lon1

    key = _metar_key(lat0, lon0, lat1, lon1)
    cache_path = METAR_CACHE_DIR / f"{key}.json"
    if cache_path.exists():
        try:
            mtime = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc)
            if datetime.now(timezone.utc) - mtime < timedelta(minutes=_METAR_TTL_MIN):
                return JSONResponse(content=json.loads(cache_path.read_text()),
                                    headers={"X-Metar-Cache": "hit"})
        except Exception as e:
            logger.warning(f"[METAR] cache read failed for {cache_path}: {e}")

    # AviationWeather wants bbox as: minLat,minLon,maxLat,maxLon and doesn't
    # accept dateline-crossing ranges natively. For a wrapping bbox we issue
    # two sub-queries — one for [lon0, 180] and one for [-180, lon1] — and
    # merge the station lists before thinning.
    if not crosses_dateline:
        lon_min, lon_max = sorted([lon0, lon1])
        bbox_params = [f"{lat_min:.2f},{lon_min:.2f},{lat_max:.2f},{lon_max:.2f}"]
    else:
        bbox_params = [
            f"{lat_min:.2f},{lon0:.2f},{lat_max:.2f},180.00",
            f"{lat_min:.2f},-180.00,{lat_max:.2f},{lon1:.2f}",
        ]
    params_list = [{"bbox": bp, "format": "json", "hours": 2} for bp in bbox_params]

    def _stale_or_empty():
        # If a cached payload exists on disk (even past TTL), serve it rather
        # than wiping the user's station layer when AviationWeather hiccups.
        # Bounded by the eviction job (24h) so it can't get arbitrarily stale.
        if cache_path.exists():
            try:
                return JSONResponse(
                    content=json.loads(cache_path.read_text()),
                    headers={"X-Metar-Cache": "stale"},
                )
            except Exception as e:
                logger.warning(f"[METAR] stale cache read failed for {cache_path}: {e}")
        return JSONResponse(content={"stations": [], "ts": None, "source": "metar"})

    body: list = []
    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            for params in params_list:
                r = await client.get(_METAR_URL, params=params)
                if r.status_code != 200:
                    logger.warning(f"[METAR] upstream {r.status_code}: {r.text[:200]}")
                    # If one half of a dateline pair fails, keep what we have
                    # so we don't blank the entire panel on a transient error.
                    continue
                sub = r.json() or []
                if isinstance(sub, list):
                    body.extend(sub)
    except Exception as e:
        logger.warning(f"[METAR] upstream fetch failed: {e}")
        return _stale_or_empty()

    if not body and not crosses_dateline:
        # Single-query path with no rows is indistinguishable from an upstream
        # outage for the user; keep serving stale data rather than clearing.
        return _stale_or_empty()

    # Build the station list with one observation per ICAO (most recent).
    by_icao: dict[str, dict] = {}
    for rec in body:
        icao = rec.get("icaoId") or rec.get("station_id") or rec.get("metar_id")
        if not icao:
            continue
        lat = rec.get("lat"); lon = rec.get("lon")
        if lat is None or lon is None:
            continue
        # Prefer slp (sea-level pressure, hPa already) when present.
        slp = rec.get("slp")
        try:
            slp = float(slp) if slp is not None else None
        except (TypeError, ValueError):
            slp = None
        # AviationWeather returns slp directly in hPa (e.g. 1013.2). Trust it
        # if it's within a sane meteorological range; otherwise fall back to
        # the altimeter setting converted from inHg.
        pressure_hpa = slp if (slp is not None and 870 <= slp <= 1085) else \
                       _altim_inhg_to_hpa(rec.get("altim"))
        if pressure_hpa is None or not (870 <= pressure_hpa <= 1085):
            continue
        prev = by_icao.get(icao)
        # Coerce obsTime / reportTime to a single int epoch so the
        # comparison below never mixes types (obsTime is epoch int,
        # reportTime is ISO string in the AviationWeather schema).
        ot_raw = rec.get("obsTime")
        if ot_raw is None:
            rt_raw = rec.get("reportTime")
            try:
                if rt_raw:
                    ot_raw = int(datetime.fromisoformat(
                        str(rt_raw).replace("Z", "+00:00")
                    ).timestamp())
            except Exception:
                ot_raw = None
        try:
            ob_time = int(ot_raw) if ot_raw is not None else 0
        except (TypeError, ValueError):
            ob_time = 0
        if prev is None or ob_time > (prev.get("_ot") or 0):
            by_icao[icao] = {
                "icao":     icao,
                "lat":      round(float(lat), 4),
                "lon":      round(float(lon), 4),
                "pressure": round(float(pressure_hpa), 1),
                "_ot":      ob_time,
            }

    stations = list(by_icao.values())

    # Spatial thinning: bucket onto a coarse grid, keep one station per bucket.
    if len(stations) > max_stations:
        # Pick a bucket size such that count/bucket ≈ max_stations
        cells = max(4, int(math.sqrt(max_stations)))
        # Dateline-aware longitude span: for a crossing bbox, the east edge
        # is (lon1 + 360) in the unwrapped frame; we use that to size buckets.
        lon_start = lon0
        lon_span = (lon1 - lon0) if not crosses_dateline else ((lon1 + 360.0) - lon0)
        dlat = (lat_max - lat_min) / cells or 1
        dlon = lon_span / cells or 1
        seen: dict[tuple[int, int], dict] = {}
        for s in stations:
            # Unwrap the station longitude into the same frame as lon_start so
            # bucket indices are contiguous across the dateline.
            slon = s["lon"]
            if crosses_dateline and slon < lon_start:
                slon += 360.0
            cy = int((s["lat"] - lat_min) / dlat)
            cx = int((slon - lon_start) / dlon)
            seen.setdefault((cy, cx), s)
        stations = list(seen.values())[:max_stations]

    for s in stations:
        s.pop("_ot", None)

    payload = {
        "stations": stations,
        "ts":       datetime.now(timezone.utc).strftime("%Y%m%dT%H%M"),
        "source":   "aviationweather.gov",
        "count":    len(stations),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")))
        os.replace(tmp, cache_path)
    except OSError as e:
        logger.warning(f"[METAR] cache write failed for {cache_path}: {e}")

    _maybe_evict()

    return JSONResponse(content=payload, headers={"X-Metar-Cache": "miss"})


# ── Eviction ────────────────────────────────────────────────────────────────

def evict_old_pressure_frames(max_age_hours: int = _CACHE_TTL_HOURS) -> int:
    """Delete cached pressure JSON files older than *max_age_hours*."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    removed = 0
    if not PRESSURE_CACHE_DIR.exists():
        return 0
    for bbox_dir in PRESSURE_CACHE_DIR.iterdir():
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
        logger.info(f"[PRESSURE EVICT] removed {removed} cached frames older than {max_age_hours}h")
    return removed
