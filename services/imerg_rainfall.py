"""
services/imerg_rainfall.py — reusable GPM IMERG rainfall core.

Shared by the offline backfill tool (scripts/imerg_storm_rainfall.py) and the
live ingest loop (api/routes.refresh_current_season_rainfall). The CLI/track-
fetch/sidecar plumbing stays in the script; the geometry + NASA fetch live here
so both call one implementation.

Everything is fail-open and the heavy deps (earthaccess, xarray, numpy) are
imported lazily — `imerg_available()` reports whether the fetch path can run, so
callers in production can skip it cleanly when the deps/creds aren't installed.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

MM_PER_INCH = 25.4
FINAL_SHORT_NAME = "GPM_3IMERGDF"   # IMERG Final daily (lags ~3.5 months)
LATE_SHORT_NAME = "GPM_3IMERGDL"    # IMERG Late daily (lags ~14 h) — live path
DEFAULT_VERSION = "07"

_AVAILABLE: Optional[bool] = None


def imerg_available() -> bool:
    """True iff the fetch path's deps import. Cached. Never raises."""
    global _AVAILABLE
    if _AVAILABLE is None:
        try:
            import numpy  # noqa: F401
            import earthaccess  # noqa: F401
            import xarray  # noqa: F401
            _AVAILABLE = True
        except Exception:
            _AVAILABLE = False
    return _AVAILABLE


# ---------------------------------------------------------------------------
# Track helpers (pure stdlib)
# ---------------------------------------------------------------------------
def storm_days(track: list[dict]) -> list[date]:
    """Distinct UTC dates spanned by the track (inclusive)."""
    d0 = _as_utc(track[0]["time"]).date()
    d1 = _as_utc(track[-1]["time"]).date()
    out, d = [], d0
    while d <= d1:
        out.append(d)
        d += timedelta(days=1)
    return out


def track_bbox(track: list[dict], pad_deg: float) -> tuple[float, float, float, float]:
    lats = [p["lat"] for p in track]
    lons = [p["lon"] for p in track]
    return (
        max(-90.0, min(lats) - pad_deg),
        min(90.0, max(lats) + pad_deg),
        max(-180.0, min(lons) - pad_deg),
        min(180.0, max(lons) + pad_deg),
    )


def _as_utc(t) -> datetime:
    if isinstance(t, datetime):
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Geometry (numpy — imported lazily so import-time stays cheap)
# ---------------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    """Vectorized great-circle distance in km. Args may be scalars or arrays."""
    import numpy as np

    r = 6371.0088
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(np.asarray(lat2) - np.asarray(lat1))
    dlmb = np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def track_mask(lat2d, lon2d, track: list[dict], buffer_km: float):
    """Boolean grid: True where a cell is within buffer_km of ANY track point."""
    import numpy as np

    min_d = np.full(lat2d.shape, np.inf)
    for p in track:
        d = haversine_km(p["lat"], p["lon"], lat2d, lon2d)
        np.minimum(min_d, d, out=min_d)
    return min_d <= buffer_km


def summarize(accum_mm, lat2d, lon2d, mask) -> dict:
    """Peak cell, its location, areal mean, and area over thresholds."""
    import numpy as np

    masked = np.where(mask, accum_mm, np.nan)
    if not np.any(np.isfinite(masked)):
        raise ValueError("no valid cells inside the track buffer")
    iy, ix = np.unravel_index(np.nanargmax(masked), masked.shape)
    peak_mm = float(masked[iy, ix])

    dlat = abs(float(lat2d[1, 0] - lat2d[0, 0])) if lat2d.shape[0] > 1 else 0.1
    dlon = abs(float(lon2d[0, 1] - lon2d[0, 0])) if lon2d.shape[1] > 1 else 0.1
    cell_km2 = (dlat * 111.0) * (dlon * 111.0 * np.cos(np.radians(lat2d)))

    def area_over(th):
        return float(np.nansum(np.where(masked >= th, cell_km2, 0.0)))

    return {
        "peak_cell_mm": round(peak_mm, 1),
        "peak_cell_in": round(peak_mm / MM_PER_INCH, 2),
        "peak_cell_lat": round(float(lat2d[iy, ix]), 3),
        "peak_cell_lon": round(float(lon2d[iy, ix]), 3),
        "mean_footprint_mm": round(float(np.nanmean(masked)), 1),
        "area_gt_100mm_km2": round(area_over(100), 0),
        "area_gt_250mm_km2": round(area_over(250), 0),
        "area_gt_500mm_km2": round(area_over(500), 0),
        "cells_in_buffer": int(np.sum(mask)),
    }


# ---------------------------------------------------------------------------
# IMERG accumulation (earthaccess + xarray — imported lazily)
# ---------------------------------------------------------------------------
def accumulate_imerg(track, days, bbox, short_name, version):
    """Sum IMERG daily precip over `days`, subset to bbox. Returns
    (accum_mm[y,x], lat2d, lon2d) numpy arrays."""
    import numpy as np
    import earthaccess
    import xarray as xr

    earthaccess.login(persist=True)  # env vars > ~/.netrc > interactive

    lat_min, lat_max, lon_min, lon_max = bbox
    results = earthaccess.search_data(
        short_name=short_name,
        version=version,
        temporal=(str(days[0]), str(days[-1])),
        bounding_box=(lon_min, lat_min, lon_max, lat_max),
    )
    if not results:
        raise RuntimeError(
            f"No {short_name} v{version} granules for {days[0]}..{days[-1]} "
            "(Final lags ~3.5 months; Late ~14 h)"
        )

    accum = lat2d = lon2d = None
    used = 0
    for fh in earthaccess.open(results):
        ds = xr.open_dataset(fh, engine="h5netcdf")
        var = "precipitation" if "precipitation" in ds else "precipitationCal"
        da = ds[var].squeeze()
        if "lat" in da.dims and "lon" in da.dims:
            da = da.transpose("lat", "lon")
        sub = da.sel(lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max))
        vals = np.asarray(sub.values, dtype="float64")
        vals[vals < 0] = 0.0  # IMERG fill is -9999.9
        if accum is None:
            accum = np.zeros_like(vals)
            lon2d, lat2d = np.meshgrid(
                np.asarray(sub["lon"].values), np.asarray(sub["lat"].values)
            )
        accum += vals
        used += 1
        ds.close()

    if accum is None:
        raise RuntimeError("granules found but none overlapped the bounding box")
    logger.debug("IMERG accumulated %d daily granule(s) over %s", used, accum.shape)
    return accum, lat2d, lon2d


# ---------------------------------------------------------------------------
# High-level, fully fail-open entry point (used by the live ingest loop)
# ---------------------------------------------------------------------------
def observed_rainfall_for_track(
    track: list[dict],
    *,
    short_name: str = LATE_SHORT_NAME,
    version: str = DEFAULT_VERSION,
    buffer_km: float = 150.0,
    pad_deg: float = 2.0,
) -> Optional[dict]:
    """Storm-total IMERG rainfall summary for a track, or None on any problem.

    `track` is a list of {"time": datetime|iso, "lat": float, "lon": float}.
    Never raises — returns None if the deps/creds are missing or anything fails,
    so a live caller can treat it as best-effort enrichment.
    """
    try:
        if not track or len(track) < 2 or not imerg_available():
            return None
        days = storm_days(track)
        bbox = track_bbox(track, pad_deg)
        accum, lat2d, lon2d = accumulate_imerg(track, days, bbox, short_name, version)
        mask = track_mask(lat2d, lon2d, track, buffer_km)
        summary = summarize(accum, lat2d, lon2d, mask)
        summary["days"] = [days[0], days[-1]]
        return summary
    except Exception:
        logger.warning("IMERG rainfall lookup failed (best-effort)", exc_info=True)
        return None
