"""Fetch + cache forecast rain over land for the rain bar (core.land_rain).

Per storm, at most once per _TTL_S (served stale-while-revalidate after that,
so a page view never waits on a refresh once a result exists):

  NHC basins (AL/EP/CP): CurrentStorms.json -> the storm's public advisory
      (one ~10 KB text) -> its RAINFALL section. With amounts, the nearest
      US places also get NWS grid totals as town-level detail.
  Otherwise (JTWC basins, or the advisory can't be read): named places near
      the 0-72 h forecast track -> NWS grids for US places (Hawaii, Guam, PR,
      CONUS; one /points lookup per place, cached for the process), and one
      Open-Meteo request for the rest.

Fail-open: every failure degrades to {"available": False}, which leaves the
frontend's existing observed-track banner untouched.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional, Sequence, Tuple

import httpx

from core import land_rain as lr

logger = logging.getLogger(__name__)

NHC_CURRENT_STORMS = "https://www.nhc.noaa.gov/CurrentStorms.json"
NWS_POINTS = "https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
# api.weather.gov asks for an identifying User-Agent; the site URL is the contact.
_HEADERS = {"User-Agent": "StormDPS/1.0 (https://stormdps.com)", "Accept": "application/geo+json"}

_NHC_BASINS = ("AL", "EP", "CP")
_TTL_S = 30 * 60              # advisories: 3-6 h; NWS grids: ~hourly
_FAIL_TTL_S = 5 * 60
_BUILD_TIMEOUT_S = 25.0
_MAX_NWS_POINTS = 6
_NWS_CONCURRENCY = 3

_cache: dict = {}             # SID -> (built_at, result)
_inflight: dict = {}          # SID -> asyncio.Task
_nws_grid: dict = {}          # (lat, lon) rounded -> forecastGridData URL | None (not NWS land)
_places_cache: Optional[List[Tuple[float, float, str]]] = None


def all_places() -> List[Tuple[float, float, str]]:
    """Every named coastal place the site knows, all basins: the coastline
    waypoint DB (Atlantic/Gulf/Caribbean + WP/SH/NI lists), the Pacific-coast
    supplement (Mexico, Central America, Hawaii) and the rain-only extras."""
    global _places_cache
    if _places_cache is not None:
        return _places_cache
    out: List[Tuple[float, float, str]] = []
    try:
        from core.land_proximity import _get_coastline_db
        db = _get_coastline_db()
        for attr in ("waypoints", "wp_waypoints", "sh_waypoints", "ni_waypoints"):
            for w in getattr(db, attr, None) or []:
                if w.region_key != "open_ocean" and getattr(w, "name", None):
                    out.append((float(w.lat), float(w.lon), str(w.name)))
    except Exception as e:  # pragma: no cover - degrade to the supplements
        logger.warning(f"[land_rain] coastline DB unavailable: {e}")
    try:
        from core.pacific_coast import PACIFIC_COAST_POINTS
        out += [(float(a), float(b), str(n)) for a, b, n in PACIFIC_COAST_POINTS]
    except Exception as e:  # pragma: no cover
        logger.warning(f"[land_rain] Pacific coast points unavailable: {e}")
    out += list(lr.EXTRA_PLACES)
    _places_cache = out
    return out


async def _get_json(client: httpx.AsyncClient, url: str, **kw):
    r = await client.get(url, timeout=kw.pop("timeout", 10.0), **kw)
    r.raise_for_status()
    return r.json()


async def _advisory(client: httpx.AsyncClient, storm_id: str) -> Optional[dict]:
    """The parsed public advisory, or None when the storm has none / it can't
    be read (callers then fall back to grid/model points)."""
    try:
        cs = await _get_json(client, NHC_CURRENT_STORMS, headers={"User-Agent": _HEADERS["User-Agent"]})
    except Exception as e:
        logger.info(f"[land_rain] CurrentStorms failed: {e}")
        return None
    entry = next((s for s in cs.get("activeStorms", [])
                  if str(s.get("id", "")).lower() == storm_id.lower()), None)
    pub = (entry or {}).get("publicAdvisory") or {}
    if not pub.get("url"):
        return None
    try:
        r = await client.get(pub["url"], timeout=10.0, headers={"User-Agent": _HEADERS["User-Agent"]})
        r.raise_for_status()
    except Exception as e:
        logger.info(f"[land_rain] public advisory fetch failed for {storm_id}: {e}")
        return None
    text = lr.advisory_text(r.text)
    parsed = lr.parse_rainfall(text)
    return {"center": lr.issuing_center(text), "label": lr.advisory_label(text),
            "issued_utc": pub.get("issuance"), **parsed}


async def _nws_total(client: httpx.AsyncClient, place: dict, now: datetime) -> Optional[float]:
    key = (round(place["lat"], 3), round(place["lon"], 3))
    if key in _nws_grid:
        url = _nws_grid[key]
    else:
        r = await client.get(NWS_POINTS.format(lat=place["lat"], lon=place["lon"]),
                             headers=_HEADERS, timeout=8.0)
        if r.status_code == 404:           # not NWS land (Bahamas, Mexico, ...)
            _nws_grid[key] = None
            return None
        r.raise_for_status()
        url = r.json()["properties"]["forecastGridData"]
        if len(_nws_grid) < 5000:
            _nws_grid[key] = url
    if not url:
        return None
    data = await _get_json(client, url, headers=_HEADERS, timeout=10.0)
    values = ((data.get("properties") or {}).get("quantitativePrecipitation") or {}).get("values")
    return lr.sum_nws_qpf(values or [], now)


async def _nws_points(client: httpx.AsyncClient, places: Sequence[dict], now: datetime) -> List[dict]:
    sem = asyncio.Semaphore(_NWS_CONCURRENCY)

    async def one(p):
        async with sem:
            try:
                return {**p, "total_in": await _nws_total(client, p, now), "src": "nws"}
            except Exception as e:
                logger.info(f"[land_rain] NWS grid failed at {p['name']}: {e}")
                return {**p, "total_in": None, "src": "nws"}

    return list(await asyncio.gather(*(one(p) for p in places)))


async def _model_points(client: httpx.AsyncClient, places: Sequence[dict], now: datetime) -> List[dict]:
    if not places:
        return []
    params = {
        "latitude": ",".join(f"{p['lat']:.3f}" for p in places),
        "longitude": ",".join(f"{p['lon']:.3f}" for p in places),
        "hourly": "precipitation", "models": "best_match",
        "forecast_days": 4, "timezone": "GMT",
    }
    try:
        data = await _get_json(client, OPEN_METEO, params=params, timeout=12.0)
    except Exception as e:
        logger.info(f"[land_rain] Open-Meteo failed: {e}")
        return [{**p, "total_in": None, "src": "model"} for p in places]
    rows = data if isinstance(data, list) else [data]
    out = []
    for p, d in zip(places, rows):
        h = (d or {}).get("hourly") or {}
        out.append({**p, "total_in": lr.sum_hourly(h.get("time"), h.get("precipitation"), now),
                    "src": "model"})
    return out


def _source_of(points: Sequence[dict]) -> Optional[str]:
    srcs = {p["src"] for p in points if p.get("total_in") is not None}
    if not srcs:
        return None
    return srcs.pop() if len(srcs) == 1 else "mixed"


async def _build(storm_id: str, track: Sequence[dict], client: httpx.AsyncClient) -> dict:
    now = datetime.now(timezone.utc)
    advisory = None
    if storm_id[:2].upper() in _NHC_BASINS:
        advisory = await _advisory(client, storm_id)

    places = lr.select_places(track, all_places()) if track else []

    if advisory is not None:
        if not advisory["areas"] or max(a["max_in"] for a in advisory["areas"]) < lr.SHOW_MIN_IN:
            return lr.build(advisory, [], None)
        us = [p for p in places if lr.is_us_place(p["lat"], p["lon"])][:_MAX_NWS_POINTS]
        pts = await _nws_points(client, us, now) if us else []
        pts = [p for p in pts if p["total_in"] is not None]
        return lr.build(advisory, pts, "nws" if pts else None)

    if not places:
        return {"available": True, "show": False, "source": "none", "points": [],
                "note": "no named places near the forecast track"}
    us = [p for p in places if lr.is_us_place(p["lat"], p["lon"])][:_MAX_NWS_POINTS]
    nws = await _nws_points(client, us, now) if us else []
    got = {p["name"] for p in nws if p["total_in"] is not None}
    model = await _model_points(client, [p for p in places if p["name"] not in got], now)
    pts = [p for p in nws + model if p["total_in"] is not None]
    return lr.build(None, pts, _source_of(pts))


async def _refresh(key: str, storm_id: str, track: Sequence[dict],
                   http_client: Optional[httpx.AsyncClient]) -> dict:
    owns = http_client is None
    client = http_client or httpx.AsyncClient(timeout=10.0, follow_redirects=True)
    try:
        res = await asyncio.wait_for(_build(storm_id, track, client), _BUILD_TIMEOUT_S)
    except Exception as e:
        logger.warning(f"[land_rain] build failed for {storm_id}: {e!r}")
        res = {"available": False, "note": "error"}
    finally:
        if owns:
            await client.aclose()
        _inflight.pop(key, None)
    prev = _cache.get(key)
    if res.get("available") or not prev or not prev[1].get("available"):
        _cache[key] = (time.time(), res)      # never replace a good result with a failure
    else:
        _cache[key] = (time.time() - _TTL_S + _FAIL_TTL_S, prev[1])   # retry soon
    return res


async def get_land_rain(storm_id: str, track: Sequence[dict],
                        http_client: Optional[httpx.AsyncClient] = None,
                        wait_s: float = 6.0) -> dict:
    """Land-rain payload for one active storm (see core.land_rain.build).

    Only a storm's first request per process can wait (at most wait_s; the
    build keeps running and the next page view gets it). After that, expired
    results are served stale while a background refresh runs."""
    key = (storm_id or "").upper()
    if not key:
        return {"available": False, "note": "no storm id"}
    hit = _cache.get(key)
    fresh = hit and (time.time() - hit[0]) < (_TTL_S if hit[1].get("available") else _FAIL_TTL_S)
    if fresh:
        return hit[1]
    task = _inflight.get(key)
    if task is None:
        task = asyncio.create_task(_refresh(key, storm_id, list(track or []), http_client))
        _inflight[key] = task
    if hit:                                    # stale-while-revalidate
        return hit[1]
    try:
        return await asyncio.wait_for(asyncio.shield(task), wait_s)
    except asyncio.TimeoutError:
        return {"available": False, "note": "timeout"}
