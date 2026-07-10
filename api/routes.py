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
import contextvars
import csv
import hashlib
import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from timeutil import utcnow
from pathlib import Path
from typing import Optional
import hmac
import httpx
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse

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


# ------------------------------------------------------------------
# Admin auth + path-sanitization helpers
# ------------------------------------------------------------------

# Per-IP rate limit for admin-auth attempts. ADMIN_TOKEN is 32 bytes of
# entropy and effectively unbruteable, but capping attempts at 5/min per
# IP prevents log spam from probe traffic and shrinks the window if the
# token were ever rotated to something weaker. Belt-and-suspenders on
# top of the global 300/min slowapi default.
from collections import deque
from threading import Lock as _AdminLock

_ADMIN_RATE_LIMIT = 5
_ADMIN_RATE_WINDOW_SEC = 60
_admin_attempts: dict[str, deque] = {}
_admin_attempts_lock = _AdminLock()


def _admin_rate_check(client_ip: str) -> bool:
    """Return True if this IP is under the admin rate limit, False if over."""
    if not client_ip:
        # No identifiable client IP — let Cloudflare's own rate limiting
        # handle it rather than fail open or globally deny.
        return True
    now = time.time()
    with _admin_attempts_lock:
        dq = _admin_attempts.setdefault(client_ip, deque())
        while dq and (now - dq[0]) > _ADMIN_RATE_WINDOW_SEC:
            dq.popleft()
        if len(dq) >= _ADMIN_RATE_LIMIT:
            return False
        dq.append(now)
        return True


def _client_ip_for_admin(request: Request) -> str:
    """Resolve the real client IP, honoring Cloudflare's CF-Connecting-IP."""
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else ""


def require_admin(
    request: Request,
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
):
    """Reject requests without a valid admin token.

    Token is read from the ADMIN_TOKEN env var on each request so it can be
    rotated without a restart. If ADMIN_TOKEN is unset, the gated endpoint
    is disabled entirely (503) — refuses to fall back to "no auth". Adds
    a 5/min/IP rate limit on top of the global slowapi default so admin
    auth attempts are extra-throttled.
    """
    expected = os.getenv("ADMIN_TOKEN")
    if not expected:
        raise HTTPException(503, "admin endpoints disabled (ADMIN_TOKEN unset)")
    if not _admin_rate_check(_client_ip_for_admin(request)):
        raise HTTPException(429, "too many admin requests")
    if not x_admin_token or not hmac.compare_digest(x_admin_token, expected):
        raise HTTPException(403, "invalid or missing admin token")
    return True


_SAFE_STORM_ID_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")


def _safe_storm_id(storm_id: str) -> str:
    """Strip anything that isn't [A-Za-z0-9_-] from a storm_id before using
    it in a filesystem glob. Prevents callers passing `*` / `[abc]` /
    `../foo` to wipe arbitrary cache files."""
    cleaned = "".join(c for c in (storm_id or "") if c in _SAFE_STORM_ID_CHARS)
    if not cleaned:
        raise HTTPException(400, "invalid storm_id")
    return cleaned

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

# ── Single-flight + TTL cache for expensive IBTrACS searches ───────────────
# A historical name search fetches + parses the IBTrACS archive and computes
# IKE for the whole track (20-40s). Without coordination, N simultaneous users
# searching the same storm each launch that work, exhausting the httpx pool
# (max_connections=200) and the IKE executor. Single-flight collapses concurrent
# identical jobs into ONE computation (the rest await it); the TTL cache makes
# repeats instant across users and sessions.
_ibtracs_search_cache: dict[str, tuple] = {}   # key -> (results_list, timestamp)
_IBTRACS_SEARCH_TTL = timedelta(hours=12)
_inflight_jobs: dict[str, "asyncio.Future"] = {}
_inflight_guard = asyncio.Lock()


async def _single_flight(key: str, factory):
    """Run async ``factory()`` once per ``key`` even under concurrent callers;
    every caller receives the same result (or the same raised exception)."""
    async with _inflight_guard:
        fut = _inflight_jobs.get(key)
        is_owner = fut is None
        if is_owner:
            fut = asyncio.get_event_loop().create_future()
            _inflight_jobs[key] = fut
    if not is_owner:
        return await fut  # ride along with the in-flight computation
    try:
        result = await factory()
        if not fut.done():
            fut.set_result(result)
        return result
    except BaseException as e:
        if not fut.done():
            fut.set_exception(e)
        raise
    finally:
        async with _inflight_guard:
            _inflight_jobs.pop(key, None)


# Persistent data directory — centralised in storage.py
from storage import (
    PERSISTENT_DATA_DIR as _PERSISTENT_DATA,
    IKE_CACHE_DIR as _IKE_CACHE_DIR,
    DPS_CACHE_DIR as _DPS_CACHE_DIR,
    TRACK_CACHE_DIR as _TRACK_CACHE_DIR,
    IBTRACS_CACHE_FILE as _GLOBAL_IBTRACS_CACHE_FILE_PATH,
    IBTRACS_INDEX_FILE as _IBTRACS_INDEX_FILE,
    ACTIVE_STORMS_FILE as _ACTIVE_STORMS_FILE,
    CURRENT_SEASON_FILE as _CURRENT_SEASON_FILE,
    SST_TRACK_CACHE_DIR as _SST_TRACK_CACHE_DIR,
    RAINFALL_TRACK_CACHE_DIR as _RAINFALL_TRACK_CACHE_DIR,
    OBSERVED_TRACK_CACHE_DIR as _OBSERVED_TRACK_CACHE_DIR,
    atomic_write_json as _atomic_write_json,
    cache_read as _cache_read,
    cache_write as _cache_write,
)


# ── Per-storm along-track data cache (SST / rainfall / observed) ─────────────
# Historical storms have immutable observations, so the first fetch of a given
# storm is written to the persistent volume and every later view serves from
# disk. Helpers below are shared by the three /track endpoints.

def _track_latest_dt(points: list[dict]):
    """Most recent timestamp in a track, or None if unparseable."""
    latest = None
    for p in points:
        ts = (p.get("timestamp") or "")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "").split("+")[0][:19])
        except ValueError:
            continue
        if latest is None or dt > latest:
            latest = dt
    return latest


def _track_is_historical(points: list[dict], min_age_days: int = 7) -> bool:
    """True when the whole track is old enough that its observations are final.

    Guards against caching a still-evolving active storm or data that upstream
    archives (ERA5 ~5d lag, CO-OPS verification ~30d) haven't published yet.
    """
    latest = _track_latest_dt(points)
    if latest is None:
        return False
    return latest < (utcnow() - timedelta(days=min_age_days))


def _track_fingerprint(points: list[dict]) -> str:
    """Short stable hash of the posted track (count + rounded coords + hour).

    Keying the cache by storm_id ALONE is unsafe: the same storm can be posted
    at different resolutions (preset bundle track vs the finer /track endpoint),
    and the SST/rainfall results are index-aligned to the posted points — so a
    cache warmed at one resolution would return a misaligned array to a caller
    at another. The fingerprint gives each distinct track its own cache entry.
    """
    h = hashlib.sha1()
    for p in points:
        try:
            lat = round(float(p.get("lat")), 2)
            lon = round(float(p.get("lon")), 2)
            ts = str(p.get("timestamp") or "")[:13]  # to the hour
            h.update(f"{lat},{lon},{ts};".encode())
        except (TypeError, ValueError):
            h.update(b"x;")
    return f"{len(points)}_{h.hexdigest()[:12]}"


def _storm_cache_path(cache_dir, storm_id: str, points: list[dict]):
    """Sanitised <cache_dir>/<storm_id>__<track-fp>.json, or None if id unusable."""
    if not storm_id:
        return None
    safe = re.sub(r"[^A-Za-z0-9_.-]", "", str(storm_id))[:48]
    if not safe:
        return None
    return cache_dir / f"{safe}__{_track_fingerprint(points)}.json"


def _read_storm_cache(cache_path, version=None, max_age_s=None):
    """Return cached data for a storm layer, or None on miss/unreadable.

    When *version* is given, the on-disk payload must be a {"_v", "data"} wrapper
    with a matching version; any mismatch (or an old bare payload from before the
    layer's meaning changed) reads as a miss, auto-invalidating stale shapes.
    When *version* is None the raw payload is returned as-is (legacy bare files).

    *max_age_s* bounds freshness for active-storm entries: a file older than this
    reads as a miss so the live "tip" of the track is re-fetched. None = no age
    limit (immutable historical storms).
    """
    if cache_path is None or not cache_path.exists():
        return None
    if max_age_s is not None:
        try:
            if (time.time() - cache_path.stat().st_mtime) > max_age_s:
                return None  # stale active-storm entry → re-fetch
        except OSError:
            return None
    raw = _cache_read(cache_path)
    if raw is None:
        return None
    if version is None:
        return raw
    if isinstance(raw, dict) and raw.get("_v") == version:
        return raw.get("data")
    return None  # version mismatch / pre-versioning shape → treat as miss


def _write_storm_cache(cache_path, data, version=None) -> None:
    """Persist a storm layer result via the unified cache writer; best-effort.

    With *version*, wraps as {"_v": version, "data": data} so future schema
    changes invalidate old files via _read_storm_cache's version check. Eviction
    (and the size cap) are handled by cache_write.
    """
    if cache_path is None:
        return
    payload = data if version is None else {"_v": version, "data": data}
    _cache_write(cache_path, payload, evict_dir=cache_path.parent)


# Bump when the /rainfall/track payload meaning changes so old caches drop.
# v2: per-track-point value is now the 6-hour accumulation ending at the point
#     (summed from hourly ERA5), replacing the v1 whole-day precipitation_sum.
_RAINFALL_CACHE_VERSION = 2

# Bump when the /observed/track result changes so stale caches drop.
# v2: NDBC pre-2007 year-column parse fix — buoys now populate for pre-2007
#     storms (Katrina etc.) that previously cached with zero buoys.
# v3: CO-OPS surge records now carry surge_ft (observed water level − predicted
#     tide = true storm-surge residual) for the analyst surge-validation panel.
_OBSERVED_CACHE_VERSION = 3

# Active/recent storms aren't immutable, so their track-data is cached only
# briefly: the fingerprint key already changes when a new advisory updates the
# track, and this TTL bounds how long an unchanged track is served before the
# live "tip" (newly published SST, fresh observations) is re-fetched. Its real
# job is absorbing the traffic spike that concentrates on one storm during an
# event — all viewers in a cycle share a single upstream fetch.
_ACTIVE_TRACK_TTL_S = 5400  # 90 minutes

# Bump this when the unified DPS engine's formula changes so stale cached
# bundles are automatically invalidated on the next request.
# v6-sqrt: initial sqrt compression (T=60, S=4), flat RI bonus +15.
# v7-audit: WP formula audit — sub-basin mult before bonuses (was after, compounded
#           the ceiling); RI bonus scaled 5–20 by 24h magnitude (was flat +15);
#           no-landfall ×0.60 dampener (was no penalty for open-ocean Cat 5s);
#           compression retuned (T=70, S=2.5) — restores 90–98 discrimination
#           that v6 collapsed to 99 for every major WP storm.
# v8-audit-rainfall (2026-04): rec #5 — rainfall-footprint proxy adds
#           +6 × duration × breadth pts for rainfall-prone WP sub-basins
#           (JP / S.China / VN / TW). Separates Doksuri-class slow-broad
#           inland-flood storms from Goni-class small-intense ones.
# v9-wp-coastal (2026-04-15): Sinlaku audit — added Mariana Islands box +
#           full WP coastal coverage to core/cumulative_dpi.COASTAL_BOXES
#           and ZONE_WEIGHTS. Before v9, every WP storm had
#           duration_factor=0 / breadth_factor=0 (coastal boxes were
#           US-only). Retroactively re-credits Yagi, Hagibis, Haiyan,
#           Mangkhut, etc. Also fixes Saipan landfall routing via new
#           Mariana Islands region.
# v10-per-basin-compression (2026-05-14): Stage-5 sqrt compression is now
#           per-basin (compression_T, compression_S read from
#           BASIN_COEFFICIENTS) instead of a single global (T=70, S=2.5).
#           Atlantic reverts to its original (T=60, S=4) curve so the
#           hand-tuned Atlantic spread (Katrina 93 / Maria 86 / Harvey 83)
#           is preserved. Other basins retain (T=70, S=2.5) to handle
#           their bonus-stack saturation. See DURATION_STALL_COASTAL_AUDIT.md
#           §4 for the analysis that motivated reverting Atlantic.
# v11-ep-basin (2026-05-15): Eastern Pacific basin extended to match the
#           sophistication of WP ahead of El Niño 2026 season. EP now has
#           sub-basin multipliers (Mexico Pacific 1.10 / Baja 0.95 /
#           Central America 1.05 / Hawaii 0.85 / General 1.00), multi-
#           landfall bonus, orographic bonus (Sierra Madre del Sur,
#           Hawaiian volcanic peaks, Guatemala highlands), rainfall-
#           footprint proxy gated on Mexico/Central America sub-basins,
#           and a no-landfall dampener (×0.60 for open-ocean recurvers).
#           COASTAL_BOXES + ZONE_WEIGHTS extended to cover EP coast so
#           duration_factor and breadth_factor actually fire. See
#           EP_DPS_AUDIT.md for the full analysis + storm-by-storm
#           before/after.
# v12-exp-landrel (2026-07-02): C1 exponential compression curve replaces
#           sqrt+clamp (ATLANTIC_RI_COMPRESSION_AUDIT.md), and the engine
#           now emits `rainfall_land_relevant` so the flood banner can
#           distinguish rainfall volume from flood exposure (open-ocean
#           fish storms no longer show a red flood warning). Bump forces
#           on-demand recomputes so cached active-storm entries pick up
#           both changes.
_DPS_CACHE_VERSION = "v13-full-track"

# Cache for global IBTrACS catalog to avoid repeated large downloads/parses.
# We also persist a json cache file so restarts can reuse the catalog quickly.
_GLOBAL_IBTRACS_CATALOG_CACHE = None
_GLOBAL_IBTRACS_CATALOG_TIMESTAMP = None
_GLOBAL_IBTRACS_CATALOG_TTL = timedelta(hours=6)
_GLOBAL_IBTRACS_CACHE_FILE = _GLOBAL_IBTRACS_CACHE_FILE_PATH

# ------------------------------------------------------------------
# Per-storm IKE result cache
# ------------------------------------------------------------------
# Caches the full IKEResponse list for each storm+params combination.
# IKE depends on the wind model (grid resolution, quadrant method) but
# NOT on the DPS formula — DPS is computed client-side from cached IKE.
# Cache key: storm_id + grid_resolution + skip_points
# ------------------------------------------------------------------
# _IKE_CACHE_DIR imported from storage.py (dirs already created)

# Bump this when the IKE wind model changes (e.g., Holland profile, quadrant method)
# DPS formula changes do NOT require a version bump — DPS is client-side.
# v3: JTWC T+0 parser fix + JMA central-pressure enrichment for WP storms
# v4: ATCF b-deck (UCAR RAL) as primary source for in-season JTWC storms —
#     full historical track from storm birth, not just warning T+0 + forecasts.
# v5: WP basin DPS formula audit — fixed RI threshold (was unreachable),
#     removed WP_GENERAL double-count, expanded sub-basins (Korea, Hainan,
#     split China), tightened orographic radius to 110 km, raised orographic
#     cap to +9.
# v6: IKE methodology change — canonical Powell & Reinhold (2007) regression
#     (core.ike_coaps) replaces the heuristic band integration. Invalidates all
#     cached IKE so storms (incl. active, e.g. JANGMI) recompute with it.
_IKE_CACHE_VERSION = "v6-coaps"

# Eviction policy: keep at most this many cache files.  When exceeded, the
# oldest files by mtime are purged.  A typical hurricane season has ~20
# named storms; each storm generates a handful of parameter combos, so
# 500 is generous headroom while preventing unbounded growth from years
# of accumulated batch runs.
# IKE eviction limits — applied by the unified cache_write (via _save_ike_cache).
# IKE results are larger than track-data, so they get their own caps.
_IKE_CACHE_MAX_FILES = 500
_IKE_CACHE_MAX_SIZE_MB = 200  # soft cap — triggers eviction when exceeded

# Live storms recompute their track (~13s: b-deck/IBTrACS fetch + IKE compute)
# on every view because the storm is still evolving. Rather than bypass the
# cache entirely, cache the result with this short TTL: a burst of viewers
# during an event shares one compute, and the next advisory (~every 6h) is
# picked up within the window. The track GET is keyed by (storm_id, grid, skip)
# — no track fingerprint — so the TTL is the sole freshness gate.
_LIVE_TRACK_TTL_S = 1800  # 30 minutes

# Stale-while-revalidate for live-storm tracks (2026-07 perf audit): the first
# viewer after every TTL expiry used to BLOCK ~13s on the inline recompute —
# the single worst load-time event on the site. Now a TTL-expired cache is
# served immediately and ONE background task re-enters get_storm_track to
# recompute + re-save it. The contextvar marks that background call so it
# bypasses the cache read and the SWR branch (a shared flag would make
# concurrent USER requests skip the cache and pay the 13s inline).
_track_swr_inflight: set = set()
# Strong refs to in-flight refresh tasks: asyncio keeps only weak refs, so an
# unreferenced task can be GC'd mid-flight (silently killed). Done-callback
# discard keeps the set from growing.
_track_swr_tasks: set = set()
# Serve-stale ceiling: SWR normally serves a copy ≤1 advisory old, but after
# downtime the cache can be arbitrarily stale — beyond this, recompute inline.
_LIVE_TRACK_STALE_MAX_S = 6 * 3600
_track_swr_bypass: contextvars.ContextVar = contextvars.ContextVar(
    "_track_swr_bypass", default=False
)


async def _swr_refresh_track(storm_id: str, grid_resolution_km: float, skip_points: int):
    """Singleflight background recompute of a live storm's track cache."""
    key = (storm_id.upper(), grid_resolution_km, skip_points)
    if key in _track_swr_inflight:
        return
    _track_swr_inflight.add(key)
    _track_swr_bypass.set(True)   # task-local: create_task copied this context
    try:
        # Re-enters the route handler; with the bypass set it skips straight
        # to the fetch+compute pipeline and saves the fresh cache at the end.
        await get_storm_track(
            storm_id, grid_resolution_km=grid_resolution_km, skip_points=skip_points
        )
        logger.info(f"[TRACK SWR] {storm_id} background refresh complete")
    except Exception as e:
        # Fail-open: the stale copy keeps serving; the next request re-spawns.
        logger.warning(f"[TRACK SWR] {storm_id} background refresh failed: {e}")
    finally:
        _track_swr_inflight.discard(key)


def _ike_cache_key(storm_id: str, grid_res_km: float, skip: int) -> str:
    """Generate cache filename for a storm+params combo.

    storm_id is sanitized to [A-Za-z0-9_-] before being used in the
    filename so probe traffic / malformed IDs don't accumulate as oddly-
    named files in the IKE cache directory. Mirrors the sanitization
    already done in the DPS cache path."""
    safe_sid = "".join(c for c in (storm_id or "") if c in _SAFE_STORM_ID_CHARS)
    if not safe_sid:
        safe_sid = "UNKNOWN"
    raw = f"{safe_sid}_{grid_res_km}_{skip}_{_IKE_CACHE_VERSION}"
    h = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{safe_sid}_{h}.json"


def _load_ike_cache(storm_id: str, grid_res_km: float, skip: int,
                    max_age_s: float | None = None) -> list[dict] | None:
    """Load cached IKE results if present and valid (version + storm_id match).

    *max_age_s* bounds freshness for live storms: a file older than this reads
    as a miss so the next advisory is picked up. None = no age limit (immutable
    historical storms).
    """
    path = _IKE_CACHE_DIR / _ike_cache_key(storm_id, grid_res_km, skip)
    if max_age_s is not None:
        try:
            if (time.time() - path.stat().st_mtime) > max_age_s:
                return None  # stale live entry → recompute
        except OSError:
            return None  # missing/unreadable → miss
    data = _cache_read(path)
    if not isinstance(data, dict):
        return None
    if data.get("_version") != _IKE_CACHE_VERSION or data.get("_storm_id") != storm_id:
        return None
    return data.get("results")


def _save_ike_cache(storm_id: str, grid_res_km: float, skip: int,
                    results: list[dict], source: str, compute_ms: float):
    """Save IKE results via the unified cache writer (atomic + size-bounded)."""
    payload = {
        "_version": _IKE_CACHE_VERSION,
        "_storm_id": storm_id,
        "_source": source,
        "_grid_res_km": grid_res_km,
        "_skip_points": skip,
        "_compute_ms": round(compute_ms, 1),
        "_cached_at": utcnow().isoformat(),
        "_obs_count": len(results),
        "results": results,
    }
    _cache_write(
        _IKE_CACHE_DIR / _ike_cache_key(storm_id, grid_res_km, skip), payload,
        evict_dir=_IKE_CACHE_DIR,
        evict_max_files=_IKE_CACHE_MAX_FILES,
        evict_max_bytes=_IKE_CACHE_MAX_SIZE_MB * 1_048_576,
    )


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
    def _build_quads(qmap):
        if not qmap:
            return None
        return QuadrantRadii(
            NE=round(meters_to_nm(qmap.get("NE", 0) or 0), 1),
            SE=round(meters_to_nm(qmap.get("SE", 0) or 0), 1),
            SW=round(meters_to_nm(qmap.get("SW", 0) or 0), 1),
            NW=round(meters_to_nm(qmap.get("NW", 0) or 0), 1),
        )

    r34_quads = _build_quads(snapshot.r34_quadrants_m if snapshot else None)
    r50_quads = _build_quads(snapshot.r50_quadrants_m if snapshot else None)
    r64_quads = _build_quads(snapshot.r64_quadrants_m if snapshot else None)

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

    from core.ike_coaps import sdp_from_ike_ts, sdp_label
    _sdp_val = round(sdp_from_ike_ts(result.ike_total_tj), 2)
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
        r50_quadrants=r50_quads,
        r64_quadrants=r64_quads,
        forward_speed_knots=round(ms_to_knots(snapshot.forward_speed_ms), 1) if snapshot and snapshot.forward_speed_ms else None,
        forward_direction_deg=round(snapshot.forward_direction_deg, 1) if snapshot and snapshot.forward_direction_deg is not None else None,
        radii_confidence=radii_confidence,
        sdp=_sdp_val,
        sdp_label=sdp_label(_sdp_val),
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


def _load_current_season_storms() -> list[dict]:
    """Load the auto-ingested current-season NHC storms from the persistent
    volume (written by the background ingest loop). Same dict shape as
    ``_load_custom_storms``. Fail-open -> []."""
    try:
        if not _CURRENT_SEASON_FILE.exists():
            return []
        with open(_CURRENT_SEASON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        logger.debug("[SEASON] current-season file read failed", exc_info=True)
        return []


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

def _persist_active_storms(storms: list, ts: datetime) -> None:
    """Write the active-storms snapshot to the persistent volume.

    Swallows errors — persistence is an optimization, not a contract.
    The in-memory cache stays the source of truth while the process lives.
    """
    try:
        _atomic_write_json(_ACTIVE_STORMS_FILE, {
            "timestamp": ts.isoformat() + "Z",
            "storms": storms,
        })
    except Exception as e:  # pragma: no cover — disk / permissions
        logger.debug(f"[ACTIVE_STORMS] Persist to disk failed: {e}")


def load_active_storms_from_disk() -> bool:
    """Populate the in-memory active-storms cache from disk.

    Called at startup so the first request never sees an empty cache.
    Any snapshot is better than none; the next background refresh will
    overwrite it with current data. Returns True if anything was loaded.
    """
    global _active_storms_cache, _active_storms_cache_time
    try:
        if not _ACTIVE_STORMS_FILE.exists():
            return False
        with open(_ACTIVE_STORMS_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f)
        storms = payload.get("storms") or []
        ts_raw = payload.get("timestamp") or ""
        try:
            # Trim trailing Z / fractional seconds safely
            ts_clean = ts_raw.rstrip("Z").split(".")[0]
            ts = datetime.fromisoformat(ts_clean) if ts_clean else utcnow()
        except ValueError:
            ts = utcnow()
        # Only install the disk snapshot if the in-memory cache is empty —
        # never clobber a live cache with stale data.
        if _active_storms_cache is None:
            _active_storms_cache = storms
            _active_storms_cache_time = ts
            age_min = (utcnow() - ts).total_seconds() / 60.0
            logger.info(
                f"[ACTIVE_STORMS] Restored {len(storms)} storms from disk "
                f"(age {age_min:.1f} min)"
            )
            return True
    except Exception as e:
        logger.debug(f"[ACTIVE_STORMS] Disk restore failed: {e}")
    return False


async def _refresh_active_storms(request: Request):
    """
    Background task to refresh active storms cache.
    Acquires lock to ensure only ONE refresh happens at a time (prevents dogpile).
    """
    global _active_storms_cache, _active_storms_cache_time
    async with _active_storms_lock:
        # Double-check: another task may have refreshed while we waited for the lock
        now = utcnow()
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
                _active_storms_cache_time = utcnow()
                # Persist to disk so restarts don't cold-start from zero.
                _persist_active_storms(storms, _active_storms_cache_time)
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

    now = utcnow()

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
    request: Request,
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

    # Reuse the shared connection pool tuned in main.py's lifespan rather than
    # spinning up (and tearing down) a fresh httpx client per request.
    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
        try:
            # Name+year -> exact lookup; name-only -> scan for matches via
            # get_ibtracs_by_name. (Calling get_ibtracs_by_name_year with year=None
            # returned nothing — the bug.) Allow time for the historical-archive fetch.
            if year:
                snapshots = await asyncio.wait_for(
                    client.get_ibtracs_by_name_year(query.upper(), year, basin), timeout=20.0)
            else:
                snapshots = await asyncio.wait_for(
                    client.get_ibtracs_by_name(query.upper(), basin), timeout=20.0)
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

    # Convert HurricaneSnapshot objects to StormSummary objects.
    # Deduplicate by storm, using each storm's most recent snapshot as the
    # representative, and cap at `limit` DISTINCT storms. Iterating
    # most-recent-first (reversed) is what makes `limit` count distinct storms:
    # the old `snapshots[-limit:]` sliced raw track rows before deduping, so a
    # single long-lived storm's track points could fill the window and the
    # search would return far fewer storms than `limit` — or miss matches.
    results = []
    seen_ids = set()
    for snap in reversed(snapshots):  # newest observations first
        if snap.storm_id in seen_ids:
            continue
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
        if len(results) >= limit:
            break

    return results


# ── JTWC forecast-cone synthesis ──────────────────────────────────────────
# JTWC (WP/IO/SH) publishes forecast positions in its text warnings but no
# GeoJSON cone like NHC. We synthesize an NHC-style uncertainty cone by
# offsetting each forecast point perpendicular to the local track heading by
# the climatological ~5-yr-average track-error radius at that lead time.
# Climatological track-error radii table + interpolation now live in
# core/landfall_forecast.py (single server-side copy — the landfall-window
# estimator uses the same numbers). Refreshed 2026-07 to recent JTWC
# annual-report five-year mean track errors (WestPac); the previous table
# (30/50/70/90/135/180/230) was 2010s-era and drew cones ~25-35% wider than
# modern JTWC verification, overstating uncertainty. Keep in lockstep with
# CONE_ERR_NM in frontend/index.html (cone-aware forecast ERS sampling).
from core.landfall_forecast import (
    CONE_ERR_NM as _CONE_ERR_NM,
    interp_err_nm as _interp_err_nm,
)


def _synthesize_cone(forecast_track: list[dict]) -> list[list[float]]:
    """Approximate an NHC-style forecast cone as a [[lat, lon], ...] ring."""
    import math
    pts = [(p["lat"], p["lon"], p.get("hour", 0))
           for p in forecast_track
           if p.get("lat") is not None and p.get("lon") is not None]
    if len(pts) < 2:
        return []
    left, right = [], []
    n = len(pts)
    for i in range(n):
        lat, lon, hr = pts[i]
        lat1, lon1 = (pts[i - 1][0], pts[i - 1][1]) if i > 0 else (lat, lon)
        lat2, lon2 = (pts[i + 1][0], pts[i + 1][1]) if i < n - 1 else (lat, lon)
        dlon = math.radians(lon2 - lon1)
        y = math.sin(dlon) * math.cos(math.radians(lat2))
        x = (math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) -
             math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))
        brng = math.atan2(y, x)
        r_deg = _interp_err_nm(hr) / 60.0          # 1 nm ~ 1/60 deg latitude
        coslat = max(0.1, math.cos(math.radians(lat)))
        for sign, acc in ((1, left), (-1, right)):
            perp = brng + sign * math.pi / 2
            acc.append([round(lat + r_deg * math.cos(perp), 3),
                        round(lon + r_deg * math.sin(perp) / coslat, 3)])
    return left + right[::-1]


async def _jtwc_forecast(storm_id: str) -> dict:
    """Forecast track + synthesized cone for a JTWC (WP/IO/SH) storm, built
    from its warning bulletin. Lightweight: fetches text products only — no
    IKE/DPS compute."""
    from services.jtwc_client import JTWCClient
    async with JTWCClient() as jtwc:
        snaps = await jtwc.get_storm_track(storm_id)
    if not snaps:
        return {"storm_id": storm_id, "forecast_track": [], "cone_polygon": []}
    snaps = sorted(snaps, key=lambda s: s.timestamp)
    t0 = snaps[0].timestamp
    track = []
    for s in snaps:
        hr = round((s.timestamp - t0).total_seconds() / 3600)
        track.append({
            "lat": round(s.lat, 2),
            "lon": round(s.lon, 2),
            "hour": hr,
            "max_wind_kt": round(ms_to_knots(s.max_wind_ms)) if s.max_wind_ms else None,
            "time": s.timestamp.strftime("%a %m/%d %HZ"),
        })
    return {
        "storm_id": storm_id,
        "forecast_track": track,
        "cone_polygon": _synthesize_cone(track),
        "source": "jtwc",
        # tau-0 time of the warning the track came from — lets the frontend
        # show forecast age instead of leaving users to guess freshness.
        "valid_time_utc": t0.isoformat(),
    }


@router.get("/storms/{storm_id}/forecast")
async def get_storm_forecast(request: Request, storm_id: str):
    """
    Forecast track + uncertainty cone for an active storm.

    NHC (Atlantic / E-Pacific) returns the official GeoJSON forecast track and
    error cone. JTWC basins (WP/IO/SH) publish no GeoJSON product, so we
    synthesize the track from the warning bulletin's forecast positions plus a
    climatological uncertainty cone. Stall risk is computed from the implied
    forward speeds between forecast positions.
    """
    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
        try:
            forecast = await client.get_forecast_track(storm_id)
        except Exception as e:
            logger.warning(f"[forecast] NHC forecast failed for {storm_id}: {e}")
            forecast = {"storm_id": storm_id, "forecast_track": [], "cone_polygon": []}

    # JTWC fallback for non-NHC basins (or when NHC has nothing for this ID).
    if not forecast.get("forecast_track") and storm_id[:2].upper() in ("WP", "IO", "SH"):
        try:
            jtwc_fc = await _jtwc_forecast(storm_id)
            if jtwc_fc.get("forecast_track"):
                forecast = jtwc_fc
        except Exception as e:
            logger.warning(f"[forecast] JTWC synth failed for {storm_id}: {e}")

    # ── Stall Risk Analysis ──
    # Implied forward speed between consecutive forecast positions (Haversine /
    # time delta); flags stall risk when forecast speeds drop below thresholds.
    forecast["stall_risk"] = _compute_stall_risk(forecast.get("forecast_track", []))

    # ── Forecast landfall window (public landfall panel) ──
    # Fail-open: an estimator error must never drop the cone/track/stall data
    # this endpoint already serves.
    try:
        from core.landfall_forecast import compute_forecast_landfall
        forecast["landfall"] = compute_forecast_landfall(
            forecast.get("forecast_track", []))
    except Exception as e:
        logger.warning(f"[forecast] landfall estimate failed for {storm_id}: {e}")
        forecast["landfall"] = {"expected": False, "coverage": True, "description": ""}

    from datetime import datetime as _dt, timezone as _tz
    forecast["fetched_at_utc"] = _dt.now(_tz.utc).isoformat()

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
async def get_sst_along_track(
    request: Request,
    points: list[dict] = Body(...),
    storm_id: Optional[str] = Query(None, description="Storm id; enables per-storm disk cache for historical storms"),
):
    """
    Fetch sea surface temperature from ERDDAP for a list of track points.

    Expects a JSON array of {lat, lon, timestamp} objects.
    Returns SST (°C) at each point from the NOAA Geo-polar Blended SST dataset.

    When *storm_id* is supplied and the track is historical, the result is
    cached on the persistent volume and served from disk on later views.
    """
    # Input cap. Each point spawns an upstream ERDDAP request inside
    # services/noaa_client.get_sst_along_track, so an unbounded array
    # turns one HTTP request to us into thousands of egress requests.
    # 500 covers the longest IBTrACS track at 6-hourly resolution
    # (~125 days) with headroom.
    if not isinstance(points, list):
        raise HTTPException(status_code=400, detail="points must be an array")
    if len(points) > 500:
        raise HTTPException(
            status_code=413,
            detail=f"too many points ({len(points)}); max 500",
        )

    # Serve from the persistent volume: historical storms permanently,
    # active/recent storms briefly (TTL) to absorb the per-event traffic spike.
    cache_path = None
    cache_max_age = None
    if storm_id:
        cache_path = _storm_cache_path(_SST_TRACK_CACHE_DIR, storm_id, points)
        if not _track_is_historical(points):
            cache_max_age = _ACTIVE_TRACK_TTL_S
        cached = _read_storm_cache(cache_path, max_age_s=cache_max_age)
        if cached is not None:
            logger.info(f"[SST] cache hit {storm_id} ({len(cached)} points)")
            return cached

    from services.source_health import SourceHealthMonitor
    monitor = SourceHealthMonitor.instance()

    logger.info(f"[SST] Request received: {len(points)} track points")
    if points:
        logger.debug(f"[SST] First point: {points[0]}")
        logger.debug(f"[SST] Last point:  {points[-1]}")
    t0 = time.time()
    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
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

    # Persist only a result with real data, so a transient ERDDAP outage
    # (all-null) is never frozen into the historical cache.
    if cache_path is not None and valid_count > 0:
        _write_storm_cache(cache_path, sst_data)
        logger.info(f"[SST] cached {storm_id} ({valid_count} valid points)")
    return sst_data


@router.post("/rainfall/track")
async def get_rainfall_along_track(
    request: Request,
    points: list[dict] = Body(...),
    storm_id: Optional[str] = Query(None, description="Storm id; enables per-storm disk cache for historical storms"),
):
    """
    Observed precipitation along the storm track, from the Open-Meteo historical
    reanalysis archive (ERA5, global hourly, 1940-present). Returns one
    {"rainfall_mm": x|null} per posted point, in order.

    Each value is the **accumulation over the track segment ending at that
    point** — the rain that fell at the storm's location during that step (the
    window from the previous track point to this one, summed from hourly ERA5).
    This keeps every value on the track's own timeline (one per point, at its
    timestamp), independent per bar (not cumulative — the storm moves, so
    summing across points would add unrelated locations), and free of the
    daily-total double-representation the old per-day value had.

    Note: this is precipitation at the storm *center* track point, a proxy —
    the eye is typically drier than the eyewall. It is not the storm's peak or
    total footprint rainfall (that needs a gridded swath product).

    Reliability/scale: requests are batched by calendar day — one multi-location
    Open-Meteo call covers every point on that day (date range [day-1, day] also
    covers midnight-crossing windows), collapsing a 131-point track from 131
    calls to ~one-per-day. Failures are retried once; null means genuine
    no-data, never a dropped request. When *storm_id* is supplied and the track
    is historical the result is cached on the persistent volume — but only when
    every point resolved (no hard failures), so a transient rate-limit is never
    frozen into the cache.
    """
    if not isinstance(points, list):
        raise HTTPException(status_code=400, detail="points must be an array")
    if len(points) > 500:
        raise HTTPException(status_code=413, detail=f"too many points ({len(points)}); max 500")

    # Serve from the persistent volume: historical storms permanently,
    # active/recent storms briefly (TTL) to absorb the per-event traffic spike.
    cache_path = None
    cache_max_age = None
    if storm_id:
        cache_path = _storm_cache_path(_RAINFALL_TRACK_CACHE_DIR, storm_id, points)
        if not _track_is_historical(points):
            cache_max_age = _ACTIVE_TRACK_TTL_S
        cached = _read_storm_cache(cache_path, version=_RAINFALL_CACHE_VERSION, max_age_s=cache_max_age)
        if cached is not None:
            logger.info(f"[RAINFALL] cache hit {storm_id} ({len(cached)} points)")
            return cached

    from services.open_meteo_client import OPEN_METEO_HISTORICAL_URL

    shared = getattr(request.app.state, "http_client", None)
    own = None
    if shared is None:
        own = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))
    client = shared or own

    # Pre-parse timestamps so each point's window is [prev_point, this_point):
    # the rain that fell along that track segment.
    parsed: list[Optional[datetime]] = []
    for p in points:
        ts = p.get("timestamp")
        try:
            parsed.append(datetime.fromisoformat(str(ts).replace("Z", "").split("+")[0][:19]) if ts else None)
        except ValueError:
            parsed.append(None)

    _DEFAULT_STEP = timedelta(hours=6)
    _MAX_STEP = timedelta(hours=12)

    def _window_start(i: int, t_i: datetime) -> datetime:
        prev = parsed[i - 1] if i > 0 else None
        start = prev if prev is not None else (t_i - _DEFAULT_STEP)
        if not (timedelta(0) < (t_i - start) <= _MAX_STEP):
            start = t_i - _DEFAULT_STEP  # guard gaps / out-of-order timestamps
        return start

    # Outcome per point: ('ok', value) | ('nodata', None) | ('fail', None).
    # Points without coords/timestamp can't be placed → 'nodata'.
    outcomes: list[tuple[str, Optional[float]]] = [("nodata", None)] * len(points)

    # Group point indices by the calendar day of their timestamp. One request
    # per day (range [day-1, day]) covers every point that day plus any
    # midnight-crossing window — far fewer upstream calls than one-per-point.
    by_day: dict = {}
    for i, t_i in enumerate(parsed):
        if t_i is None or points[i].get("lat") is None or points[i].get("lon") is None:
            continue
        by_day.setdefault(t_i.date(), []).append(i)

    _CHUNK = 100          # max locations per Open-Meteo multi-location request
    sem = asyncio.Semaphore(6)

    async def _fetch_chunk(day, idxs: list[int]) -> None:
        params = {
            "latitude": ",".join(str(points[i]["lat"]) for i in idxs),
            "longitude": ",".join(str(points[i]["lon"]) for i in idxs),
            "start_date": (day - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": day.strftime("%Y-%m-%d"),
            "hourly": "precipitation", "timezone": "UTC",
        }
        payload = None
        for attempt in range(2):
            try:
                async with sem:
                    r = await client.get(OPEN_METEO_HISTORICAL_URL, params=params)
                r.raise_for_status()
                payload = r.json()
                break
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as e:
                if attempt == 0:
                    await asyncio.sleep(0.75)
                    continue
                logger.debug(f"[RAINFALL] batch {params['start_date']} ({len(idxs)} pts) failed: {type(e).__name__}: {e}")
                for i in idxs:
                    outcomes[i] = ("fail", None)
                return
        # Multi-location → list (input order, no dedup); single coord → dict.
        locs = payload if isinstance(payload, list) else [payload]
        for j, i in enumerate(idxs):
            loc = locs[j] if j < len(locs) else None
            hourly = (loc or {}).get("hourly") or {}
            htimes = hourly.get("time") or []
            hprec = hourly.get("precipitation") or []
            if not htimes:
                outcomes[i] = ("nodata", None)
                continue
            # Hourly precip is left-labeled (value at H covers [H, H+1)), so the
            # segment is the sum over hours [start, t_i).
            t_i = parsed[i]
            start = _window_start(i, t_i)
            total = 0.0
            for tstr, val in zip(htimes, hprec):
                if val is None:
                    continue
                try:
                    ht = datetime.fromisoformat(str(tstr)[:19])
                except ValueError:
                    continue
                if start <= ht < t_i:
                    total += float(val)
            outcomes[i] = ("ok", round(total, 1))

    tasks = []
    for day, idxs in by_day.items():
        for c in range(0, len(idxs), _CHUNK):
            tasks.append(_fetch_chunk(day, idxs[c:c + _CHUNK]))
    try:
        if tasks:
            await asyncio.gather(*tasks)
    finally:
        if own is not None:
            await own.aclose()

    results = [{"rainfall_mm": v} for (_s, v) in outcomes]
    had_failure = any(s == "fail" for (s, _v) in outcomes)
    valid = sum(1 for (s, _v) in outcomes if s == "ok")
    logger.info(f"[RAINFALL] {valid}/{len(results)} segments with precip across {len(by_day)} day-batches; had_failure={had_failure}")

    # Cache only a complete result: any hard fetch failure (as opposed to genuine
    # no-data) forces a re-fetch next view, so transient rate-limiting is never
    # frozen. Genuine zeros (dry segments) are real data and do get cached.
    if cache_path is not None and valid > 0 and not had_failure:
        _write_storm_cache(cache_path, results, version=_RAINFALL_CACHE_VERSION)
        logger.info(f"[RAINFALL] cached {storm_id} ({valid} valid segments)")
    return results


@router.post("/observed/track")
async def get_observed_peaks(
    request: Request,
    points: list[dict] = Body(...),
    storm_id: Optional[str] = Query(None, description="Storm id; enables per-storm disk cache for historical storms"),
):
    """
    Observed peak impacts at fixed stations near the storm track — ground truth
    for the map's "Observed peaks" layer:
      - NOAA CO-OPS tide gauges → peak storm surge (ft). Reliable, historical.
      - NDBC buoys → peak wind/wave. Additive/best-effort (often empty for
        older storms), so it never blocks the surge layer.

    Returns a flat list of {type, lat, lon, name, station, value, unit, label,
    time, source}. Each source is timeout-guarded so a slow/flaky feed can't
    hang the request.

    When *storm_id* is supplied and the track is historical, the result is
    cached on the persistent volume and served from disk on later views. The
    cache is only written when BOTH sources completed cleanly, so a timeout on
    one feed never freezes a partial station set.
    """
    if not isinstance(points, list):
        raise HTTPException(status_code=400, detail="points must be an array")
    if len(points) > 500:
        raise HTTPException(status_code=413, detail=f"too many points ({len(points)}); max 500")

    # Serve from the persistent volume. Historical gate is longer than
    # SST/rainfall: CO-OPS publishes *verified* water levels on a ~30-day lag
    # (preliminary before that), so we wait ~35 days before freezing surge peaks
    # permanently. Active/recent storms are cached only briefly (TTL) to absorb
    # the per-event traffic spike while still refreshing the live tip.
    cache_path = None
    cache_max_age = None
    if storm_id:
        cache_path = _storm_cache_path(_OBSERVED_TRACK_CACHE_DIR, storm_id, points)
        if not _track_is_historical(points, min_age_days=35):
            cache_max_age = _ACTIVE_TRACK_TTL_S
        cached = _read_storm_cache(cache_path, version=_OBSERVED_CACHE_VERSION, max_age_s=cache_max_age)
        if cached is not None:
            logger.info(f"[OBSERVED] cache hit {storm_id} ({len(cached)} stations)")
            return cached

    from services.coops_client import COOPSClient
    from services.ndbc_client import NDBCClient

    out: list[dict] = []
    coops_ok = False
    ndbc_ok = False

    # CO-OPS surge — the reliable layer.
    try:
        async def _coops():
            async with COOPSClient() as c:
                return await c.find_peak_along_path(points)
        for g in await asyncio.wait_for(_coops(), timeout=25.0):
            out.append({
                "type": "surge", "lat": g.lat, "lon": g.lon,
                "station": g.station, "name": g.name,
                "value": g.peak_ft_mllw, "unit": "ft",
                # True storm-surge residual (obs − predicted tide); None when tide
                # predictions weren't available. Consumed by the analyst panel.
                "surge_ft": g.peak_surge_ft,
                "surge_time": g.surge_time_utc or None,
                "label": f"Peak surge {g.peak_ft_mllw:.1f} ft",
                "time": g.peak_time_utc, "source": "NOAA CO-OPS tide gauge",
            })
        coops_ok = True
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning(f"[OBSERVED] CO-OPS failed/timed out: {e}")

    # NDBC wind/wave — additive, best-effort. Fetches run concurrently inside
    # the client (per-station timeout), so the outer cap is just a safety net
    # and partial results survive a slow feed instead of the layer going empty.
    try:
        async def _ndbc():
            async with NDBCClient() as c:
                return await c.find_peaks_along_path(points)
        for b in await asyncio.wait_for(_ndbc(), timeout=18.0):
            mph = round(b.peak_wind_ms * 2.23694)
            # Accept wind-OR-wave buoys: build the label from whatever logged.
            bits = []
            if mph > 0:
                bits.append(f"Peak wind {mph} mph")
            if b.peak_wave_m:
                bits.append(f"{b.peak_wave_m:.1f} m waves")
            label = ", ".join(bits) or "Buoy observation"
            out.append({
                "type": "wind", "lat": b.lat, "lon": b.lon,
                "station": b.station, "name": b.name,
                "value": mph if mph > 0 else b.peak_wave_m,
                "unit": "mph" if mph > 0 else "m",
                "label": label,
                "time": b.peak_time_utc, "source": "NDBC buoy",
            })
        ndbc_ok = True
    except (asyncio.TimeoutError, Exception) as e:
        logger.warning(f"[OBSERVED] NDBC failed/timed out: {e}")

    logger.info(f"[OBSERVED] {len(out)} observed peaks near track")

    # Cache only a complete result: both feeds must have finished without a
    # timeout/error, otherwise a clean-but-empty (open ocean) result is safe to
    # store while a partial one (one feed died) is re-fetched next time.
    if cache_path is not None and coops_ok and ndbc_ok:
        _write_storm_cache(cache_path, out, version=_OBSERVED_CACHE_VERSION)
        logger.info(f"[OBSERVED] cached {storm_id} ({len(out)} stations)")
    return out


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
        timestamp=utcnow(),
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
    now = utcnow()
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
        _valuation_cache[cache_key] = (response, utcnow())
        logger.info(f"[VALUATION] Cached result for {storm_id}")
        return response



# ------------------------------------------------------------------
# Historical track with IKE timeline (HURDAT2)
# ------------------------------------------------------------------

@router.get("/storms/{storm_id}/history", response_model=list[IKEResponse])
async def get_historical_ike_track(
    request: Request,
    storm_id: str,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0),
):
    """
    Compute IKE at every 6-hour best-track observation for a historical storm.

    Uses HURDAT2 extended format with quadrant wind radii when available,
    enabling asymmetric IKE computation for storms after ~2004.
    """
    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
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

    Backed by the same disk-cached IBTrACS catalog as /storms/catalog/global.
    The original implementation created a fresh NOAAClient with no cache_dir
    on every call, re-downloading IBTrACS from NCEI per request (~13 s
    response time per PageSpeed on cold-start). Reusing _build_global_catalog
    gives the response straight from the persistent volume.

    Returns storm ID (IBTrACS SID), name, year, basin, peak wind (kt),
    and Saffir-Simpson category. Includes custom storms for future years.
    """
    if (
        min_year == _CATALOG_DEFAULT_MIN_YEAR
        and max_year == _CATALOG_DEFAULT_MAX_YEAR
        and _CATALOG_DEFAULT_VIEW_FILE.exists()
    ):
        return FileResponse(
            _CATALOG_DEFAULT_VIEW_FILE,
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
        )

    catalog = await _build_global_catalog()
    if not catalog:
        return []
    return _harmonized([s for s in catalog if min_year <= s.get("year", 0) <= max_year])


def _lookup_storm_name_from_catalog(storm_id: str) -> Optional[str]:
    """
    Look up a storm's name given an ID (SID or ATCF) from the in-memory
    global catalog cache or the custom_storms.csv file. Used by the
    JTWC name-match fallback when an IBTrACS SID has no track data yet.
    Returns None if not found or the entry is unnamed.
    """
    try:
        if _GLOBAL_IBTRACS_CATALOG_CACHE:
            for entry in _GLOBAL_IBTRACS_CATALOG_CACHE:
                if entry.get("id") == storm_id:
                    name = (entry.get("name") or "").strip()
                    if name and name.upper() not in ("UNNAMED", "NOT_NAMED", "NOT NAMED", ""):
                        return name
                    return None
    except Exception:
        pass
    try:
        for entry in _load_custom_storms(1851, 2099):
            if entry.get("id") == storm_id:
                name = (entry.get("name") or "").strip()
                if name:
                    return name
    except Exception:
        pass
    # Auto-ingested current-season NHC storms (read straight off the volume so a
    # just-formed storm resolves its name even before the 6h catalog refresh).
    try:
        for entry in _load_current_season_storms():
            if entry.get("id") == storm_id:
                name = (entry.get("name") or "").strip()
                if name:
                    return name
    except Exception:
        pass
    return None


# ------------------------------------------------------------------
# Unified track endpoint (auto-routes HURDAT2 vs IBTrACS)
# ------------------------------------------------------------------

def _densify_snapshots_3h(snapshots: list) -> list:
    """Insert linearly-interpolated ~3-hourly midpoints into any gap > 4.5h.

    Applied ONLY to live/current-season storms (see get_storm_track), so the
    baked historical catalog stays at its native cadence — no re-bake, no
    golden-master drift. Densifying the live track gives the DPS engine finer
    duration/landfall-timing integration and the frontend smoother windfield +
    animation. Midpoint fields are linear interpolations of the two bracketing
    fixes (radii, wind, pressure, quadrants); forward direction is the
    great-circle bearing along the segment. IKE is computed physically on each
    resulting point downstream, so midpoints get real (not interpolated) IKE.
    """
    import math as _math
    from models.hurricane import HurricaneSnapshot as _HS

    def _lerp(a, b, f):
        if a is None or b is None:
            return a if b is None else b
        return a + (b - a) * f

    def _lerp_q(qa, qb, f):
        if not qa or not qb:
            return qa or qb
        return {k: _lerp(qa.get(k), qb.get(k), f) for k in ("NE", "SE", "SW", "NW")}

    def _bearing(a, b):
        la1, lo1, la2, lo2 = map(_math.radians, (a.lat, a.lon, b.lat, b.lon))
        dlon = lo2 - lo1
        y = _math.sin(dlon) * _math.cos(la2)
        x = _math.cos(la1) * _math.sin(la2) - _math.sin(la1) * _math.cos(la2) * _math.cos(dlon)
        return _math.degrees(_math.atan2(y, x)) % 360.0

    out: list = []
    for i, s in enumerate(snapshots):
        out.append(s)
        if i + 1 >= len(snapshots):
            break
        a, b = s, snapshots[i + 1]
        if a.timestamp is None or b.timestamp is None:
            continue
        gap_h = (b.timestamp - a.timestamp).total_seconds() / 3600.0
        if gap_h <= 4.5 or gap_h > 13.5:
            # <=4.5h: already 3h-ish (or finer near landfall) — leave it.
            # >13.5h: too large a hole (missing advisories) to fabricate
            # across — interpolating 5+ synthetic points over a day of missing
            # data is a guess, not a fix; leave the real gap intact.
            continue
        n = max(1, round(gap_h / 3.0) - 1)  # 6h→1 midpoint, 12h→3, ...
        brng = _bearing(a, b)
        for k in range(1, n + 1):
            f = k / (n + 1)
            out.append(_HS(
                storm_id=a.storm_id, name=a.name,
                timestamp=a.timestamp + (b.timestamp - a.timestamp) * f,
                lat=_lerp(a.lat, b.lat, f), lon=_lerp(a.lon, b.lon, f),
                max_wind_ms=_lerp(a.max_wind_ms, b.max_wind_ms, f),
                min_pressure_hpa=_lerp(a.min_pressure_hpa, b.min_pressure_hpa, f),
                rmw_m=_lerp(a.rmw_m, b.rmw_m, f),
                r34_m=_lerp(a.r34_m, b.r34_m, f),
                r50_m=_lerp(a.r50_m, b.r50_m, f),
                r64_m=_lerp(a.r64_m, b.r64_m, f),
                r34_quadrants_m=_lerp_q(a.r34_quadrants_m, b.r34_quadrants_m, f),
                r50_quadrants_m=_lerp_q(a.r50_quadrants_m, b.r50_quadrants_m, f),
                r64_quadrants_m=_lerp_q(a.r64_quadrants_m, b.r64_quadrants_m, f),
                forward_speed_ms=_lerp(a.forward_speed_ms, b.forward_speed_ms, f),
                forward_direction_deg=brng,
            ))
    return out


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
    prefix = storm_id[:2].upper()

    # --- Check IKE cache FIRST — skip all network I/O if already computed ---
    # Exception: active storms get a new advisory every 3-6 hours, so serving
    # a months-old IKE cache would show a stale position/intensity. Bypass the
    # disk cache for anything we consider "live":
    #   * All 8-char JTWC ATCF IDs (WP/IO/SH) — these only exist in-season
    #   * NHC ATCF IDs (AL/EP) that currently appear in /storms/active
    # Historical NHC storms (e.g. AL122005 Katrina) share the AL/EP prefix and
    # 8-char length, so we can't key off that alone — we have to consult the
    # active-storms cache. This keeps the hot path fast for museum pieces while
    # making the hourly DPS refresh loop actually pick up live advisories.
    _is_live_jtwc = prefix in ("WP", "IO", "SH") and len(storm_id) == 8
    _is_live_nhc = False
    if prefix in ("AL", "EP") and len(storm_id) == 8 and _active_storms_cache:
        _is_live_nhc = any(
            (s.get("id") or "").upper() == storm_id.upper()
            for s in _active_storms_cache
        )
    _is_live = _is_live_jtwc or _is_live_nhc
    # Historical storms are immutable (no age limit). Live storms get a short
    # TTL so a new advisory is picked up within _LIVE_TRACK_TTL_S — but a hit
    # still skips the b-deck/IBTrACS fetch AND the IKE compute (~13s for a live
    # storm), so a burst of viewers during an event shares a single compute.
    _ike_ttl = _LIVE_TRACK_TTL_S if _is_live else None
    _swr_bypass = _track_swr_bypass.get()
    cached = None if _swr_bypass else _load_ike_cache(
        storm_id, grid_resolution_km, skip_points, max_age_s=_ike_ttl
    )
    if cached:
        logger.info(f"[CACHE HIT] {storm_id} — {len(cached)} cached IKE results"
                    + (" (live, within TTL)" if _is_live else ""))
        return JSONResponse(content=cached, headers={
            "X-Track-Cache": "hit", "Cache-Control": "public, max-age=120"})
    if _is_live and not _swr_bypass:
        # Stale-while-revalidate: a TTL-expired live cache (normally one
        # advisory old, capped at _LIVE_TRACK_STALE_MAX_S after downtime) is
        # served NOW and refreshed in the background instead of blocking this
        # viewer ~13s on the fetch+IKE recompute.
        stale = _load_ike_cache(storm_id, grid_resolution_km, skip_points,
                                max_age_s=_LIVE_TRACK_STALE_MAX_S)
        if stale:
            _t = asyncio.create_task(
                _swr_refresh_track(storm_id, grid_resolution_km, skip_points))
            _track_swr_tasks.add(_t)
            _t.add_done_callback(_track_swr_tasks.discard)
            logger.info(f"[CACHE STALE→SWR] {storm_id} — served {len(stale)} "
                        f"stale results, background refresh spawned")
            return JSONResponse(content=stale, headers={
                "X-Track-Cache": "stale-refreshing",
                "Cache-Control": "public, max-age=120"})

    snapshots = []
    source = None
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

            # 3) IBTrACS by ATCF ID for AL/EP/WP/IO/SH storms (e.g. AL092017,
            #    WP262013 Haiyan). Covers historical basins globally. In-season
            #    WP/IO/SH storms won't be in IBTrACS yet (multi-month lag) —
            #    those fall to step 3a (b-deck) below.
            if not snapshots and prefix in ("AL", "EP", "WP", "IO", "SH") and len(storm_id) == 8:
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
                        # IBTrACS has a multi-month publication lag — current-year
                        # storms are expected misses, not service failures.
                        import datetime as _dt
                        _storm_year = int(storm_id[4:8]) if len(storm_id) >= 8 else 0
                        if _storm_year >= _dt.datetime.now().year:
                            logger.debug(f"[IBTrACS] {storm_id} not in IBTrACS (expected — current season)")
                        else:
                            _monitor.record_failure("ibtracs", error="ATCF ID not found", latency_ms=(time.time() - t0) * 1000)
                except (NOAAClientError, Exception):
                    _monitor.record_failure("ibtracs", error="ATCF lookup failed", latency_ms=(time.time() - t0) * 1000)
                    snapshots = []

            # 3a) ATCF b-deck — HISTORY from storm birth to latest synoptic
            #     analysis. Primary source for all in-season storms:
            #     - JTWC (WP/IO/SH): UCAR RAL mirror
            #     - NHC  (EP/AL):    NHC FTP (ftp.nhc.noaa.gov/atcf/btk/)
            #     IBTrACS has a multi-month publication lag so current-year
            #     storms fall through to here. The b-deck carries the full
            #     observed track so users see "where the storm has been."
            if not snapshots and prefix in ("WP", "IO", "SH", "EP", "AL") and len(storm_id) == 8:
                t0 = time.time()
                source_tag = "nhc_bdeck" if prefix in ("EP", "AL") else "jtwc_bdeck"
                try:
                    from services.atcf_bdeck_client import ATCFBDeckClient
                    async with ATCFBDeckClient() as bdeck:
                        snapshots = await bdeck.get_storm_track(storm_id)
                    if snapshots:
                        source = source_tag
                        _monitor.record_success(source_tag, latency_ms=(time.time() - t0) * 1000)
                        logger.info(
                            f"[TRACK] b-deck matched {storm_id} → {len(snapshots)} observations"
                        )
                    else:
                        _monitor.record_failure(source_tag, error="b-deck empty or unavailable", latency_ms=(time.time() - t0) * 1000)
                except Exception as e:
                    _monitor.record_failure(source_tag, error=f"b-deck lookup failed: {e}", latency_ms=(time.time() - t0) * 1000)
                    snapshots = []

            # 3b) JTWC warning-bulletin fallback for WP/IO/SH ATCF IDs.
            #     Only runs if the b-deck above was unavailable. The bulletin
            #     gives T+0 + forward forecasts; not ideal for a retrospective
            #     tracker, but better than nothing if UCAR's mirror is down.
            if not snapshots and prefix in ("WP", "IO", "SH") and len(storm_id) == 8:
                t0 = time.time()
                try:
                    from services.jtwc_client import JTWCClient
                    async with JTWCClient() as jtwc:
                        snapshots = await jtwc.get_storm_track(storm_id)
                    if snapshots:
                        source = "jtwc"
                        _monitor.record_success("jtwc", latency_ms=(time.time() - t0) * 1000)
                        logger.info(f"[TRACK] JTWC direct lookup matched {storm_id} → {len(snapshots)} points")
                    else:
                        _monitor.record_failure("jtwc", error="ATCF ID not in active warnings", latency_ms=(time.time() - t0) * 1000)
                except Exception as e:
                    _monitor.record_failure("jtwc", error=f"JTWC lookup failed: {e}", latency_ms=(time.time() - t0) * 1000)
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

            # 5b) JTWC name-match fallback: if storm_id is an IBTrACS SID
            #     (digit-leading) that didn't resolve, try to match it by
            #     name against the current JTWC active warnings. This
            #     handles in-season storms (e.g. Sinlaku) that appear in
            #     the global catalog under a synthetic SID before IBTrACS
            #     has published real track data.
            #
            #     Prefer the b-deck (full history) over the warning bulletin
            #     (T+0 + forecasts only). StormDPS is a retrospective tracker;
            #     forecast-only data makes every in-season storm render as a
            #     4-point future arc, which is the bug that prompted this fix.
            if not snapshots and storm_id[0].isdigit():
                try:
                    candidate_name = _lookup_storm_name_from_catalog(storm_id)
                    if candidate_name:
                        from services.jtwc_client import JTWCClient
                        async with JTWCClient() as jtwc:
                            warnings = await jtwc._fetch_active_warning_index()
                        match = next(
                            (
                                w for w in warnings
                                if w.get("name", "").upper() == candidate_name.upper()
                            ),
                            None,
                        )
                        matched_atcf = match["id"] if match else None

                        # Primary: b-deck (full history)
                        if matched_atcf:
                            try:
                                from services.atcf_bdeck_client import ATCFBDeckClient
                                async with ATCFBDeckClient() as bdeck:
                                    snapshots = await bdeck.get_storm_track(matched_atcf)
                                if snapshots:
                                    source = "jtwc_bdeck"
                                    logger.info(
                                        f"[TRACK] name-match b-deck: {storm_id} → "
                                        f"{candidate_name} ({matched_atcf}) "
                                        f"→ {len(snapshots)} observations"
                                    )
                            except Exception as e:
                                logger.debug(
                                    f"[TRACK] b-deck name-match failed for "
                                    f"{candidate_name}: {e}"
                                )

                        # Fallback: warning bulletin (forward-only — last resort)
                        if not snapshots:
                            async with JTWCClient() as jtwc:
                                snapshots = await jtwc.get_storm_track(candidate_name)
                            if snapshots:
                                source = "jtwc"
                                logger.info(
                                    f"[TRACK] warning-bulletin name-match: "
                                    f"{storm_id} → {candidate_name} "
                                    f"({len(snapshots)} points, forward-only)"
                                )
                except Exception as e:
                    logger.debug(f"[TRACK] JTWC name-match fallback failed for {storm_id}: {e}")
                    snapshots = snapshots or []

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

    # Densify LIVE/current-season tracks to ~3-hourly. Scoped to live storms
    # (Bavi onward) so the baked historical catalog is untouched — no re-bake,
    # no baseline drift. Feeds finer DPS integration + smoother windfield and
    # animation for the storm people are actually watching. IKE below is then
    # computed on the densified points.
    #
    # Also require the storm to be CURRENT-season: _is_live_jtwc is true for any
    # 8-char WP/IO/SH id, so a historical typhoon requested by its ATCF id
    # (e.g. WP262013) would otherwise densify and score inconsistently with its
    # baked IBTrACS-SID version. NHC live storms are already gated by the
    # active-storms list, and their id year is the current year, so they pass.
    _cur_season = (
        len(storm_id) == 8 and storm_id[4:8].isdigit()
        and int(storm_id[4:8]) >= datetime.now().year
    )
    if _is_live and _cur_season and len(snapshots) >= 2:
        _n0 = len(snapshots)
        snapshots = _densify_snapshots_3h(snapshots)
        if len(snapshots) != _n0:
            logger.info(f"[TRACK] {storm_id} densified {_n0}→{len(snapshots)} pts (3h, live)")

    # Compute IKE in parallel for all snapshots
    grid_resolution_m = grid_resolution_km * 1000
    ike_batch = await _compute_ike_batch(snapshots, grid_resolution_m, max_workers=4)

    results = [_ike_to_response(ike, snap) for ike, snap in ike_batch]

    # --- Save to cache for future requests ---
    # Live storms are cached too, but read back only within _LIVE_TRACK_TTL_S
    # (see the read above), so the next advisory is picked up promptly while a
    # burst of concurrent viewers during an event still shares one compute.
    compute_ms = (time.time() - t0) * 1000
    _save_ike_cache(
        storm_id, grid_resolution_km, skip_points,
        [_ike_response_to_dict(r) for r in results],
        source=source or "unknown",
        compute_ms=compute_ms,
    )
    logger.info(f"[CACHE MISS] {storm_id} — computed {len(results)} IKE results in {compute_ms:.0f}ms, saved"
                + (" (live, TTL)" if _is_live else ""))

    return results


# ------------------------------------------------------------------
# Unified DPS endpoint — single source of truth for all DPS values
# ------------------------------------------------------------------
#
# Every location the frontend shows DPS (hero card, accordion, map
# marker colors, sparkline) must fetch from this endpoint so that the
# hero, accordion, and map are derived from the exact same formula and
# cached numbers. Presets load from compiled_bundle.json (built once
# at deploy time); ad-hoc searches hit this endpoint on demand and the
# result is cached to the persistent volume for subsequent loads.

def _dps_cache_path(storm_id: str) -> Path:
    safe = "".join(c for c in storm_id if c.isalnum() or c in "_-")
    return _DPS_CACHE_DIR / f"{safe}_{_DPS_CACHE_VERSION}.json"


def _load_dps_cache(storm_id: str) -> Optional[dict]:
    # Version is encoded in the filename (_dps_cache_path), so a bump orphans
    # old files automatically — no in-content version check needed here.
    data = _cache_read(_dps_cache_path(storm_id))
    return data if isinstance(data, dict) else None


def _dps_cache_needs_rearm(bundle: dict) -> bool:
    """True when a cached bundle carries a DEFERRED no-landfall dampener but
    the storm's last fix has aged past the freshness window.

    compile_cache defers the retrospective ×0.60 fish-storm dampener while a
    track is in progress (track_is_in_progress, 7-day window). But the hourly
    force-refresh only covers storms still on the active list — its LAST
    recompute happens while the final fix is hours old, so a dead WP/EP
    fish storm's final (undampened) bundle would otherwise be served from
    cache forever. Treating that bundle as a miss makes the next view
    recompute: the track is now stale, the dampener re-applies, and the
    corrected bundle overwrites the cache once.
    """
    try:
        notes = bundle.get("adjustment_notes") or ""
        if "no-LF dampener deferred" not in notes:
            return False
        ts = bundle.get("_last_fix_ts")
        if not ts:
            # Bundle predates the _last_fix_ts stamp — approximate with the
            # last DPI timeseries entry (dpi>0 fixes only; close enough for a
            # 7-day staleness check, and errs toward recomputing).
            series = bundle.get("dpi_timeseries") or []
            ts = (series[-1] or {}).get("t") if series else None
        if not ts:
            return True  # deferred note but unknown age → recompute to be safe
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(str(ts))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_s = (datetime.now(timezone.utc) - dt).total_seconds()
        return age_s >= 7 * 86400.0
    except Exception:
        return True  # malformed cache entry → recompute (fail toward correctness)


def _invalidate_dps_cache(storm_id: str) -> None:
    """Mark a storm's cached DPS bundle stale so the next /dps call recomputes
    (e.g. after recording observed rainfall). On the Railway volume the app user
    can overwrite existing files but cannot unlink them (no dir-write; see
    _save_dps_cache), so fall back to overwriting with invalid JSON — which
    _load_dps_cache treats as a miss. Fully fail-open."""
    fp = _dps_cache_path(storm_id)
    try:
        fp.unlink()
    except FileNotFoundError:
        return  # nothing cached → next view computes fresh anyway
    except OSError:
        try:
            fp.write_text("stale")  # invalid JSON → _load_dps_cache returns None
        except OSError:
            logger.debug("[DPS CACHE] could not invalidate %s", storm_id)


def _save_dps_cache(storm_id: str, bundle: dict) -> None:
    """Persist a storm's computed DPS bundle via the unified cache writer.

    The `/app/persistent/cache/dps/` dir on the Railway volume is root-owned
    (mode 755): the app uid can overwrite existing files but cannot create new
    ones (no dir-write, so a .tmp+rename also fails). cache_write's atomic-then-
    in-place-overwrite fallback is exactly what keeps the pre-existing storm
    files updatable; brand-new storm_ids will log a single write warning per
    refresh tick until the volume perms are fixed (Dockerfile chown at boot).
    No eviction here — the dir is root-owned (unlink would fail) and DPS files
    are bounded by the storm count anyway.
    """
    _cache_write(_dps_cache_path(storm_id), bundle)


def _ike_responses_to_engine_snapshots(responses: list) -> list[dict]:
    """
    Convert /storms/{id}/track output (IKEResponse objects or dicts) into the
    plain-dict shape that core.dps_engine.compute_storm_dps expects.
    """
    out: list[dict] = []
    for r in responses:
        d = r if isinstance(r, dict) else _ike_response_to_dict(r)
        out.append({
            "timestamp": d.get("timestamp", ""),
            "lat": d.get("lat") or 0.0,
            "lon": d.get("lon") or 0.0,
            "max_wind_ms": d.get("max_wind_ms") or 0.0,
            "min_pressure_hpa": d.get("min_pressure_hpa") or 1013.0,
            "r34_nm": d.get("r34_nm") or 0.0,
            "r64_nm": d.get("r64_nm") or 0.0,
            "rmw_nm": d.get("rmw_nm") or 0.0,
            "forward_speed_knots": d.get("forward_speed_knots") or 0.0,
            "ike_total_tj": d.get("ike_total_tj") or 0.0,
            "r34_quadrants": d.get("r34_quadrants"),
            "r50_quadrants": d.get("r50_quadrants"),
            "r64_quadrants": d.get("r64_quadrants"),
        })
    return out


# ------------------------------------------------------------------
# Storm analogs — "storms like this one"
# ------------------------------------------------------------------
# Nearest neighbors in the baked catalog by weighted L1 distance over the
# canonical bundle features (intensity, size/IKE, rainfall, duration, basin).
# Powers the storm page's analog strip; each analog links into /compare.
# The comparison pool is compiled_bundle.json (223 curated storms); live
# storms are scored from their (cached) /dps bundle.

def _analog_pool() -> dict:
    """Bundle storms keyed by id — via seo's VOLUME-FIRST cached reader.
    compile_cache can rebake onto the persistent volume between deploys;
    reading only the repo-baked copy would rank analogs with scores the
    site no longer displays (and hold a second multi-MB parsed copy of a
    bundle seo already keeps in memory)."""
    try:
        from seo import _read_compiled_bundle
        return (_read_compiled_bundle() or {}).get("storms", {}) or {}
    except Exception as e:
        logger.warning(f"[ANALOGS] bundle load failed: {e}")
        return {}


def _analog_distance(a: dict, b: dict) -> float:
    """Weighted L1 distance in the bundle feature space (0 = identical)."""
    def n(x, cap):
        try:
            return min(float(x or 0), cap) / cap
        except (TypeError, ValueError):
            return 0.0
    d = 0.0
    d += 0.30 * abs(n(a.get("dps"), 100) - n(b.get("dps"), 100))
    d += 0.20 * abs(n(a.get("peak_wind_kt"), 160) - n(b.get("peak_wind_kt"), 160))
    d += 0.20 * abs(n(a.get("peak_ike_tj"), 300) - n(b.get("peak_ike_tj"), 300))
    d += 0.10 * abs(n(a.get("rainfall_warning"), 100) - n(b.get("rainfall_warning"), 100))
    d += 0.05 * abs(n(a.get("track_hours"), 400) - n(b.get("track_hours"), 400))
    if (a.get("basin") or "") != (b.get("basin") or ""):
        d += 0.15
    return d


@router.get("/storms/{storm_id}/analogs")
async def get_storm_analogs(
    storm_id: str,
    n: int = Query(4, ge=1, le=8, description="How many analogs to return"),
):
    """Historical analogs: the catalog storms nearest to this one in the
    canonical DPS feature space. Live storms are scored from their live
    /dps bundle; baked storms come straight from the pool entry."""
    pool = _analog_pool()
    if not pool:
        raise HTTPException(status_code=503, detail="catalog unavailable")
    sid = storm_id.upper()
    query = pool.get(sid)
    if query is None:
        # Live / ad-hoc storm: reuse the canonical /dps machinery (cached).
        # Every Query-default param is passed EXPLICITLY — calling a route
        # handler directly otherwise leaves truthy Query(...) sentinel
        # objects in the parameters (the documented re-entry trap).
        try:
            res = await get_storm_dps(storm_id, name=None, year=None,
                                      grid_resolution_km=15.0, skip_points=0,
                                      force=False)
            query = json.loads(res.body) if isinstance(res, JSONResponse) else res
        except HTTPException:
            raise
        except Exception as e:
            logger.warning(f"[ANALOGS] dps lookup failed for {storm_id}: {e}")
            raise HTTPException(status_code=404, detail="storm not found")
    if not isinstance(query, dict) or not query.get("dps"):
        raise HTTPException(status_code=404, detail="storm has no DPS bundle")
    q_name = str(query.get("name") or sid).lower()
    q_year = query.get("year")
    ranked = []
    for aid, s in pool.items():
        if aid.upper() == sid or not s.get("dps"):
            continue
        # The identity seam: the same storm can appear under both its ATCF
        # id and IBTrACS SID — never offer a storm as its own analog.
        if str(s.get("name") or "").lower() == q_name and s.get("year") == q_year:
            continue
        ranked.append((_analog_distance(query, s), aid, s))
    ranked.sort(key=lambda t: t[0])
    analogs = [{
        "id": aid,
        "name": s.get("name") or aid,
        "year": s.get("year"),
        "basin": s.get("basin"),
        "dps": round(s.get("dps") or 0),
        "dps_label": s.get("dps_label"),
        "category": s.get("category"),
        "peak_wind_kt": s.get("peak_wind_kt"),
        "similarity": max(0, round((1.0 - dist) * 100)),
    } for dist, aid, s in ranked[:n]]
    return JSONResponse(content={
        "query": {"id": sid, "name": query.get("name") or sid,
                  "year": q_year, "dps": round(query.get("dps") or 0),
                  "dps_label": query.get("dps_label")},
        "analogs": analogs,
    }, headers={"Cache-Control": "public, max-age=3600"})


# ── Storm identity aliases (docs/DATA_ARCHITECTURE.md roadmap #2) ──────────
# data/storm_aliases.json maps ATCF ids <-> IBTrACS SIDs for every named
# storm since 1980 (built by scripts/build_alias_table.py from USA_ATCF_ID).
# It exists to retire the dual-identity bug class: raw ids rendered as
# names, and bundle entries (name, actual_impact) unreachable because the
# caller held the other id form. Implementation lives in the dependency-
# light core/storm_identity.py so the offline suite can test it.
from core.storm_identity import (
    ID_FORM_RE as _ID_FORM_RE,
    harmonize_catalog as _harmonize_catalog_rows,
    storm_identity as _storm_identity,
)


def _dps_cache_scores() -> dict:
    """{storm_id: {dps, dps_label}} for every DPS volume-cache entry at the
    current cache version. Live/unbaked storms (e.g. a current-season WP
    system) aren't in the compiled bundle — their hero score lives in this
    cache, written by the hourly warm loop and by page views. Letting the
    catalog overlay read it closes the Sinlaku case: sidebar 36 vs hero 82.
    Bounded: the cache dir holds tens of entries (storage.py eviction caps).
    """
    out: dict = {}
    try:
        suffix = f"_{_DPS_CACHE_VERSION}.json"
        for f in _DPS_CACHE_DIR.glob(f"*{suffix}"):
            data = _cache_read(f)
            if isinstance(data, dict) and data.get("dps") is not None:
                out[f.name[: -len(suffix)]] = {
                    "dps": data["dps"], "dps_label": data.get("dps_label")}
    except Exception:
        logger.exception("[catalog] dps-cache score scan failed")
    return out


def _harmonized(catalog: list) -> list:
    """Sidebar catalog rows with the hero's engine scores overlaid, labels
    canonicalized, and SID+ATCF twins collapsed (cross-surface score audit
    2026-07-10). Engine scores come from the compiled bundle first, then the
    live DPS cache (bundle wins on conflict). Fail-open: any error serves
    the raw catalog."""
    try:
        from seo import _read_compiled_bundle
        lookup = _dps_cache_scores()
        lookup.update(_read_compiled_bundle().get("storms", {}))
        return _harmonize_catalog_rows(catalog, lookup)
    except Exception:
        logger.exception("[catalog] harmonize wrapper failed")
        return catalog


def _overlay_bundle_identity(payload: dict, storm_id: str) -> dict:
    """Best-effort merge of canonical identity + observed impact onto a /dps
    payload.

    The compiled bundle keys Atlantic storms by ATCF id and other basins by
    SID; a caller can hold either form. Try the requested id plus both alias
    forms against the bundle, then:
      - replace a missing/raw-id `name` with the bundle's (or alias table's),
      - fill a missing `year`,
      - attach the bundle's `actual_impact` (recorded damage + FEMA) so the
        compare page's Recorded Damage row works for every id form.
    Applied AFTER the DPS cache write on purpose — impact data must not be
    frozen into DPS cache entries; the bundle stays its source of truth.
    Fail-open: any error returns the payload untouched.
    """
    try:
        ident = _storm_identity(storm_id)
        entry = None
        try:
            from seo import _read_compiled_bundle
            storms = _read_compiled_bundle().get("storms", {})
            for key in (storm_id, (storm_id or "").upper(),
                        ident.get("atcf"), ident.get("sid")):
                if key and key in storms and isinstance(storms[key], dict):
                    entry = storms[key]
                    break
        except Exception:
            entry = None
        cur_name = str(payload.get("name") or "")
        if not cur_name or _ID_FORM_RE.match(cur_name):
            for cand in ((entry or {}).get("name"), ident.get("name")):
                if cand and not _ID_FORM_RE.match(str(cand)):
                    payload["name"] = cand
                    break
        if not payload.get("year"):
            payload["year"] = (entry or {}).get("year") or ident.get("year") or payload.get("year")
        if entry and entry.get("actual_impact") and not payload.get("actual_impact"):
            # copy — the entry dict belongs to seo's module-cached bundle;
            # never hand consumers a reference into that shared state
            payload["actual_impact"] = dict(entry["actual_impact"])
    except Exception as e:
        logger.debug(f"[alias] overlay skipped for {storm_id}: {e}")
    return payload


@router.get("/storms/{storm_id}/dps")
async def get_storm_dps(
    storm_id: str,
    name: Optional[str] = Query(None, description="Display name (e.g. Katrina). Used for cumulative DPI era adjustments."),
    year: Optional[int] = Query(None, description="Year for era adjustments (defaults to storm_id year if ATCF)"),
    grid_resolution_km: float = Query(15.0, ge=1.0, le=50.0),
    # skip_points=0: DPS must see the FULL synoptic track. The old default
    # of 1 (every 2nd fix) dropped Bavi 2026's actual peak (145kt/913hPa →
    # bundle said 140kt/919), doubled the RI estimator's apparent rate on
    # the resulting 12-hourly spacing, and would halve future duration
    # credit. IKE per-fix cost is absorbed by the /track cache.
    skip_points: int = Query(0, ge=0, le=10),
    force: bool = Query(False, description="Bypass the DPS cache and recompute."),
):
    """
    Return the canonical DPS bundle for a single storm.

    Cache order:
      1. Persistent-volume DPS cache (cache/dps/<id>_<version>.json)
      2. Compute: fetch snapshots via get_storm_track() → compute_storm_dps()
      3. Save to persistent volume and return

    The returned dict matches the per-storm schema of compiled_bundle.json,
    so the frontend can use it identically for presets and ad-hoc storms.
    """
    # 1) Cache hit — unless the bundle holds a deferred no-landfall dampener
    # for a track that has since gone stale (see _dps_cache_needs_rearm).
    if not force:
        cached = _load_dps_cache(storm_id)
        if cached and not _dps_cache_needs_rearm(cached):
            return JSONResponse(content=_overlay_bundle_identity(cached, storm_id))

    # 2) Fetch snapshots via the unified track endpoint (which itself caches IKE).
    # Pass explicit ints — when called internally (not as a FastAPI route) the
    # default Query() sentinels are not resolved, so comparisons fail.
    try:
        track_results = await get_storm_track(
            storm_id,
            grid_resolution_km=float(grid_resolution_km),
            skip_points=int(skip_points),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"[DPS] track fetch failed for {storm_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Failed to fetch track for {storm_id}")

    # get_storm_track may return a JSONResponse (cache hit) or a list of IKEResponse.
    if isinstance(track_results, JSONResponse):
        try:
            raw = json.loads(track_results.body)
        except Exception:
            raw = []
    else:
        raw = track_results

    engine_snaps = _ike_responses_to_engine_snapshots(raw)
    if not engine_snaps:
        raise HTTPException(status_code=404, detail=f"No snapshots available for {storm_id}")

    # Resolve name + year. For ATCF IDs (AL122005, WP092026) derive year from the ID.
    derived_year = year
    if derived_year is None and len(storm_id) == 8 and storm_id[:2].upper() in (
        "AL", "EP", "WP", "IO", "SH", "CP", "SP", "SI"
    ):
        try:
            derived_year = int(storm_id[4:])
        except ValueError:
            derived_year = 0
    # IBTrACS SIDs (YYYYDDDNxxLLL) carry the year up front. Without this, a
    # SID lookup ran ground_truth.get_by_name_year(name, 0) and missed every
    # anchor — the same storm showed rainfall "Historic" under its ATCF id
    # and "Normal" under its SID (Sinlaku 2026, data-fusion audit seam #1).
    if derived_year is None and len(storm_id) >= 10 and storm_id[:4].isdigit():
        _y = int(storm_id[:4])
        if 1840 <= _y <= datetime.now().year + 1:
            derived_year = _y
    if derived_year is None:
        derived_year = 0
    # Fall back to the catalog name, then the alias table, so bundles say
    # "Bavi", not "WP092026" — for any id form IBTrACS has ever named.
    storm_name = (name or _lookup_storm_name_from_catalog(storm_id)
                  or _storm_identity(storm_id).get("name") or storm_id)

    # 3) Compute via the unified engine (same code path as compile_cache.py)
    from core.dps_engine import compute_storm_dps
    try:
        bundle = compute_storm_dps(
            storm_id=storm_id,
            snapshots=engine_snaps,
            storm_name=storm_name,
            storm_year=int(derived_year),
        )
    except Exception as e:
        logger.exception(f"[DPS] engine failed for {storm_id}: {e}")
        raise HTTPException(status_code=500, detail=f"DPS computation failed: {e}")

    bundle["_cache_version"] = _DPS_CACHE_VERSION
    # Last observed fix (tracks are chronologically sorted) — lets the cache
    # layer detect when a deferred no-landfall dampener must re-arm.
    bundle["_last_fix_ts"] = engine_snaps[-1].get("timestamp") or None
    _save_dps_cache(storm_id, bundle)
    return JSONResponse(content=_overlay_bundle_identity(bundle, storm_id))


@router.delete("/cache/dps/{storm_id}", dependencies=[Depends(require_admin)])
async def clear_storm_dps_cache(storm_id: str):
    """Clear the cached DPS bundle for a single storm (forces recomputation)."""
    safe = _safe_storm_id(storm_id)
    cleared = 0
    for f in _DPS_CACHE_DIR.glob(f"{safe}_*.json"):
        try:
            f.unlink()
            cleared += 1
        except OSError:
            pass
    return {"cleared": cleared, "storm_id": safe}


@router.delete("/cache/dps", dependencies=[Depends(require_admin)])
async def clear_all_dps_cache():
    """Clear the entire DPS cache (forces full recomputation on next request)."""
    cleared = 0
    for f in _DPS_CACHE_DIR.glob("*.json"):
        try:
            f.unlink()
            cleared += 1
        except OSError:
            pass
    return {"cleared": cleared, "message": "All DPS cache cleared"}


# ------------------------------------------------------------------
# DPS cache pre-warming
#
# Called from main.py lifespan at startup (plus an hourly refresh loop
# for the subset that represents live tropics). On Railway the cache
# files land on the persistent volume at $PERSISTENT_DATA_DIR/cache/dps/
# so a fresh deploy rehydrates instantly without a cold fetch for every
# preset. Only storms missing a cache file at the current version are
# (re)computed — noop if everything is already warmed.
# ------------------------------------------------------------------

async def _warm_one_dps(storm_id: str, *, force: bool = False) -> str:
    """Compute + persist a single storm's DPS bundle. Returns status tag."""
    if not force:
        fp = _dps_cache_path(storm_id)
        if fp.exists():
            # Recompute anyway when the cached bundle is unreadable (e.g.
            # post-invalidation "stale" marker) or carries a deferred
            # no-landfall dampener for a now-stale track.
            cached = _load_dps_cache(storm_id)
            if cached is not None and not _dps_cache_needs_rearm(cached):
                return "cached"
    try:
        # skip_points=0 — must match get_storm_dps so the warm loop and the
        # request path compute from the same full-resolution track.
        # Bypass the track SWR branch: this loop IS the freshness driver for
        # live DPS (force=True), so it must recompute an expired track inline
        # rather than accept the stale copy SWR hands ordinary viewers —
        # otherwise every hourly tick computes DPS from last hour's track.
        _tok = _track_swr_bypass.set(True)
        try:
            track_results = await get_storm_track(storm_id, grid_resolution_km=15.0, skip_points=0)
        finally:
            _track_swr_bypass.reset(_tok)
    except Exception as e:
        logger.warning(f"[DPS WARM] track fetch failed for {storm_id}: {e}")
        return "failed"

    if isinstance(track_results, JSONResponse):
        try:
            raw = json.loads(track_results.body)
        except Exception:
            raw = []
    else:
        raw = track_results

    engine_snaps = _ike_responses_to_engine_snapshots(raw)
    if not engine_snaps:
        return "no_snapshots"

    derived_year = 0
    if len(storm_id) == 8 and storm_id[:2].upper() in (
        "AL", "EP", "WP", "IO", "SH", "CP", "SP", "SI"
    ):
        try:
            derived_year = int(storm_id[4:])
        except ValueError:
            derived_year = 0
    elif len(storm_id) >= 10 and storm_id[:4].isdigit():
        # IBTrACS SID — year is the leading four digits (see get_storm_dps).
        _y = int(storm_id[:4])
        if 1840 <= _y <= datetime.now().year + 1:
            derived_year = _y

    try:
        from core.dps_engine import compute_storm_dps
        # compute_storm_dps is pure CPU (math-only, no I/O). Running it
        # on the event-loop thread during startup warm can block /api
        # responses for several seconds per storm — push it to a worker.
        bundle = await asyncio.to_thread(
            compute_storm_dps,
            storm_id=storm_id,
            snapshots=engine_snaps,
            storm_name=_lookup_storm_name_from_catalog(storm_id) or storm_id,
            storm_year=int(derived_year),
        )
    except Exception as e:
        logger.warning(f"[DPS WARM] engine failed for {storm_id}: {e}")
        return "failed"

    bundle["_cache_version"] = _DPS_CACHE_VERSION
    bundle["_last_fix_ts"] = engine_snaps[-1].get("timestamp") or None
    _save_dps_cache(storm_id, bundle)
    return "computed"


async def _collect_active_storm_ids(app_state) -> list[str]:
    """
    Pull NHC active-storm IDs (ATCF format) without going through FastAPI DI,
    and populate the module-level _active_storms_cache so that the live-bypass
    check in get_storm_track() sees these as live. Without this, the first
    startup warm pass would serve stale IKE data for any newly-active NHC storm
    (no one has hit /storms/active yet, so the cache is still None).
    """
    global _active_storms_cache, _active_storms_cache_time
    shared_client = getattr(app_state, "http_client", None)
    try:
        async with NOAAClient(http_client=shared_client) as client:
            storms = await asyncio.wait_for(client.get_active_storms(), timeout=5.0)
    except Exception as e:
        logger.warning(f"[DPS WARM] active-storms fetch failed: {e}")
        return []
    if storms is not None:
        _active_storms_cache = storms
        _active_storms_cache_time = utcnow()
        _persist_active_storms(storms, _active_storms_cache_time)
    ids: list[str] = []
    for s in storms or []:
        sid = (s.get("id") or "").strip()
        if sid:
            ids.append(sid)
    return ids


async def warm_dps_cache(app_state=None, *, include_active: bool = True) -> dict:
    """
    Pre-warm DPS bundles for all preset storms (one-time on deploy) and any
    currently-active storms (refreshed periodically). Throttled to 3 concurrent
    computes so we don't hammer NOAA on cold start.
    """
    targets: list[str] = list(PRESET_STORM_IDS)
    if include_active and app_state is not None:
        active_ids = await _collect_active_storm_ids(app_state)
        for sid in active_ids:
            if sid not in targets:
                targets.append(sid)

    sem = asyncio.Semaphore(3)
    stats = {"cached": 0, "computed": 0, "failed": 0, "no_snapshots": 0}

    async def _one(sid: str):
        async with sem:
            tag = await _warm_one_dps(sid)
            stats[tag] = stats.get(tag, 0) + 1

    await asyncio.gather(*[_one(sid) for sid in targets], return_exceptions=True)
    logger.info(
        f"[DPS WARM] {len(targets)} storms — "
        f"cached={stats.get('cached',0)} computed={stats.get('computed',0)} "
        f"failed={stats.get('failed',0)} no_snapshots={stats.get('no_snapshots',0)}"
    )
    return stats


async def refresh_active_dps_loop(app_state, interval_seconds: int = 3600):
    """
    Periodically recompute DPS for currently-active storms so hero-card values
    stay fresh as a live system intensifies. Runs forever until the task is
    cancelled on shutdown. `force=True` because active-storm tracks change
    each advisory cycle.
    """
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            active_ids = await _collect_active_storm_ids(app_state)
            if active_ids:
                sem = asyncio.Semaphore(2)

                async def _one(sid: str):
                    async with sem:
                        await _warm_one_dps(sid, force=True)

                await asyncio.gather(*[_one(sid) for sid in active_ids], return_exceptions=True)
                logger.info(f"[DPS WARM] hourly active refresh: {len(active_ids)} storms")
            # Heartbeat even with zero active storms — an empty result is a healthy
            # iteration, not a dead loop (read by /health/selfcheck).
            _h = getattr(app_state, "health", None)
            if isinstance(_h, dict):
                _h["active_dps"] = {"last_ok": time.time(), "detail": f"{len(active_ids)} active"}
        except asyncio.CancelledError:
            logger.info("[DPS WARM] refresh loop cancelled")
            raise
        except Exception as e:
            _h = getattr(app_state, "health", None)
            if isinstance(_h, dict):
                _e = _h.setdefault("active_dps", {})
                _e["last_error"] = str(e)[:200]
                _e["last_error_at"] = time.time()
            logger.warning(f"[DPS WARM] refresh loop error (will retry): {e}")


# ------------------------------------------------------------------
# Active Storm context — aggregates live government APIs into one
# payload for the Active Storm view. Combines:
#   * NHC GIS (authoritative track, cone, wind radii)
#   * NWS Alerts (hurricane/surge/flood warnings)
#   * NOMADS GFS (forecast intensity trend)
#   * USGS STN (post-event HWMs, if the storm has already struck)
# ------------------------------------------------------------------

@router.get("/storms/active-context")
async def get_active_storm_context(
    storm_name: Optional[str] = Query(None, description="Storm name for STN lookup"),
    storm_year: Optional[int] = Query(None, description="Storm year for STN disambiguation"),
    radius_deg: float = Query(2.0, description="Alert bounding-box radius in degrees"),
):
    """
    Aggregate live authoritative data for active/just-past tropical cyclones.

    Pulls (in parallel where possible):
      * Active NHC advisories — current position, forecast track, cone,
        wind radii for every live tropical system.
      * NWS alerts — Hurricane/Storm Surge/Flash Flood warnings filtered
        to each storm's track envelope.
      * GFS point forecast — intensity trend at each storm's current
        position out to ~120h.
      * USGS STN high water marks — for a named storm, the post-event
        peak flood observations (if the STN event exists).

    This endpoint is safe to call during the off-season; it returns an
    empty ``storms`` list when no systems are active.
    """
    import asyncio as _asyncio
    from services.nhc_gis import NHCGISClient
    from services.nws_alerts import NWSAlertsClient
    from services.nomads_models import NOMADSClient
    from services.usgs_stn import USGSSTNClient

    nhc = NHCGISClient()
    alerts_client = NWSAlertsClient()
    nomads = NOMADSClient()

    # Step 1: active NHC advisories
    try:
        active_storms = await nhc.get_active_storms()
    except Exception as e:
        logger.warning(f"[active-context] NHC fetch failed: {e}")
        active_storms = []

    # Step 2: pull the alert feed once for the whole country
    try:
        all_alerts = await alerts_client.fetch_active()
    except Exception as e:
        logger.warning(f"[active-context] NWS alerts fetch failed: {e}")
        all_alerts = []

    payload_storms: list[dict] = []

    async def _enrich(storm):
        # Per-storm enrichment — GFS forecast trend + filtered alerts
        track_points: list[dict] = []
        if storm.position:
            track_points.append({"lat": storm.position[0], "lon": storm.position[1]})
        for lat, lon in storm.track:
            track_points.append({"lat": lat, "lon": lon})

        forecast_task = (
            nomads.forecast_intensity_trend(storm.position[0], storm.position[1])
            if storm.position
            else _asyncio.sleep(0, result=[])
        )
        forecast_points = await forecast_task

        # Filter alerts by bounding box around this storm's track
        if track_points:
            lats = [p["lat"] for p in track_points]
            lons = [p["lon"] for p in track_points]
            lat_min, lat_max = min(lats) - radius_deg, max(lats) + radius_deg
            lon_min, lon_max = min(lons) - radius_deg, max(lons) + radius_deg

            def _alert_in_range(a):
                if not a.geometry:
                    return True
                coords = a.geometry.get("coordinates")
                if not coords:
                    return True
                stack = [coords]
                while stack:
                    c = stack.pop()
                    if isinstance(c, (list, tuple)):
                        if len(c) == 2 and all(isinstance(v, (int, float)) for v in c):
                            if lat_min <= c[1] <= lat_max and lon_min <= c[0] <= lon_max:
                                return True
                        else:
                            stack.extend(c)
                return False

            storm_alerts = [a for a in all_alerts if _alert_in_range(a)]
        else:
            storm_alerts = all_alerts

        return {
            "storm_id": storm.storm_id,
            "name": storm.name,
            "advisory_number": storm.advisory_number,
            "category": storm.category,
            "position": {"lat": storm.position[0], "lon": storm.position[1]} if storm.position else None,
            "intensity_kt": storm.intensity_kt,
            "min_pressure_mb": storm.min_pressure_mb,
            "motion": storm.motion,
            "advisory_time_utc": storm.advisory_time_utc,
            "forecast_track": [{"lat": lat, "lon": lon} for lat, lon in storm.track],
            "cone_geometry": storm.cone,
            "wind_radii": storm.wind_radii,
            "forecast_trend": [
                {
                    "lead_hours": p.lead_hours,
                    "wind_kt": p.wind_kt,
                    "pressure_mb": p.pressure_mb,
                    "valid_time_utc": p.valid_time_utc,
                }
                for p in forecast_points
            ],
            "alerts": [
                {
                    "id": a.id,
                    "event": a.event,
                    "severity": a.severity,
                    "urgency": a.urgency,
                    "headline": a.headline,
                    "areas": a.areas,
                    "expires": a.expires,
                }
                for a in storm_alerts[:50]  # cap for payload size
            ],
            "alert_count": len(storm_alerts),
        }

    try:
        payload_storms = await _asyncio.gather(*(_enrich(s) for s in active_storms))
    except Exception as e:
        logger.warning(f"[active-context] enrichment failed: {e}")
        payload_storms = []

    # Step 4: optional USGS STN post-event HWMs if caller specified a name
    historical_hwms: list[dict] = []
    if storm_name:
        try:
            stn = USGSSTNClient()
            top = await stn.top_hwms_for_storm(storm_name, year=storm_year, n=10)
            historical_hwms = [
                {
                    "hwm_id": h.hwm_id,
                    "elev_ft": h.elev_ft,
                    "lat": h.latitude,
                    "lon": h.longitude,
                    "location": h.location_description,
                    "state": h.state,
                    "county": h.county,
                    "quality": h.hwm_quality,
                    "environment": h.hwm_environment,
                }
                for h in top
            ]
        except Exception as e:
            logger.warning(f"[active-context] STN lookup failed: {e}")

    return {
        "active_count": len(payload_storms),
        "storms": payload_storms,
        "total_alerts": len(all_alerts),
        "historical_hwms": historical_hwms,
        "query": {"storm_name": storm_name, "storm_year": storm_year},
    }


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


@router.delete("/cache/ike/{storm_id}", dependencies=[Depends(require_admin)])
async def clear_storm_cache(storm_id: str):
    """Clear cached IKE results for a specific storm."""
    safe = _safe_storm_id(storm_id)
    cleared = 0
    for f in _IKE_CACHE_DIR.glob(f"{safe}_*.json"):
        f.unlink()
        cleared += 1
    return {"cleared": cleared, "storm_id": safe}


@router.delete("/cache/ike", dependencies=[Depends(require_admin)])
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
    now = utcnow()

    # Return cached if fresh
    if (_preload_cache is not None and _preload_cache_time
            and (now - _preload_cache_time) < _PRELOAD_CACHE_TTL):
        return JSONResponse(content=_preload_cache)

    # Build in background thread to avoid blocking event loop
    async with _preload_lock:
        # Double-check after lock to prevent dogpile
        if (_preload_cache is not None and _preload_cache_time
                and (utcnow() - _preload_cache_time) < _PRELOAD_CACHE_TTL):
            return JSONResponse(content=_preload_cache)

        # Move ALL file I/O to a thread pool
        loop = asyncio.get_event_loop()
        bundle = await loop.run_in_executor(_IKE_EXECUTOR, _build_preload_bundle_sync)

        _preload_cache = bundle
        _preload_cache_time = utcnow()
        return JSONResponse(content=bundle)


@router.post("/preload/generate", dependencies=[Depends(require_admin)])
async def generate_preload_bundle(
    grid_resolution_km: float = Query(15.0, ge=1.0, le=50.0),
    skip_points: int = Query(0, ge=0, le=10),  # 0 = full track; matches /track, /dps, startup warm
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

def _write_ibtracs_index(catalog: list[dict]) -> None:
    """Write a compact metadata-only index derived from the full catalog.

    The index keeps just the fields needed for search / list UIs so a
    cold restart can populate the in-memory cache in milliseconds instead
    of re-parsing the full catalog file (which contains a superset of
    fields we don't always need).
    """
    try:
        index = []
        for s in catalog or []:
            if not s:
                continue
            index.append({
                "id":              s.get("id", ""),
                "name":            s.get("name", ""),
                "year":            s.get("year", 0),
                "basin":           s.get("basin", ""),
                "peak_wind_kt":    s.get("peak_wind_kt", 0),
                "min_pressure_hpa": s.get("min_pressure_hpa", 0),
                "category":        s.get("category", ""),
            })
        _atomic_write_json(_IBTRACS_INDEX_FILE, index)
        logger.info(f"[IBTRACS] Wrote metadata index ({len(index)} storms)")
    except Exception as e:
        logger.debug(f"[IBTRACS] Index write failed: {e}")


def _load_ibtracs_index_if_present() -> list[dict] | None:
    """Return the cached metadata index if it exists on disk, else None."""
    try:
        if _IBTRACS_INDEX_FILE.exists():
            with open(_IBTRACS_INDEX_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.debug(f"[IBTRACS] Index read failed: {e}")
    return None


# Pre-baked default-view response file. The frontend's three catalog fetches
# all use min_year=2015 (with max_year defaulted to 2099). Writing the
# filtered response to disk once per refresh lets the endpoint stream the
# bytes via FileResponse instead of filtering + JSON-serializing on every
# call. FileResponse delegates I/O to a thread, so the hot path is immune
# to event-loop contention from concurrent warm tasks during cold-start.
# _v2: harmonized rows (engine-score overlay + canonical labels + dedup —
# cross-surface score audit 2026-07-10). New filename so a volume file
# written by pre-harmonize code is orphaned and regenerated at boot warm
# instead of being served stale.
_CATALOG_DEFAULT_VIEW_FILE = _GLOBAL_IBTRACS_CACHE_FILE.parent / "catalog_default_view_v2.json"
_CATALOG_DEFAULT_MIN_YEAR = 2015
_CATALOG_DEFAULT_MAX_YEAR = 2099


def _write_catalog_default_view(catalog: list[dict]) -> None:
    """Persist the default-filter (min_year=2015) response to disk.

    Called after every full-catalog refresh; the file is what /storms/catalog
    and /storms/catalog/global serve when the caller asks for the default
    year range. Falls back gracefully if the write fails (the endpoints
    still have an in-memory path).
    """
    try:
        filtered = _harmonized([
            s for s in (catalog or [])
            if _CATALOG_DEFAULT_MIN_YEAR <= s.get("year", 0) <= _CATALOG_DEFAULT_MAX_YEAR
        ])
        _atomic_write_json(_CATALOG_DEFAULT_VIEW_FILE, filtered)
        logger.info(
            f"[IBTRACS] Wrote default-view response cache "
            f"({len(filtered)} storms, {_CATALOG_DEFAULT_MIN_YEAR}+)"
        )
    except Exception:
        logger.exception("[IBTRACS] Could not write default-view cache")


async def _build_global_catalog() -> list[dict]:
    """Build or load a cached global storm catalog (IBTrACS + custom storms).

    Cache strategy (fail-open, never serve an empty list if we have anything):
      1. Fresh in-memory cache → return immediately.
      2. Fresh disk cache (< TTL) → load, install in-memory, return.
      3. Stale disk cache (> TTL) → load, install in-memory, AND kick off a
         background NOAA refresh. Keeps every request fast; prevents a
         single slow NOAA fetch from stalling the first visitor.
      4. No disk cache → must fetch synchronously (only on truly cold install).
      5. NOAA fetch fails → keep any stale data we have rather than drop it.
    """

    global _GLOBAL_IBTRACS_CATALOG_CACHE, _GLOBAL_IBTRACS_CATALOG_TIMESTAMP

    now = utcnow()

    # ── 1. In-memory cache (fast) ────────────────────────────────────────
    if (
        _GLOBAL_IBTRACS_CATALOG_CACHE
        and _GLOBAL_IBTRACS_CATALOG_TIMESTAMP
        and (now - _GLOBAL_IBTRACS_CATALOG_TIMESTAMP) < _GLOBAL_IBTRACS_CATALOG_TTL
    ):
        logger.debug("[IBTRACS] In-memory catalog hit")
        return _GLOBAL_IBTRACS_CATALOG_CACHE

    # ── 2 / 3. Disk cache ────────────────────────────────────────────────
    disk_catalog: list[dict] | None = None
    disk_mtime: datetime | None = None
    try:
        if _GLOBAL_IBTRACS_CACHE_FILE.exists():
            disk_mtime = datetime.fromtimestamp(_GLOBAL_IBTRACS_CACHE_FILE.stat().st_mtime)
            with open(_GLOBAL_IBTRACS_CACHE_FILE, "r", encoding="utf-8") as f:
                disk_catalog = json.load(f)
    except Exception as e:
        logger.debug(f"[IBTRACS] Disk catalog read failed: {e}")
        disk_catalog = None

    if disk_catalog and disk_mtime:
        age = now - disk_mtime
        _GLOBAL_IBTRACS_CATALOG_CACHE = disk_catalog
        _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = now  # treat just-loaded as fresh in memory
        if age < _GLOBAL_IBTRACS_CATALOG_TTL:
            logger.info(f"[IBTRACS] Loaded catalog from disk ({len(disk_catalog)} storms, age={age})")
            return disk_catalog
        # Stale but usable — serve it and refresh in background.
        logger.info(
            f"[IBTRACS] Serving stale catalog ({len(disk_catalog)} storms, "
            f"age={age}); scheduling background refresh"
        )
        try:
            asyncio.create_task(_refresh_global_catalog_async())
        except RuntimeError:
            pass  # no running loop (e.g. sync test context)
        return disk_catalog

    # ── 4. No disk cache: must fetch synchronously ───────────────────────
    return await _refresh_global_catalog_async()


async def _refresh_global_catalog_async() -> list[dict]:
    """Fetch catalog from NOAA under lock, merge custom storms, persist.

    Always returns the newest catalog we can produce; if NOAA is unreachable
    we fall back to whatever is already on disk rather than returning an
    empty list.
    """
    global _GLOBAL_IBTRACS_CATALOG_CACHE, _GLOBAL_IBTRACS_CATALOG_TIMESTAMP

    # FIX 4: Protect catalog building with lock — only one request fetches from IBTrACS
    async with _catalog_lock:
        # Double-check cache after acquiring lock (another request may have built it)
        now = utcnow()
        if (
            _GLOBAL_IBTRACS_CATALOG_CACHE
            and _GLOBAL_IBTRACS_CATALOG_TIMESTAMP
            and (now - _GLOBAL_IBTRACS_CATALOG_TIMESTAMP) < _GLOBAL_IBTRACS_CATALOG_TTL
        ):
            logger.debug("[IBTRACS] Catalog freshened while waiting for lock")
            return _GLOBAL_IBTRACS_CATALOG_CACHE

        # Fetch from IBTrACS (may be slow on first run) but time out if it takes too long.
        # Timeout covers the full 8.8MB download from NCEI; Railway egress can be
        # slow so we allow 90s rather than 45s before giving up.
        noaa_catalog: list[dict] = []
        ibtracs_ok = False
        cache_dir = _PERSISTENT_DATA / "cache"
        async with NOAAClient(timeout=120.0, cache_dir=str(cache_dir)) as client:
            try:
                noaa_catalog = await asyncio.wait_for(
                    client.get_ibtracs_catalog(1851, 2099), timeout=90.0
                )
                ibtracs_ok = True
                logger.info(f"[IBTRACS] Fetched catalog: {len(noaa_catalog)} storms")
            except asyncio.TimeoutError:
                logger.error("[IBTRACS] Catalog fetch timed out after 90s")
            except Exception:
                # exception() logs the full traceback — the prior warning() call
                # swallowed the stack and hid root-cause info (TLS errors, DNS,
                # upstream 5xx, etc.).
                logger.exception("[IBTRACS] Catalog fetch failed")

        catalog = list(noaa_catalog)

        # Merge auto-ingested current-season NHC storms (AL/EP) BEFORE the manual
        # custom storms, so fresh NHC data wins on id collisions and the manual
        # custom_storms.csv rows act only as a fallback (e.g. if the ingest loop
        # hasn't run yet, or NHC was unreachable). IBTrACS still wins over both —
        # this only appends ids not already present.
        try:
            season = _load_current_season_storms()
            if season:
                logger.info(f"[IBTRACS] Merging {len(season)} current-season NHC storms")
                existing_ids = {s.get("id", "") for s in catalog if s}
                for storm in season:
                    sid = storm.get("id", "")
                    if sid and sid not in existing_ids:
                        catalog.append(storm)
                        existing_ids.add(sid)
        except Exception:
            logger.exception("[IBTRACS] Current-season merge failed")

        # Merge custom storms (future years, etc.)
        try:
            custom = _load_custom_storms(1851, 2099)
            logger.info(f"[IBTRACS] Loaded {len(custom)} custom storms")
            existing_ids = {s.get("id", "") for s in catalog if s}
            for storm in custom:
                sid = storm.get("id", "")
                if sid and sid not in existing_ids:
                    catalog.append(storm)
                    existing_ids.add(sid)
        except Exception:
            logger.exception("[IBTRACS] Custom storms load failed")

        # ── Fail-open: if NOAA upstream actually failed, never replace a good
        # cached catalog with a custom-only one. Previously, a NOAA failure
        # followed by a successful custom-storm load produced a non-empty
        # [custom_only] list, which bypassed this guard and poisoned the
        # cache for the full 6h TTL — that's how production ended up serving
        # ~10 storms instead of ~600 after a single transient NCEI blip.
        if not ibtracs_ok and _GLOBAL_IBTRACS_CATALOG_CACHE:
            logger.warning(
                "[IBTRACS] NOAA fetch failed; keeping existing "
                f"{len(_GLOBAL_IBTRACS_CATALOG_CACHE)}-storm cache"
            )
            return _GLOBAL_IBTRACS_CATALOG_CACHE

        # Still cache a fallback when NOAA failed AND we had nothing prior
        # (e.g. fresh install): serves custom storms until NOAA recovers,
        # but marks the timestamp as stale so the next request triggers a
        # retry instead of waiting the full TTL.
        _GLOBAL_IBTRACS_CATALOG_CACHE = catalog
        if ibtracs_ok:
            _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = utcnow()
        else:
            # Force the next call to re-fetch by leaving the timestamp stale.
            _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = utcnow() - _GLOBAL_IBTRACS_CATALOG_TTL

        # Only persist a real NOAA catalog to disk — never a custom-only one,
        # or a cold restart would read the stub back and treat it as authoritative.
        if ibtracs_ok:
            try:
                cache_dir.mkdir(parents=True, exist_ok=True)
                _atomic_write_json(_GLOBAL_IBTRACS_CACHE_FILE, catalog)
                _write_ibtracs_index(catalog)
                _write_catalog_default_view(catalog)
                logger.info(
                    f"[IBTRACS] Persisted {len(catalog)}-storm catalog to "
                    f"{_GLOBAL_IBTRACS_CACHE_FILE}"
                )
            except Exception:
                logger.exception("[IBTRACS] Could not write catalog cache")

        return catalog


def _republish_catalog_with_current_season() -> dict:
    """Splice the latest current-season file into the live in-memory catalog and
    regenerate the pre-baked default-view + metadata-index files that the
    frontend's catalog endpoints serve off disk — WITHOUT a full IBTrACS
    refetch — so newly-ingested storms appear within the ingest interval rather
    than waiting the 6h catalog TTL. Fail-open; returns a small summary.

    Precedence matches _refresh_global_catalog_async (IBTrACS > current-season >
    manual custom): drop any prior 'nhc-current' rows and any custom rows a fresh
    current-season entry supersedes, then re-add the fresh set.
    """
    global _GLOBAL_IBTRACS_CATALOG_CACHE
    try:
        base_catalog = _GLOBAL_IBTRACS_CATALOG_CACHE
        # Sanity guard: never republish off a tiny/degraded catalog (e.g. the
        # custom-only fail-open stub) — that would shrink the served default view
        # and hide real storms. The healthy catalog is ~1000 storms.
        if not base_catalog or len(base_catalog) < 50:
            return {"added": 0, "reason": "no_healthy_base_catalog"}

        season = _load_current_season_storms()
        season_ids = {s.get("id", "") for s in season if s.get("id")}

        rebuilt = [
            s for s in base_catalog
            if s
            and s.get("source") != "nhc-current"
            and not (s.get("source") == "custom" and s.get("id") in season_ids)
        ]
        existing_ids = {s.get("id", "") for s in rebuilt}
        added = 0
        for storm in season:
            sid = storm.get("id", "")
            if sid and sid not in existing_ids:
                rebuilt.append(storm)
                existing_ids.add(sid)
                added += 1

        _GLOBAL_IBTRACS_CATALOG_CACHE = rebuilt
        # Regenerate the disk artifacts the catalog endpoints stream from.
        _write_catalog_default_view(rebuilt)
        _write_ibtracs_index(rebuilt)
        # Persist the merged catalog so the storms survive a restart (base already
        # held real IBTrACS data, so this is never a custom-only stub).
        try:
            _atomic_write_json(_GLOBAL_IBTRACS_CACHE_FILE, rebuilt)
        except Exception:
            logger.debug("[SEASON] catalog persist after republish failed", exc_info=True)
        return {"catalog_total": len(rebuilt), "current_season_added": added}
    except Exception:
        logger.exception("[SEASON] republish failed")
        return {"added": 0, "error": True}


async def refresh_current_season(http_client=None) -> dict:
    """Fetch the current NHC season, persist it to the volume, and republish the
    catalog so the new storms are immediately browsable / searchable / named.
    Fully fail-open — never raises, never degrades the existing catalog.
    """
    try:
        from services.current_season_ingest import fetch_current_season_storms
        entries = await fetch_current_season_storms(http_client)
        # Only overwrite the season file when we actually got storms — an empty
        # result (NHC unreachable, or a genuinely quiet pre-season) must not wipe
        # a previously-good file.
        if entries:
            try:
                _atomic_write_json(_CURRENT_SEASON_FILE, entries)
            except Exception:
                logger.exception("[SEASON] could not write current-season file")
        # Republish under the catalog lock so the in-memory swap + disk-artifact
        # rewrites can't interleave with a concurrent 6h IBTrACS refresh.
        async with _catalog_lock:
            summary = _republish_catalog_with_current_season()
        # Best-effort: enrich live storms with observed IMERG rainfall (OFF by
        # default; never blocks or degrades the catalog refresh).
        try:
            rain = await refresh_current_season_rainfall(http_client)
            summary = {**summary, **rain}
        except Exception:
            logger.exception("[SEASON] rainfall enrichment step failed")
        return {"fetched": len(entries), **summary}
    except Exception:
        logger.exception("[SEASON] refresh_current_season failed")
        return {"fetched": 0, "error": True}


async def refresh_current_season_rainfall(http_client=None) -> dict:
    """Enrich current-season storms with observed GPM IMERG Late-Run rainfall and
    record it in the ground-truth registry, so the DPS rainfall override fires
    for live storms instead of the kinematic estimate.

    OFF by default — requires ``IMERG_LIVE_INGEST=1`` AND the fetch deps
    (earthaccess/xarray) AND Earthdata creds in the environment. Anything missing
    makes this a clean no-op. Never raises; per-storm failures are skipped.
    """
    # Accept any common truthy form (tolerate quotes/whitespace/case that sneak
    # into env-var values), and echo what we actually saw so a still-"disabled"
    # result is diagnosable instead of opaque.
    _raw_flag = os.getenv("IMERG_LIVE_INGEST")
    _flag = (_raw_flag or "").strip().strip('"').strip("'").lower()
    if _flag not in ("1", "true", "yes", "on"):
        return {"rainfall_recorded": 0, "rainfall_status": "disabled",
                "imerg_flag_seen": _raw_flag}
    try:
        from services.imerg_rainfall import (
            observed_rainfall_for_track, imerg_available, LATE_SHORT_NAME,
        )
        from services.atcf_bdeck_client import ATCFBDeckClient
        from core import ground_truth
    except Exception:
        logger.exception("[SEASON] rainfall imports failed")
        return {"rainfall_recorded": 0, "rainfall_status": "import_error"}
    if not imerg_available():
        return {"rainfall_recorded": 0, "rainfall_status": "deps_missing"}

    storms = _load_current_season_storms()
    if not storms:
        return {"rainfall_recorded": 0, "rainfall_status": "no_storms"}

    own_client = http_client is None
    http = http_client or httpx.AsyncClient(
        headers={"User-Agent": "StormDPS/1.0 (research)"},
        follow_redirects=True, timeout=30.0,
    )
    recorded = 0
    try:
        async with ATCFBDeckClient(http_client=http) as bdeck:
            for s in storms:
                sid = s.get("id")
                if not sid:
                    continue
                try:
                    snaps = await bdeck.get_storm_track(sid)
                    track = [
                        {"time": sn.timestamp, "lat": sn.lat, "lon": sn.lon}
                        for sn in (snaps or [])
                        if sn.lat is not None and sn.lon is not None
                    ]
                    if len(track) < 2:
                        continue
                    # IMERG fetch is blocking (network + xarray) — run off-loop.
                    res = await asyncio.to_thread(
                        observed_rainfall_for_track, track, short_name=LATE_SHORT_NAME
                    )
                    if res and res.get("peak_cell_in") and ground_truth.record_observed_rainfall(
                        sid, s.get("name"), s.get("year"), res["peak_cell_in"],
                        (res["peak_cell_lat"], res["peak_cell_lon"]),
                        "NASA GPM IMERG Late daily", peak_rainfall_mm=res["peak_cell_mm"],
                    ):
                        recorded += 1
                        # Drop the stale pre-rainfall DPS bundle so the next /dps
                        # (and the marquee) recomputes with the override.
                        _invalidate_dps_cache(sid)
                except Exception:
                    logger.info("[SEASON] rainfall enrich failed for %s", sid, exc_info=True)
                    continue
    finally:
        if own_client:
            await http.aclose()
    logger.info("[SEASON] recorded IMERG rainfall for %d current-season storm(s)", recorded)
    return {"rainfall_recorded": recorded, "rainfall_status": "ok"}


async def warm_ibtracs_catalog() -> dict:
    """Startup task: ensure the IBTrACS catalog is loaded into memory.

    Cheap if the disk cache is fresh (a single JSON read). Falls through
    to the live NOAA fetch only if we have no disk data at all — in which
    case we want this to happen once at boot rather than on the first
    user request.
    """
    try:
        catalog = await _build_global_catalog()
        # Make sure the compact index exists — useful for future features
        # that only need metadata (search autocomplete, list views).
        if catalog and not _IBTRACS_INDEX_FILE.exists():
            _write_ibtracs_index(catalog)
        # Make sure the pre-baked default-view response exists. A volume
        # populated by an older build won't have this file even though the
        # full catalog is fresh; regenerate from in-memory data so the
        # fast-path FileResponse is available before the first user request.
        if catalog and not _CATALOG_DEFAULT_VIEW_FILE.exists():
            _write_catalog_default_view(catalog)
        return {"storms": len(catalog or [])}
    except Exception as e:
        logger.warning(f"[IBTRACS WARM] failed: {e}")
        return {"storms": 0, "error": str(e)}


@router.post("/admin/ingest-current-season", dependencies=[Depends(require_admin)])
async def admin_ingest_current_season():
    """Force an immediate current-season NHC ingest (AL/EP), bypassing the
    hourly background loop: fetch the btk index, derive a catalog row per named
    in-season storm, persist to the volume, and republish the catalog so the
    storms are browsable / searchable / named right away.

    Auth: requires the X-Admin-Token header to match the ADMIN_TOKEN env var.
    """
    result = await refresh_current_season()
    return {"ok": True, **result}


@router.post("/admin/warm-ibtracs", dependencies=[Depends(require_admin)])
async def admin_warm_ibtracs():
    """Force-refresh the IBTrACS catalog on demand, bypassing the 6h TTL.

    Use when the startup warm failed silently and the year-tab accordion is
    empty on prod. Returns structured diagnostics (bytes downloaded, parse
    time, on-disk path, sample storm count per year) so an operator can
    see what went wrong without tailing logs.

    Auth: requires the X-Admin-Token header to match the ADMIN_TOKEN env var.
    """
    global _GLOBAL_IBTRACS_CATALOG_CACHE, _GLOBAL_IBTRACS_CATALOG_TIMESTAMP

    # Force a refetch by invalidating the in-memory cache timestamp.
    prior_count = len(_GLOBAL_IBTRACS_CATALOG_CACHE or [])
    _GLOBAL_IBTRACS_CATALOG_TIMESTAMP = None

    t0 = utcnow()
    try:
        catalog = await _refresh_global_catalog_async()
    except Exception as e:
        logger.exception("[IBTRACS ADMIN] refresh raised")
        raise HTTPException(500, "refresh failed")
    elapsed = (utcnow() - t0).total_seconds()

    # Report disk state so the operator can confirm the file actually landed.
    cache_file = _GLOBAL_IBTRACS_CACHE_FILE
    disk_size_mb = 0.0
    disk_mtime_iso = None
    try:
        if cache_file.exists():
            disk_size_mb = round(cache_file.stat().st_size / (1024 * 1024), 2)
            disk_mtime_iso = datetime.fromtimestamp(cache_file.stat().st_mtime).isoformat()
    except OSError:
        pass

    # Year histogram for quick visual sanity check.
    by_year: dict[int, int] = {}
    for s in catalog or []:
        y = s.get("year")
        if isinstance(y, int):
            by_year[y] = by_year.get(y, 0) + 1

    return {
        "ok": True,
        "storms_returned": len(catalog or []),
        "prior_cache_size": prior_count,
        "elapsed_seconds": round(elapsed, 2),
        "disk_cache": {
            "path": str(cache_file),
            "size_mb": disk_size_mb,
            "mtime": disk_mtime_iso,
        },
        "storms_by_year": dict(sorted(by_year.items(), reverse=True)),
    }


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
    # Fast path: the SPA's default page-load fetch hits this with
    # min_year=2015 (max_year unset → 2099). Serve the pre-baked filtered
    # response straight off disk so the request never touches the in-memory
    # catalog filter or JSON serialization — both block the event loop
    # under cold-start contention. FileResponse delegates I/O to a thread.
    if (
        min_year == _CATALOG_DEFAULT_MIN_YEAR
        and max_year == _CATALOG_DEFAULT_MAX_YEAR
        and _CATALOG_DEFAULT_VIEW_FILE.exists()
    ):
        return FileResponse(
            _CATALOG_DEFAULT_VIEW_FILE,
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
        )

    catalog = await _build_global_catalog()
    if not catalog:
        return []

    # Filter by requested years
    return _harmonized([s for s in catalog if min_year <= s.get("year", 0) <= max_year])


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

# ------------------------------------------------------------------
# Track cache — persist parsed HurricaneSnapshot lists from IBTrACS to
# avoid re-parsing the IBTrACS CSV on every request for the same SID.
# Version-keyed; bump _TRACK_CACHE_VERSION when the dict schema changes.
# ------------------------------------------------------------------

_TRACK_CACHE_VERSION = "v1"
# How stale a track cache file can be before we refresh it. IBTrACS
# revises old storms rarely but active-season storms gain observations
# every 6 hours, so keep the window short and re-fetch often for recent
# seasons. Historical storms (>2 yrs old) aren't affected by the TTL
# since their bundles effectively never change.
_TRACK_CACHE_TTL = timedelta(days=7)


def _track_cache_file(sid: str) -> Path:
    """Persistent-volume filename for a cached track bundle."""
    safe = "".join(c for c in sid if c.isalnum() or c in "-_")[:64]
    return _TRACK_CACHE_DIR / f"{safe}_{_TRACK_CACHE_VERSION}.json"


def _snapshot_to_dict(snap) -> dict:
    """Serialize a HurricaneSnapshot for the track cache.

    Drops the (expensive, usually None) ``wind_field`` grid. Stores
    timestamps as ISO strings for JSON compatibility.
    """
    return {
        "storm_id":            snap.storm_id,
        "name":                snap.name,
        "timestamp":           snap.timestamp.isoformat() if snap.timestamp else None,
        "lat":                 snap.lat,
        "lon":                 snap.lon,
        "max_wind_ms":         snap.max_wind_ms,
        "min_pressure_hpa":    snap.min_pressure_hpa,
        "rmw_m":               snap.rmw_m,
        "r34_m":               snap.r34_m,
        "r50_m":               snap.r50_m,
        "r64_m":               snap.r64_m,
        "r34_quadrants_m":     snap.r34_quadrants_m,
        "r50_quadrants_m":     snap.r50_quadrants_m,
        "r64_quadrants_m":     snap.r64_quadrants_m,
        "forward_speed_ms":    snap.forward_speed_ms,
        "forward_direction_deg": snap.forward_direction_deg,
    }


def _dict_to_snapshot(d: dict):
    """Hydrate a HurricaneSnapshot dict written by _snapshot_to_dict."""
    from models.hurricane import HurricaneSnapshot as _HS
    ts_raw = d.get("timestamp")
    if isinstance(ts_raw, str) and ts_raw:
        try:
            ts = datetime.fromisoformat(ts_raw.rstrip("Z"))
        except ValueError:
            ts = utcnow()
    else:
        ts = utcnow()
    return _HS(
        storm_id=d.get("storm_id", ""),
        name=d.get("name", ""),
        timestamp=ts,
        lat=d.get("lat", 0.0),
        lon=d.get("lon", 0.0),
        max_wind_ms=d.get("max_wind_ms", 0.0),
        min_pressure_hpa=d.get("min_pressure_hpa"),
        rmw_m=d.get("rmw_m"),
        r34_m=d.get("r34_m"),
        r50_m=d.get("r50_m"),
        r64_m=d.get("r64_m"),
        r34_quadrants_m=d.get("r34_quadrants_m"),
        r50_quadrants_m=d.get("r50_quadrants_m"),
        r64_quadrants_m=d.get("r64_quadrants_m"),
        forward_speed_ms=d.get("forward_speed_ms"),
        forward_direction_deg=d.get("forward_direction_deg"),
    )


def _load_track_cache(sid: str, *, max_age: timedelta | None = _TRACK_CACHE_TTL):
    """Return cached snapshots for *sid* if present and fresh, else None.

    ``max_age=None`` disables the TTL check — useful for fail-open fallback
    when the live source is unreachable.
    """
    fp = _track_cache_file(sid)
    try:
        if not fp.exists():
            return None
        if max_age is not None:
            mtime = datetime.fromtimestamp(fp.stat().st_mtime)
            if (utcnow() - mtime) > max_age:
                return None
        with open(fp, "r", encoding="utf-8") as f:
            payload = json.load(f)
        snaps = payload.get("snapshots") or []
        if not snaps:
            return None
        return [_dict_to_snapshot(d) for d in snaps]
    except Exception as e:
        logger.debug(f"[TRACK CACHE] read failed for {sid}: {e}")
        return None


def _save_track_cache(sid: str, snapshots) -> None:
    """Persist snapshots for *sid* to the track cache directory."""
    if not snapshots:
        return
    try:
        payload = {
            "sid": sid,
            "version": _TRACK_CACHE_VERSION,
            "cached_at": utcnow().isoformat() + "Z",
            "snapshots": [_snapshot_to_dict(s) for s in snapshots],
        }
        _atomic_write_json(_track_cache_file(sid), payload)
    except Exception as e:
        logger.debug(f"[TRACK CACHE] write failed for {sid}: {e}")


async def _fetch_track_with_cache(sid: str, *, force: bool = False, http_client=None):
    """Fetch IBTrACS snapshots for *sid*, using the track cache aggressively.

    - Fresh cache hit → return instantly.
    - Stale cache but NOAA unreachable → return the stale data rather than 404.
    - No cache and NOAA fails → raise so the caller can 404 normally.
    """
    if not force:
        snaps = _load_track_cache(sid)
        if snaps:
            return snaps

    try:
        async with NOAAClient(http_client=http_client) as client:
            # Dispatch on ID format:
            #   - IBTrACS SID (starts with digit, e.g. "2005236N23285") → SID lookup
            #   - ATCF ID    (starts with letter, e.g. "AL092017")      → USA_ATCF_ID lookup
            snaps: list = []
            if sid and sid[0].isdigit():
                snaps = await client.get_ibtracs_track(sid)
            else:
                # ATCF format — search IBTrACS CSV by USA_ATCF_ID column.
                for use_recent in (True, False):
                    csv_text = await client._fetch_ibtracs(use_recent=use_recent)
                    loop = asyncio.get_event_loop()
                    snaps = await loop.run_in_executor(
                        None, _search_ibtracs_by_atcf_id, client, csv_text, sid
                    )
                    if snaps:
                        break
    except Exception as e:
        # NOAA failed — try the stale cache as a last resort.
        fallback = _load_track_cache(sid, max_age=None)
        if fallback:
            logger.warning(
                f"[TRACK CACHE] {sid}: NOAA failed ({type(e).__name__}), "
                f"serving stale cache ({len(fallback)} snapshots)"
            )
            return fallback
        raise

    if snaps:
        _save_track_cache(sid, snaps)
    return snaps


async def warm_track_cache(*, max_concurrency: int = 3) -> dict:
    """Pre-fetch and persist tracks for every preset storm at startup.

    Idempotent: skips any SID whose cache file is fresh. Failures are
    swallowed — a missing track just means the first /ibtracs/track/{sid}
    request for that storm goes to NOAA normally.
    """
    targets = list(PRESET_STORM_IDS)
    sem = asyncio.Semaphore(max_concurrency)
    stats = {"cached": 0, "fetched": 0, "failed": 0, "empty": 0}

    async def _one(sid: str):
        if _load_track_cache(sid) is not None:
            stats["cached"] += 1
            return
        async with sem:
            try:
                snaps = await _fetch_track_with_cache(sid, force=True)
                if snaps:
                    stats["fetched"] += 1
                else:
                    stats["empty"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.debug(f"[TRACK WARM] {sid}: {type(e).__name__}: {e}")

    await asyncio.gather(*[_one(sid) for sid in targets], return_exceptions=True)
    logger.info(
        f"[TRACK WARM] {len(targets)} presets — cached={stats['cached']} "
        f"fetched={stats['fetched']} empty={stats['empty']} failed={stats['failed']}"
    )
    return stats


@router.get("/ibtracs/track/{sid}", response_model=list[IKEResponse])
async def get_ibtracs_track(
    request: Request,
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
    shared_client = getattr(request.app.state, "http_client", None)

    async def _do():
        # Disk-cache the computed IKE like /storms/{id}/track does. This endpoint
        # recomputed the whole batch from scratch on every call (~25s for a long
        # track at 10km), even though IBTrACS tracks are immutable once final.
        cached = _load_ike_cache(sid, grid_resolution_km, 0)
        if cached:
            return cached
        try:
            snapshots = await _fetch_track_with_cache(sid, http_client=shared_client)
        except (NOAAClientError, Exception) as e:
            raise HTTPException(status_code=404, detail=str(e))
        if not snapshots:
            raise HTTPException(status_code=404, detail=f"No IBTrACS data for {sid}")
        t0 = time.time()
        ike_batch = await _compute_ike_batch(snapshots, grid_resolution_km * 1000, max_workers=4)
        results = [_ike_response_to_dict(_ike_to_response(ike, snap)) for ike, snap in ike_batch]
        # Don't freeze a still-evolving current-season storm (IBTrACS "last 3
        # years" updates in-season); SIDs are otherwise historical and immutable.
        last_ts = getattr(snapshots[-1], "timestamp", None)
        if last_ts is not None and getattr(last_ts, "tzinfo", None) is not None:
            last_ts = last_ts.replace(tzinfo=None)
        is_live = bool(last_ts and (utcnow() - last_ts) < timedelta(days=2))
        if not is_live:
            _save_ike_cache(sid, grid_resolution_km, 0, results,
                            source="ibtracs", compute_ms=(time.time() - t0) * 1000)
        return results

    # Collapse concurrent requests for the same SID into one fetch+compute.
    return JSONResponse(content=await _single_flight(f"ibtrack|{sid}|{grid_resolution_km}", _do))


@router.post("/ibtracs/search", response_model=list[IKEResponse])
async def search_ibtracs(
    request: Request,
    search: IBTrACSSearchInput,
    grid_resolution_km: float = Query(10.0, ge=1.0, le=50.0),
):
    """
    Search IBTrACS by storm name and optional year, compute IKE for each observation.

    If year is omitted, searches for all storms with the given name and returns
    the most recent match (useful when users only know the storm name).

    Concurrency: results are cached (12 h TTL) and concurrent identical searches
    are collapsed via single-flight, so many simultaneous users searching the same
    historical storm trigger ONE archive fetch + compute instead of N. Uses the
    shared httpx client rather than spinning up a new pool per request.
    """
    name = (search.name or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Storm name is required.")
    key = f"ibsearch|{name.upper()}|{search.year or ''}|{(search.basin or '').upper()}|{grid_resolution_km}"

    # 1) TTL cache — instant for repeat searches across users/sessions.
    hit = _ibtracs_search_cache.get(key)
    if hit and (utcnow() - hit[1]) < _IBTRACS_SEARCH_TTL:
        return JSONResponse(content=hit[0])

    # 2) Single-flight — one fetch+compute per key under concurrent load.
    async def _do():
        shared = getattr(request.app.state, "http_client", None)
        async with NOAAClient(http_client=shared) as client:
            try:
                if search.year:
                    snapshots = await client.get_ibtracs_by_name_year(name, search.year, search.basin)
                else:
                    snapshots = await client.get_ibtracs_by_name(name, search.basin)
            except (NOAAClientError, Exception) as e:
                raise HTTPException(status_code=404, detail=str(e))
        if not snapshots:
            year_str = f" ({search.year})" if search.year else ""
            raise HTTPException(status_code=404, detail=f"No IBTrACS data for {name}{year_str}")
        ike_batch = await _compute_ike_batch(snapshots, grid_resolution_km * 1000, max_workers=4)
        out = [_ike_response_to_dict(_ike_to_response(ike, snap)) for ike, snap in ike_batch]
        _ibtracs_search_cache[key] = (out, utcnow())
        return out

    return JSONResponse(content=await _single_flight(key, _do))


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
async def get_ai_vs_nhc_comparison(request: Request, storm_id: str):
    """
    Side-by-side comparison of WeatherNext AI forecast vs NHC traditional
    advisory for a given storm. Logged for post-season 2026 validation.

    Returns 404 if WeatherNext is not configured or the storm is not active.
    """
    from services.weather_data_service import WeatherDataService

    async with WeatherDataService() as svc:
        # Attempt to get storm location from NHC active list
        shared_client = getattr(request.app.state, "http_client", None)
        async with NOAAClient(http_client=shared_client) as client:
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


@router.post("/validation/outcome", dependencies=[Depends(require_admin)])
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

@router.post("/audit/radii/{storm_id}", dependencies=[Depends(require_admin)])
async def run_radii_audit(request: Request, storm_id: str):
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

    shared_client = getattr(request.app.state, "http_client", None)
    async with NOAAClient(http_client=shared_client) as client:
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
                # Current-year storms are expected misses — IBTrACS has multi-month lag
                import datetime as _dt
                _storm_yr = int(storm_id[4:8]) if len(storm_id) >= 8 else 0
                if _storm_yr < _dt.datetime.now().year:
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
