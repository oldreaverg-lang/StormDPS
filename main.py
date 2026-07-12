"""
Hurricane DPI API — main FastAPI application.

Serves the REST API consumed by:
  - React Native / Expo app (iOS, Android, Web)
  - Legacy web frontend (frontend/index.html)

Run locally:
    uvicorn main:app --reload --port 8000

API docs:
    http://localhost:8000/docs
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import httpx

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from api.routes import (
    router,
    generate_preload_bundle,
    warm_dps_cache,
    warm_current_season_dps,
    refresh_active_dps_loop,
    load_active_storms_from_disk,
    warm_ibtracs_catalog,
    warm_track_cache,
    refresh_current_season,
)
from api.weather_routes import router as weather_router
from api.satellite_routes import (
    router as satellite_router,
    evict_old_satellite_frames,
)
from api.wind_routes import (
    router as wind_router,
    evict_old_wind_frames,
)
from api.pressure_routes import (
    router as pressure_router,
    evict_old_pressure_frames,
    evict_old_metar_files,
)
from api.precip_routes import (
    router as precip_router,
    evict_old_precip_frames,
)
# surgedps_routes was removed — SurgeDPS runs as its own service now
from services.weather_data_service import WeatherDataService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle. Initializes services and pre-computes IKE data."""
    # --- STARTUP: Create shared httpx.AsyncClient ---
    # FIX 2: Tune connection pool for viral load (50K+ concurrent users)
    # - max_connections=200 (NOAA can handle this)
    # - pool=10.0 timeout provides backpressure (requests fail fast instead of queueing forever)
    # - connect=5.0 timeout for TCP handshake
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=5.0, pool=10.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
        headers={"User-Agent": "HurricaneIKE-App/1.0 (research)"},
        follow_redirects=True,
    )
    logger.info("[STARTUP] Shared httpx.AsyncClient created (max_connections=200, pool_timeout=10s)")

    # --- STARTUP: liveness heartbeat store ---
    # Background loops stamp last-success timestamps here; /health/selfcheck reads
    # them so a dead ingest/refresh loop becomes observable (and alertable) instead
    # of failing silently. The process start time doubles as an implicit first
    # heartbeat so a fresh deploy isn't flagged before the loops' first iteration.
    app.state.health = {"_started": time.time()}

    # --- STARTUP: restore persisted active-storms snapshot (synchronous) ---
    # Populates the in-memory active-storms cache from the persistent volume so
    # the first /storms/active request is served instantly (no NHC/JTWC fetch).
    # Runs synchronously before any request can hit the server.
    try:
        restored = load_active_storms_from_disk()
        logger.info(f"[STARTUP] Active-storms snapshot restored from disk: {restored}")
    except Exception as e:
        logger.warning(f"[STARTUP] Active-storms restore failed (non-fatal): {e}")

    # --- STARTUP: Initialize WeatherDataService ---
    try:
        weather_service = WeatherDataService()
        await weather_service.__aenter__()
        app.state.weather_service = weather_service
        logger.info("[STARTUP] WeatherDataService initialized")
    except Exception as e:
        logger.error(f"[STARTUP] Failed to initialize WeatherDataService: {e}")
        app.state.weather_service = None

    # --- STARTUP: warm tasks (staggered) ---
    # All four warm tasks (preload IKE, DPS, IBTrACS catalog, tracks) hit
    # the persistent volume and, on cache miss, do CPU-heavy compute or
    # NOAA fetches. Firing them all simultaneously at boot starves the
    # event loop and the first user's API requests stall behind the warm
    # work — PageSpeed measured 5-13 s response times on cold-start before
    # this staggering was added. The cheap, disk-only IBTrACS warm runs
    # first (so /storms/catalog endpoints can serve from in-memory cache
    # asap); the heavier tasks fan out over the first ~30 s. Pre-baked
    # response files (catalog_default_view.json) cover the gap before
    # IBTrACS warm completes — endpoints stream from disk regardless.

    async def warm_ibtracs():
        # Cheap path: just hydrates _GLOBAL_IBTRACS_CATALOG_CACHE from
        # disk. Goes first because everything else benefits from having
        # the catalog in memory, and the disk read is light.
        try:
            result = await warm_ibtracs_catalog()
            logger.info(f"[IBTRACS WARM] complete: {result}")
        except Exception as e:
            logger.warning(f"[IBTRACS WARM] startup warm failed (non-fatal): {e}")

    async def warm_preload():
        # IKE cache files are shipped with the Docker image, so this
        # should find everything "already cached". Delay so initial
        # /api/v1/storms/catalog requests get a clear lane.
        await asyncio.sleep(10)
        try:
            result = await generate_preload_bundle(grid_resolution_km=15.0, skip_points=0)
            logger.info(
                f"[PRELOAD] Warm-up complete: "
                f"{result['already_cached']} cached, "
                f"{result['newly_computed']} computed, "
                f"{result['failed']} failed"
            )
        except Exception as e:
            logger.warning(f"[PRELOAD] Warm-up failed (non-fatal): {e}")

    async def warm_dps():
        # Compute-heavy on cache miss; the DPS engine itself was moved to
        # asyncio.to_thread inside _warm_one_dps so it no longer blocks
        # the loop, but still stagger so disk reads don't pile up.
        await asyncio.sleep(20)
        try:
            await warm_dps_cache(app.state, include_active=True)
        except Exception as e:
            logger.warning(f"[DPS WARM] startup warm failed (non-fatal): {e}")
        # Then warm the rest of THIS season (dissipated-but-unbaked storms) so
        # the sidebar shows their canonical hero score, not a crude estimate or
        # a bare category. Depends on the IBTrACS catalog being built first
        # (warm_ibtracs above); no-ops cleanly if it isn't ready yet — the
        # hourly refresh loop is the safety net.
        try:
            await warm_current_season_dps(app.state)
        except Exception as e:
            logger.warning(f"[DPS WARM] current-season startup warm failed (non-fatal): {e}")

    async def warm_tracks():
        # Network-bound; pushes back furthest so live API traffic in the
        # first half-minute of cold-start isn't competing for the httpx
        # connection pool.
        await asyncio.sleep(30)
        try:
            result = await warm_track_cache()
            logger.info(f"[TRACK WARM] complete: {result}")
        except Exception as e:
            logger.warning(f"[TRACK WARM] startup warm failed (non-fatal): {e}")

    asyncio.create_task(warm_ibtracs())
    asyncio.create_task(warm_preload())
    asyncio.create_task(warm_dps())
    asyncio.create_task(warm_tracks())

    # --- STARTUP: hourly refresh for live tropics ---
    # Keeps active-storm DPS fresh between deploys. Loop cancels on shutdown.
    app.state.dps_refresh_task = asyncio.create_task(
        refresh_active_dps_loop(app.state, interval_seconds=3600)
    )

    # --- STARTUP: periodic overlay-cache eviction ---
    # Per-route _maybe_evict() runs only on the write path, so quiet routes
    # never reclaim disk. This loop sweeps every overlay cache (wind, precip,
    # pressure, METAR, satellite tiles) on the persistent volume on a fixed
    # cadence, regardless of traffic. Defaults to a 28-day retention window
    # with a daily sweep — both knobs are env-overridable so the timing can
    # be tuned without a code change.
    overlay_max_age_h = int(os.getenv("OVERLAY_CACHE_MAX_AGE_HOURS", str(28 * 24)))
    overlay_sweep_h   = int(os.getenv("OVERLAY_CACHE_SWEEP_HOURS",   "24"))

    async def evict_overlays_loop():
        # Stagger first sweep a bit so it doesn't pile onto cold-start work.
        await asyncio.sleep(300)
        while True:
            try:
                w = evict_old_wind_frames(max_age_hours=overlay_max_age_h)
                p = evict_old_precip_frames(max_age_hours=overlay_max_age_h)
                pr = evict_old_pressure_frames(max_age_hours=overlay_max_age_h)
                m = evict_old_metar_files(max_age_hours=overlay_max_age_h)
                s = evict_old_satellite_frames(max_age_hours=overlay_max_age_h)
                logger.info(
                    f"[OVERLAY EVICT] swept (>{overlay_max_age_h}h): "
                    f"wind={w} precip={p} pressure={pr} metar={m} satellite={s}"
                )
            except Exception as e:
                logger.warning(f"[OVERLAY EVICT] sweep failed (non-fatal): {e}")
            await asyncio.sleep(overlay_sweep_h * 3600)

    app.state.overlay_evict_task = asyncio.create_task(evict_overlays_loop())

    # --- STARTUP: current-season NHC auto-ingest (Atlantic + East Pacific) ---
    # IBTrACS lags in-season NA/EP storms by months and the active feed only
    # carries currently-active systems, so a dissipated current-season storm
    # (e.g. an early EPac storm) has no catalog entry — no name, not browsable
    # or searchable. This loop lists NHC's btk directory, derives a catalog row
    # per named in-season storm, writes them to the persistent volume, and
    # republishes the catalog. Hourly by default; env-overridable. Fail-open.
    season_ingest_h = max(1, int(os.getenv("CURRENT_SEASON_INGEST_HOURS", "1")))

    async def ingest_current_season_loop():
        # Delay past cold-start so it doesn't compete with the warm tasks.
        await asyncio.sleep(45)
        while True:
            try:
                result = await refresh_current_season(app.state.http_client)
                logger.info(f"[SEASON] current-season ingest: {result}")
                app.state.health["season_ingest"] = {"last_ok": time.time(), "detail": str(result)[:200]}
            except Exception as e:
                logger.warning(f"[SEASON] ingest loop iteration failed (non-fatal): {e}")
                _s = app.state.health.setdefault("season_ingest", {})
                _s["last_error"] = str(e)[:200]
                _s["last_error_at"] = time.time()
            await asyncio.sleep(season_ingest_h * 3600)

    app.state.season_ingest_task = asyncio.create_task(ingest_current_season_loop())

    yield
    # --- SHUTDOWN: cancel background loops ---
    for attr in ("dps_refresh_task", "overlay_evict_task", "season_ingest_task"):
        task = getattr(app.state, attr, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # --- SHUTDOWN: close shared httpx.AsyncClient ---
    try:
        await app.state.http_client.aclose()
        logger.info("[SHUTDOWN] Shared httpx.AsyncClient closed")
    except Exception as e:
        logger.error(f"[SHUTDOWN] Error closing httpx.AsyncClient: {e}")

    # --- SHUTDOWN: close WeatherDataService ---
    if hasattr(app.state, "weather_service") and app.state.weather_service:
        try:
            await app.state.weather_service.__aexit__(None, None, None)
            logger.info("[SHUTDOWN] WeatherDataService closed")
        except Exception as e:
            logger.error(f"[SHUTDOWN] Error closing WeatherDataService: {e}")


# Docs (/docs, /redoc, /openapi.json) are public-by-default in FastAPI and
# enumerate every admin/DELETE/POST route. Disable in prod and require an
# explicit opt-in env var to enable them.
_docs_enabled = os.getenv("ENABLE_API_DOCS", "false").lower() in ("1", "true", "yes")

app = FastAPI(
    title="Hurricane DPI API",
    description=(
        "Compute the Destructive Potential Index (DPI) for tropical cyclones "
        "using NOAA/NHC data, Integrated Kinetic Energy (IKE), storm surge "
        "modeling, and regional economic vulnerability analysis. "
        "Serves iOS, Android, and Web clients via REST."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
# Starlette applies middleware "last added = outermost". Order matters here:
#   - SlowAPI is added FIRST so it runs immediately before the route handler.
#   - CORS is added LAST so it wraps rate-limit 429 responses too (otherwise
#     the browser surfaces a confusing CORS error instead of the 429 body).

# Rate limiting — per-IP global default. Tunable via env vars. Honors
# Cloudflare's CF-Connecting-IP header so per-IP limits work behind the proxy.
def _client_ip(request: Request) -> str:
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return get_remote_address(request)

_default_rate = os.getenv("RATE_LIMIT_DEFAULT", "300/minute")
limiter = Limiter(
    key_func=_client_ip,
    default_limits=[_default_rate],
    headers_enabled=True,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# GZip — compress API responses for mobile bandwidth efficiency
app.add_middleware(GZipMiddleware, minimum_size=500)

# CORS — production must set ALLOWED_ORIGINS to an explicit list. Default
# is dev-localhost only (NEVER "*"). `*` plus allow_credentials is rejected
# by browsers and would still let any site call write endpoints.
_raw_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
if _raw_origins:
    allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]
else:
    allowed_origins = [
        "http://localhost:8000",
        "http://localhost:8080",
        "http://127.0.0.1:8000",
        "http://127.0.0.1:8080",
    ]
    logger.warning(
        "ALLOWED_ORIGINS env var not set — falling back to localhost-only CORS. "
        "Set ALLOWED_ORIGINS=https://yourdomain.com,... in production."
    )
# Wildcard is incompatible with allow_credentials and would defeat the
# point of the whitelist; refuse to honor it.
if "*" in allowed_origins:
    logger.warning("ALLOWED_ORIGINS contained '*' — stripping; set explicit origins.")
    allowed_origins = [o for o in allowed_origins if o != "*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Request-Id", "X-Response-Time"],
    max_age=600,  # Cache preflight for 10 min (reduces OPTIONS roundtrips on mobile)
)


# ---------------------------------------------------------------------------
# Security response headers
# ---------------------------------------------------------------------------
# Cloudflare adds HSTS at the edge for stormdps.com, but X-Frame-Options,
# X-Content-Type-Options, Referrer-Policy, and Permissions-Policy must be
# set by the origin so they reach SSR'd /storm/{id} pages too. We also
# emit a permissive but real Content-Security-Policy — restrictive enough
# to block obvious injection vectors, permissive enough not to break the
# inline scripts the SPA already ships.
@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    headers = response.headers
    # Belt-and-suspenders HSTS in case the request bypasses Cloudflare
    headers.setdefault(
        "Strict-Transport-Security",
        "max-age=31536000; includeSubDomains",
    )
    headers.setdefault("X-Content-Type-Options", "nosniff")
    headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    # Disable browser features we don't use — narrows what an exploited
    # script could do if one ever slipped through.
    headers.setdefault(
        "Permissions-Policy",
        "geolocation=(), microphone=(), camera=(), payment=(), usb=()",
    )
    # CSP: the SPA inlines a large <script> block, so 'unsafe-inline' is
    # currently required for scripts. We pin third-party origins to the
    # exact hosts already wired into preconnects + the service worker so
    # a future supply-chain attack on a random CDN can't pivot through us.
    # img-src is permissive (NASA GIBS tile servers + dynamic IBTrACS plot URLs).
    # cloudflareinsights: Cloudflare Web Analytics injects its RUM beacon at
    # the edge; the CSP blocked it on EVERY page view (console error on each
    # load + zero field data collected — found via Lighthouse Best Practices
    # 2026-07-11). script-src loads beacon.min.js; connect-src lets it POST
    # its measurements to /cdn-cgi/rum, or it fails silently one step later.
    headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com https://static.cloudflareinsights.com; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com; "
        "font-src 'self' data:; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' https://*.stormdps.com https://api.open-meteo.com https://cloudflareinsights.com; "
        "frame-ancestors 'self'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    return response


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

app.include_router(router, prefix="/api/v1")
app.include_router(weather_router, prefix="/api/v1")
app.include_router(satellite_router, prefix="/api/v1")
app.include_router(wind_router, prefix="/api/v1")
app.include_router(pressure_router, prefix="/api/v1")
app.include_router(precip_router, prefix="/api/v1")
# SurgeDPS API routes removed — SurgeDPS runs as its own service now


@app.get("/health")
async def health_check():
    """Health check endpoint used by mobile app to verify connectivity."""
    return {
        "status": "ok",
        "service": "hurricane-dpi-api",
        "version": "1.0.0",
    }


@app.get("/health/storage")
async def storage_health():
    """Return persistent volume usage breakdown for monitoring."""
    from storage import storage_summary
    return storage_summary()


# Loops are expected to heartbeat at least this often (their interval is 1h; this
# allows a couple of missed cycles + a deploy before we call them dead).
_LOOP_STALE_SECONDS = 3 * 3600

# Data feeds whose SUSTAINED failure breaks the core product (live storm tracks,
# forecast cones, the historical catalog) and should therefore PAGE. Everything
# else SourceHealthMonitor tracks (SST, GFS, AI forecasts, NWS alerts, overlays)
# is enrichment — its failure degrades but doesn't break the site, so it stays
# advisory. A source is only "unhealthy" after reliability <=0.5 or >=5 consecutive
# failures, so this can't fire on a transient blip or an untested (0-call) feed.
_CRITICAL_SOURCES = {"jtwc_bdeck", "nhc_active", "nhc_forecast", "ibtracs", "hurdat2"}


@app.get("/health/selfcheck")
async def health_selfcheck():
    """Active self-check for unattended operation. Returns 200 when healthy, 503
    otherwise, so an external scheduler (the GitHub healthcheck workflow) can poll
    it and alert on failure. Checks the core data artifact and that the background
    automation loops are still heartbeating; data-source flakiness is reported but
    advisory (transient feed blips must not page)."""
    now = time.time()
    health = getattr(app.state, "health", {}) or {}
    started = health.get("_started", now)
    checks: dict = {}
    failures: list = []

    # 1) Core data artifact: the compiled bundle must load and be populated.
    try:
        from seo import _read_compiled_bundle
        n = len(_read_compiled_bundle().get("storms", {}))
        checks["bundle"] = {"ok": n >= 100, "storms": n}
        if n < 100:
            failures.append(f"bundle has only {n} storms")
    except Exception as e:
        checks["bundle"] = {"ok": False, "error": str(e)[:200]}
        failures.append(f"bundle unreadable: {str(e)[:120]}")

    # 2) Background loops: alert if a loop hasn't succeeded within the window.
    #    The process start counts as an implicit heartbeat (deploy grace).
    for name in ("season_ingest", "active_dps"):
        st = health.get(name, {})
        last = max(st.get("last_ok", 0), started)
        age = now - last
        ok = age < _LOOP_STALE_SECONDS
        checks[name] = {
            "ok": ok,
            "age_min": round(age / 60, 1),
            "detail": st.get("detail"),
            "last_error": st.get("last_error"),
        }
        if not ok:
            failures.append(f"{name} loop stale ({age / 60:.0f} min, no success)")

    # 3) Data sources: a sustained failure on a CRITICAL feed pages; everything
    #    else stays advisory (reported but doesn't fail the check).
    try:
        from services.source_health import SourceHealthMonitor
        srcs = SourceHealthMonitor.instance().summary().get("sources", [])
        critical_down, degraded = [], []
        for s in srcs:
            if not isinstance(s, dict) or s.get("is_healthy") is not False:
                continue
            (critical_down if s.get("name") in _CRITICAL_SOURCES else degraded).append(s.get("name"))
        checks["sources"] = {
            "ok": not critical_down,
            "critical_down": critical_down,
            "degraded": degraded,
            "tracked": len(srcs),
        }
        for nm in critical_down:
            failures.append(f"critical data source '{nm}' failing (sustained)")
    except Exception as e:
        checks["sources"] = {"ok": True, "error": str(e)[:200]}

    # 4) Cross-surface score consistency (seam 3, DATA_ARCHITECTURE roadmap
    #    #3): the sidebar catalog must tell the hero card's story. After the
    #    2026-07-10 harmonize fix these agree BY CONSTRUCTION, so any
    #    mismatch here means the overlay regressed (stale default-view file,
    #    alias table missing, harmonize exception falling open) — page it
    #    instead of waiting for a user to notice. Advisory-grade errors in
    #    the probe itself never fail the check.
    try:
        from api.routes import _build_global_catalog, _harmonized
        from core.dpi import categorize_dpi
        from core.storm_identity import storm_identity
        from seo import _read_compiled_bundle
        bundle_storms = _read_compiled_bundle().get("storms", {})
        catalog = _harmonized(await _build_global_catalog())
        canon = {"Historic", "Devastating", "Extreme", "Severe",
                 "Moderate", "Low", "Minimal"}
        mismatched, off_canon, joined = [], [], 0
        for row in catalog:
            lbl = row.get("dps_label")
            if lbl is not None and lbl not in canon:
                off_canon.append(row.get("id"))
            rid = str(row.get("id") or "")
            ident = storm_identity(rid)
            entry = None
            for key in (rid, rid.upper(), ident.get("atcf"), ident.get("sid")):
                if key and key in bundle_storms:
                    entry = bundle_storms[key]
                    break
            if not entry or entry.get("dps") is None or row.get("peak_dps") is None:
                continue
            joined += 1
            hero = float(entry["dps"])
            if abs(float(row["peak_dps"]) - hero) > 1.0 or (
                    lbl and lbl != (entry.get("dps_label") or categorize_dpi(hero))):
                mismatched.append(rid)
        probe_ok = not mismatched and not off_canon
        checks["score_consistency"] = {
            "ok": probe_ok,
            "joined": joined,
            "mismatched": len(mismatched),
            "off_canon_labels": len(off_canon),
            "sample": (mismatched or off_canon)[:5],
        }
        if not probe_ok:
            failures.append(
                f"catalog/hero score drift: {len(mismatched)} mismatched, "
                f"{len(off_canon)} off-canon labels")
    except Exception as e:
        checks["score_consistency"] = {"ok": True, "error": str(e)[:200]}

    # 5) Cross-SITE score parity: SurgeDPS ships a SNAPSHOT of our canonical
    #    scores (its data/dps_scores.json + curated catalog) that must be
    #    regenerated after every bake (its scripts/build_dps_scores.py).
    #    Probe its historic catalog against the live bundle so forgetting
    #    that step pages the cron instead of a user noticing — by 2026-07-10
    #    430/446 of its keys had silently drifted. Probe/fetch errors stay
    #    advisory: SurgeDPS *availability* is its own service's concern; this
    #    check is only about the numbers disagreeing.
    try:
        from core.storm_identity import cross_site_score_drift
        from seo import _read_compiled_bundle
        surge_url = os.environ.get(
            "SURGEDPS_API_URL",
            "https://surgedps-production.up.railway.app",
        ).rstrip("/") + "/api/storms/historic"
        client = getattr(app.state, "http_client", None)
        if client is not None:
            resp = await client.get(surge_url, timeout=10.0)
        else:  # pragma: no cover — lifespan always sets the shared client
            async with httpx.AsyncClient() as _c:
                resp = await _c.get(surge_url, timeout=10.0)
        resp.raise_for_status()
        parity = cross_site_score_drift(
            resp.json(), _read_compiled_bundle().get("storms", {}))
        checks["surgedps_parity"] = {
            "ok": not parity["drifted"],
            "compared": parity["compared"],
            "drifted": len(parity["drifted"]),
            "sample": parity["drifted"][:5],
        }
        if parity["drifted"]:
            failures.append(
                f"SurgeDPS score drift: {len(parity['drifted'])}/"
                f"{parity['compared']} storms disagree with the bundle "
                f"(rerun SurgeDPS scripts/build_dps_scores.py)")
    except Exception as e:
        checks["surgedps_parity"] = {"ok": True, "advisory_error": str(e)[:200]}

    ok = not failures
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"ok": ok, "failures": failures, "checks": checks, "ts": int(now)},
    )


# ---------------------------------------------------------------------------
# Legacy web frontend (still served for backward compatibility)
# ---------------------------------------------------------------------------

FRONTEND_DIR = Path(__file__).parent / "frontend"


# ---------------------------------------------------------------------------
# Scanner-probe block list — substring patterns that any non-routed path is
# matched against. A hit returns 404 instantly without consulting the SPA
# fallback. Shared between the root 404 handler and the /surgedps SPA route
# so the two don't drift.
#
# Maintenance notes:
#   • Patterns are substring-matched against the lowercased path.
#   • Any legitimate route (defined elsewhere in this file) must NOT
#     contain any of these substrings. When adding a pattern, mentally
#     scan the route list above for collisions.
#   • Keep WordPress / PHP / IIS sections together so the next person can
#     audit at a glance.
#   • Bandwidth savings are real: each blocked probe is a 50-byte JSON
#     instead of a ~350 KB SPA shell.
# ---------------------------------------------------------------------------
SCANNER_PROBE_PATTERNS: tuple[str, ...] = (
    # ── Dotfiles / VCS / shell history ──
    ".env", ".git", ".aws", ".ssh", ".docker", ".svn", ".hg", ".bzr",
    ".DS_Store", ".npmrc", ".htaccess", ".htpasswd",
    ".bash_history", ".sh_history", ".python_history",
    ".mysql_history", ".lesshst",
    "/cvs/",  # ancient SCM
    # ── Admin / login panel probes ──
    "/admin",         # broader than the old /admin. — catches bare /admin too
    "/login", "/signin", "/signup",
    "/dashboard", "/portal", "/manager", "/manage",
    "/console",
    # ── Auth service discovery (OIDC / OAuth / SAML) ──
    "openid-config", "oauth-authorization-server",
    "/oauth/", "/oidc/", ".well-known/openid",
    ".well-known/oauth", ".well-known/jwks",
    ".well-known/host-meta", "saml/metadata",
    # ── WordPress ──
    "wp-admin", "wp-login", "wp-config", "wp-content",
    "wp-includes", "/wp-json", "/wp-trackback",
    "xmlrpc.php",
    # ── PHP / generic ──
    "phpinfo", ".php", "phpmyadmin", "/pma/", "/myadmin",
    "eval-stdin.php", "/setup.cgi", "/cgi-bin/",
    # ── Spring / Java / JMX ──
    "/actuator", "/jolokia",
    # ── Enterprise search / metrics / dashboards ──
    "/solr", "/druid", "/struts", "/elasticsearch",
    "/kibana", "/grafana", "/prometheus", "/spring",
    # ── Microsoft / SharePoint / IIS / OWA ──
    "/owa/", "/exchange/", "/autodiscover",
    "/_vti_bin", "/_layouts", "/aspnet_client",
    "/iisstart", "trace.axd",
    # ── IoT / router exploit probes (Mirai-style) ──
    "hnap1", "picsdesc.xml", "/upnp",
    "boaform", "currentsetting.htm",
    # ── Generic management / status endpoints ──
    "/server-status", "/server-info", "/metrics",
    "/jenkins", "/api-docs", "/swagger-ui", "/debug",
    # ── Sensitive paths / config dumps ──
    "/config", "/credentials", "/secret", "/vendor/",
    "/backup", "/dump", "/.well-known/security/",
    # NOTE the trailing slash on "/.well-known/security/" — without it,
    # this would match our own /.well-known/security.txt route. The route
    # itself is matched explicitly above, but defense in depth.
    # ── Backup / archive / dump files ──
    ".bak", ".backup", ".old", ".orig", ".swp",
    "db.sql", "dump.sql", "database.sql",
    # ── Dependency manifests (source repo probes) ──
    "/.idea", "/.vscode",
    "composer.json", "composer.lock",
    "package-lock.json", "yarn.lock",
    "/pom.xml", "/build.gradle", "/cargo.toml",
    "gemfile",
)


# ---------------------------------------------------------------------------
# Error handling — return JSON for all errors (mobile clients expect JSON)
# ---------------------------------------------------------------------------

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    path = request.url.path

    # ── Security: block probes for sensitive files ──
    # Patterns live in SCANNER_PROBE_PATTERNS at the top of this file so
    # the /surgedps/{path:path} handler can use the same list.
    path_lower = path.lower()
    if any(p in path_lower for p in SCANNER_PROBE_PATTERNS):
        return JSONResponse(status_code=404, content={"detail": "Not found"})

    if path.startswith("/api/"):
        return JSONResponse(
            status_code=404,
            content={"detail": "Endpoint not found"},
        )
    # For non-API paths, try to serve the legacy frontend
    # Only for paths that look like client-side routes (no file extension
    # or known frontend extensions) — NOT arbitrary file probes.
    if not "." in path.split("/")[-1] or path.endswith((".html", ".htm")):
        frontend_file = FRONTEND_DIR / "index.html"
        if frontend_file.exists():
            return FileResponse(frontend_file)
    return JSONResponse(status_code=404, content={"detail": "Not found"})


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    logger.error(f"Internal error on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# mtime-keyed cache of the SPA shell so the hint injection below never
# re-reads the ~350 KB file per request.
_INDEX_HTML_CACHE: dict = {"mtime": None, "html": None}


@app.get("/")
async def serve_frontend():
    # Cache-Control lets Cloudflare cache the SPA shell at the edge so the
    # majority of users skip the origin entirely. PageSpeed flagged the
    # document request as 1,259 ms server time — likely Railway cold-start
    # contention with the lifespan warm tasks. Edge cache hides that from
    # everyone after the first hit.
    #
    # Active-storm hint (PSI mobile 2026-07-10, perf 47): the LCP is a map
    # tile, and its critical chain serialized behind the SPA discovering the
    # live storm (/storms/active ≈560 ms) before it could start the ≈930 ms
    # /track fetch. Injecting the cached active list + a <link rel=preload>
    # for the EXACT /track URL fetchStorm will request lets the download
    # start during HTML parse. The hint may be up to a few minutes stale —
    # fine: ids change rarely, and the SPA still polls /storms/active for
    # the authoritative list. Fail-open: any error serves the raw file.
    path = FRONTEND_DIR / "index.html"
    headers = {"Cache-Control": "public, max-age=300, s-maxage=900"}
    try:
        mtime = path.stat().st_mtime
        if _INDEX_HTML_CACHE["mtime"] != mtime:
            _INDEX_HTML_CACHE.update(
                mtime=mtime, html=path.read_text(encoding="utf-8"))
        html = _INDEX_HTML_CACHE["html"]

        from storage import ACTIVE_STORMS_FILE, cache_read
        storms = (cache_read(ACTIVE_STORMS_FILE) or {}).get("storms") or []
        if storms:
            import json as _json
            # A storm WILL auto-load, so the map is coming: open the tile-CDN
            # connection and start Leaflet downloading during HTML parse.
            # These are injected (not static in index.html) because on a
            # storm-free homepage no map ever renders and they'd be dead
            # weight + PSI "unused preload" flags. Notes from measurement:
            #  - ONE carto preconnect (a-d share a cert; Chrome coalesces
            #    onto one h2 connection — extra preconnects sit unused).
            #    No crossorigin: Leaflet tiles are plain <img>.
            #  - Leaflet preload integrity/crossorigin MUST stay in lockstep
            #    with loadLeaflet's _loadScript attrs in index.html or the
            #    preload is wasted (as=script preloads ARE reused, unlike
            #    the as=fetch /track attempt: API cache headers blocked
            #    reuse and it double-downloaded 41 KB).
            # "</" escaped so feed-sourced strings can never close the tag.
            hint_json = _json.dumps(storms).replace("</", "<\\/")
            inject = (
                '<link rel="preconnect" href="https://c.basemaps.cartocdn.com">\n    '
                '<link rel="dns-prefetch" href="https://a.basemaps.cartocdn.com">\n    '
                '<link rel="dns-prefetch" href="https://b.basemaps.cartocdn.com">\n    '
                '<link rel="dns-prefetch" href="https://d.basemaps.cartocdn.com">\n    '
                '<link rel="preload" as="script" '
                'href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" '
                'integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" '
                'crossorigin="">\n    '
                f"<script>window.__ACTIVE_HINT__={hint_json};</script>\n"
            )
            html = html.replace("</head>", inject + "</head>", 1)
        return HTMLResponse(html, headers=headers)
    except Exception:
        logger.exception("[home] active-hint injection failed — serving raw shell")
        return FileResponse(path, headers=headers)


# ---------------------------------------------------------------------------
# SEO landing pages — server-rendered so crawlers get real content even
# before JS runs. Every storm gets its own canonical URL with unique title,
# description, and Article JSON-LD.
# ---------------------------------------------------------------------------

import re as _re
from seo import render_storm_page as _render_storm_page

_STORM_ID_RE = _re.compile(r"^[A-Za-z0-9_-]{1,32}$")


@app.get("/storm/{storm_id}", response_class=HTMLResponse)
async def serve_storm_page(storm_id: str):
    """SSR a per-storm landing page so Google indexes each storm separately."""
    if not _STORM_ID_RE.match(storm_id):
        raise HTTPException(status_code=404, detail="Not found")
    html_out = _render_storm_page(storm_id)
    if not html_out:
        raise HTTPException(status_code=500, detail="render failed")
    return HTMLResponse(
        content=html_out,
        headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
    )


@app.get("/og/storm/{storm_id}.png")
async def serve_storm_og_image(storm_id: str):
    """Per-storm Open Graph share card (1200x630 PNG) for social previews.
    Fail-open: any problem (bad id, unknown storm, missing Pillow/fonts, live
    storm with no baked score) falls back to the static logo so a shared link
    always renders *something*."""
    logo_fallback = RedirectResponse(url="/frontend/logo-512.png", status_code=302)
    if not _STORM_ID_RE.match(storm_id):
        return logo_fallback
    try:
        from seo import lookup_storm as _lookup_storm
        import og_card as _og_card
        storm = _lookup_storm(storm_id)
        png = _og_card.render_storm_card_png(storm_id, storm) if storm else None
    except Exception:
        png = None
    if not png:
        return logo_fallback
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400, s-maxage=86400"},
    )


@app.get("/methodology")
async def serve_methodology():
    """Long-form explanation of the DPS methodology. Static HTML, SEO-optimized."""
    fp = FRONTEND_DIR / "methodology.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/compare")
async def serve_compare():
    """Storm-vs-storm comparison page. Static HTML; the two storms come from
    ?a=&b= query params, resolved client-side against the public API. Short
    cache: the page shell is static but ships picker data expectations."""
    fp = FRONTEND_DIR / "compare.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/data")
async def serve_data_page():
    """Dataset landing page — exposes the historical storms database with
    schema.org/Dataset JSON-LD so Google Dataset Search indexes it."""
    fp = FRONTEND_DIR / "data.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/historic-storms")
async def serve_historic_storms():
    """Hub page ranking the most destructive tropical cyclones with internal
    links to each /storm/{id} page — concentrates SEO link equity."""
    fp = FRONTEND_DIR / "historic-storms.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/faq")
async def serve_faq():
    """FAQ page with schema.org/FAQPage JSON-LD for Google question-style results."""
    fp = FRONTEND_DIR / "faq.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/about")
async def serve_about():
    """About page — author bio, project mission, contact. E-E-A-T signals
    (clear authorship) are a known Google ranking factor."""
    fp = FRONTEND_DIR / "about.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/commercial")
async def serve_commercial():
    """Commercial-use / enterprise-access landing page. Demand-test for
    a future paid API tier — pricing TBD, currently a sales page with a
    mailto contact rather than a self-serve checkout flow."""
    fp = FRONTEND_DIR / "commercial.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/privacy")
async def serve_privacy():
    """Privacy policy — required for Apple App Store submission and a
    baseline trust signal. We don't collect personal data; this page
    explains exactly that to humans and to compliance reviewers."""
    fp = FRONTEND_DIR / "privacy.html"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/html",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/historical_storms_db.csv")
async def serve_historical_csv():
    """Raw CSV dataset, referenced by the Dataset JSON-LD."""
    fp = Path(__file__).parent / "historical_storms_db.csv"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="text/csv",
        headers={"Cache-Control": "public, max-age=86400"},
        filename="historical_storms_db.csv",
    )


@app.get("/historical_storms_db.json")
async def serve_historical_json():
    """Raw JSON dataset, referenced by the Dataset JSON-LD."""
    fp = Path(__file__).parent / "historical_storms_db.json"
    if not fp.exists():
        raise HTTPException(status_code=404, detail="Not found")
    return FileResponse(
        fp,
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=86400"},
        filename="historical_storms_db.json",
    )


# ---------------------------------------------------------------------------
# SurgeDPS sub-app — serves the React SPA at /surgedps (and /surgedps/*)
# Static assets under /surgedps/assets/ are handled by the StaticFiles mount
# below; everything else falls through to the SPA shell.
# ---------------------------------------------------------------------------

SURGEDPS_FRONTEND_DIR = FRONTEND_DIR / "surgedps"


@app.get("/surgedps")
async def serve_surgedps():
    return FileResponse(SURGEDPS_FRONTEND_DIR / "index.html")


@app.get("/surgedps/{path:path}")
async def serve_surgedps_spa(path: str, request: Request):
    # SurgeDPS API is now a separate service — reject any stale API calls.
    if path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")

    # ── Security: block sensitive file probes ──
    # Uses the shared SCANNER_PROBE_PATTERNS list to stay in sync with the
    # root 404 handler — previously this handler had its own tiny copy that
    # missed most patterns. Path is matched WITHOUT a leading slash here
    # because the {path:path} param strips the leading slash from the
    # request URL.
    path_lower = path.lower()
    # Prepend "/" so the patterns that anchor with "/" (e.g. "/admin",
    # "/oauth/") match the way they would against a full request path.
    if any(p in ("/" + path_lower) for p in SCANNER_PROBE_PATTERNS):
        raise HTTPException(status_code=404, detail="Not found")

    # ── Path traversal protection ──
    # Reject paths with ".." or absolute path components
    if ".." in path or path.startswith("/"):
        raise HTTPException(status_code=404, detail="Not found")

    # Static assets (js, css, svg, png …) — serve directly if present
    asset_file = SURGEDPS_FRONTEND_DIR / path
    # Verify resolved path stays within the frontend directory
    try:
        asset_file.resolve().relative_to(SURGEDPS_FRONTEND_DIR.resolve())
    except ValueError:
        raise HTTPException(status_code=404, detail="Not found")

    if asset_file.is_file():
        response = FileResponse(asset_file)
        # Vite hashed assets are immutable — cache aggressively.
        # Root-level static images (logo, favicon, social cards) are
        # content-stable too: a real change ships under a different
        # filename. Same year-long Cache-Control applies. Lighthouse
        # mobile previously flagged logo-180.png as "None" TTL,
        # accounting for 29 KiB of repeat-visit bandwidth.
        is_hashed_asset = path.startswith("assets/")
        is_root_static_image = (
            "/" not in path
            and path.lower().endswith((
                ".png", ".webp", ".jpg", ".jpeg", ".svg",
                ".ico", ".gif", ".avif",
            ))
        )
        if is_hashed_asset or is_root_static_image:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response
    # Client-side routing fallback → SPA shell
    return FileResponse(SURGEDPS_FRONTEND_DIR / "index.html")


@app.get("/robots.txt")
async def serve_robots():
    return FileResponse(FRONTEND_DIR / "robots.txt", media_type="text/plain")


@app.get("/sitemap.xml")
async def serve_sitemap():
    return FileResponse(FRONTEND_DIR / "sitemap.xml", media_type="application/xml")


@app.get("/BingSiteAuth.xml")
async def serve_bing_site_auth():
    """Site-ownership verification for Bing Webmaster Tools."""
    return FileResponse(
        FRONTEND_DIR / "BingSiteAuth.xml",
        media_type="application/xml",
    )


@app.get("/.well-known/security.txt")
async def serve_security_txt():
    """RFC 9116 — vulnerability disclosure contact for stormdps.com.

    Serves the static file from frontend/.well-known/security.txt. Returning
    text/plain (the RFC-mandated content type) and a short cache so updates
    to the contact / Expires field propagate within a day instead of being
    cached at the edge for the immutable-asset year.
    """
    return FileResponse(
        FRONTEND_DIR / ".well-known" / "security.txt",
        media_type="text/plain",
        headers={"Cache-Control": "public, max-age=86400"},
    )


# ---------------------------------------------------------------------------
# Compiled bundle — served from the persistent volume with fallback.
#
# Redeploys ship with the frontend/compiled_bundle.json baked into the image,
# but compile_cache.py can also write a newer copy to the Railway persistent
# volume. When present, the volume copy takes precedence so the hero card,
# accordion, and map all see the freshest scores without waiting for another
# image build. The frontend fetches /frontend/compiled_bundle.json; this
# explicit route wins over the StaticFiles mount below.
# ---------------------------------------------------------------------------

from storage import COMPILED_BUNDLE_FILE as _VOLUME_COMPILED_BUNDLE

@app.get("/frontend/compiled_bundle.json")
async def serve_compiled_bundle(request: Request):
    # A VERSIONED request (?v=N, sent by the SPA and bumped on every bake) is safe
    # to cache immutably — a new bundle always arrives under a new ?v=, so a cached
    # copy can never go stale. This lets the browser (and Cloudflare, once a cache
    # rule is added for this path) serve the ~287 KB bundle from cache instead of
    # hammering the origin on every first-time visit during a traffic spike.
    # Unversioned/direct hits keep the short, safe TTL.
    if request.query_params.get("v"):
        cache = "public, max-age=31536000, immutable"
    else:
        cache = "public, max-age=300, s-maxage=900"
    if _VOLUME_COMPILED_BUNDLE.exists():
        return FileResponse(
            _VOLUME_COMPILED_BUNDLE,
            media_type="application/json",
            headers={"Cache-Control": cache},
        )
    baked = FRONTEND_DIR / "compiled_bundle.json"
    if baked.exists():
        return FileResponse(
            baked,
            media_type="application/json",
            headers={"Cache-Control": cache},
        )
    raise HTTPException(status_code=404, detail="compiled_bundle.json not found")


# Static-asset cache headers. Without these the /frontend mount sends no
# Cache-Control, so logos/scripts get re-downloaded on every visit and
# Cloudflare won't cache them at the edge. PageSpeed flagged this as
# "Use efficient cache lifetimes". Image extensions get a long immutable
# cache (the URLs include the asset name which only changes on real edits);
# JS/CSS get shorter so script tweaks ship within a day.
_STATIC_CACHE_BY_EXT = {
    ".png": "public, max-age=2592000, immutable",   # 30 days
    ".jpg": "public, max-age=2592000, immutable",
    ".jpeg": "public, max-age=2592000, immutable",
    ".webp": "public, max-age=2592000, immutable",
    ".gif": "public, max-age=2592000, immutable",
    ".svg": "public, max-age=2592000, immutable",
    ".ico": "public, max-age=2592000, immutable",
    ".woff": "public, max-age=2592000, immutable",
    ".woff2": "public, max-age=2592000, immutable",
    ".ttf": "public, max-age=2592000, immutable",
    ".js": "public, max-age=86400",                  # 1 day
    ".css": "public, max-age=86400",
    ".json": "public, max-age=300",                  # 5 min (data, may update)
    ".xml": "public, max-age=3600",
    ".webmanifest": "public, max-age=86400",
}


class CachedStaticFiles(StaticFiles):
    """StaticFiles subclass that tags each response with a Cache-Control
    header based on file extension. Keeps the mount one line at the call
    site while letting Cloudflare and browsers cache aggressively."""

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            # The service-worker script is the version gate for everything the
            # SW itself caches (econ_zones.json, bundle, statics). It must
            # ALWAYS be revalidated — a cached sw.js pins clients to an old
            # CACHE_NAME and every runtime-cached asset stays frozen with it.
            # (Bavi 2026: a 17h-old edge copy of sw.js held v11 after the v12
            # ERS-zone bump.) NB Cloudflare's Browser Cache TTL override can
            # still rewrite this for browsers; edge revalidation is what we
            # need, and no-cache achieves that once the current entry expires.
            if Path(str(path).replace("\\", "/")).name == "sw.js":
                response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
                return response
            ext = Path(path).suffix.lower()
            cache = _STATIC_CACHE_BY_EXT.get(ext)
            if cache:
                response.headers["Cache-Control"] = cache
        return response


app.mount("/frontend", CachedStaticFiles(directory=FRONTEND_DIR), name="frontend")
