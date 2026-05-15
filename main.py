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
from contextlib import asynccontextmanager
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import httpx

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from api.routes import (
    router,
    generate_preload_bundle,
    warm_dps_cache,
    refresh_active_dps_loop,
    load_active_storms_from_disk,
    warm_ibtracs_catalog,
    warm_track_cache,
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

    yield
    # --- SHUTDOWN: cancel background loops ---
    for attr in ("dps_refresh_task", "overlay_evict_task"):
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
    headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
        "style-src 'self' 'unsafe-inline' https://unpkg.com; "
        "font-src 'self' data:; "
        "img-src 'self' data: blob: https:; "
        "connect-src 'self' https://*.stormdps.com https://api.open-meteo.com; "
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


# ---------------------------------------------------------------------------
# Legacy web frontend (still served for backward compatibility)
# ---------------------------------------------------------------------------

FRONTEND_DIR = Path(__file__).parent / "frontend"

# ---------------------------------------------------------------------------
# Error handling — return JSON for all errors (mobile clients expect JSON)
# ---------------------------------------------------------------------------

@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    path = request.url.path

    # ── Security: block probes for sensitive files ──
    # Bots routinely probe for .env, .git, wp-admin, etc.
    # Return a hard 404 — never serve content for these paths.
    _blocked_patterns = (
        ".env", ".git", ".aws", ".ssh", ".docker",
        "wp-admin", "wp-login", "phpinfo", ".php",
        "/.htaccess", "/server-status", "/debug",
        "/config", "/credentials", "/secret",
    )
    path_lower = path.lower()
    if any(p in path_lower for p in _blocked_patterns):
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


@app.get("/")
async def serve_frontend():
    # Cache-Control lets Cloudflare cache the SPA shell at the edge so the
    # majority of users skip the origin entirely. PageSpeed flagged the
    # document request as 1,259 ms server time — likely Railway cold-start
    # contention with the lifespan warm tasks. Edge cache hides that from
    # everyone after the first hit.
    return FileResponse(
        FRONTEND_DIR / "index.html",
        headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
    )


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
    path_lower = path.lower()
    if any(p in path_lower for p in (".env", ".git", ".aws", ".php", ".htaccess", "wp-")):
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
        # Vite hashed assets are immutable — cache aggressively
        if path.startswith("assets/"):
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
async def serve_compiled_bundle():
    if _VOLUME_COMPILED_BUNDLE.exists():
        return FileResponse(
            _VOLUME_COMPILED_BUNDLE,
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
        )
    baked = FRONTEND_DIR / "compiled_bundle.json"
    if baked.exists():
        return FileResponse(
            baked,
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=300, s-maxage=900"},
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
            ext = Path(path).suffix.lower()
            cache = _STATIC_CACHE_BY_EXT.get(ext)
            if cache:
                response.headers["Cache-Control"] = cache
        return response


app.mount("/frontend", CachedStaticFiles(directory=FRONTEND_DIR), name="frontend")
