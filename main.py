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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

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
from api.satellite_routes import router as satellite_router
from api.wind_routes import router as wind_router
from api.pressure_routes import router as pressure_router
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

    # --- STARTUP: warm the preload cache in the background ---
    # IKE cache files are shipped with the Docker image, so this should
    # find everything "already cached" and skip heavy NOAA recomputation.
    async def warm_preload():
        try:
            result = await generate_preload_bundle(grid_resolution_km=15.0, skip_points=1)
            logger.info(
                f"[PRELOAD] Warm-up complete: "
                f"{result['already_cached']} cached, "
                f"{result['newly_computed']} computed, "
                f"{result['failed']} failed"
            )
        except Exception as e:
            logger.warning(f"[PRELOAD] Warm-up failed (non-fatal): {e}")

    # Run in background so the server starts accepting requests immediately
    asyncio.create_task(warm_preload())

    # --- STARTUP: warm the DPS cache (persistent volume) ---
    # Pre-computes DPS bundles for preset + active storms and writes them to
    # $PERSISTENT_DATA_DIR/cache/dps/. Lets first visitors hit the catalog
    # and hero card without waiting for on-demand engine runs.
    async def warm_dps():
        try:
            await warm_dps_cache(app.state, include_active=True)
        except Exception as e:
            logger.warning(f"[DPS WARM] startup warm failed (non-fatal): {e}")

    asyncio.create_task(warm_dps())

    # --- STARTUP: warm the IBTrACS global catalog (persistent volume) ---
    # Fetches the ~200 MB IBTrACS CSV from NOAA and parses it into an in-memory
    # catalog keyed by sid. Writes a metadata-only index to disk for fast
    # restart. Fail-open: if NOAA is unreachable, the previously persisted
    # index is used. Non-fatal — historical-storm endpoints fall back to
    # on-demand parsing if this never completes.
    async def warm_ibtracs():
        try:
            result = await warm_ibtracs_catalog()
            logger.info(f"[IBTRACS WARM] complete: {result}")
        except Exception as e:
            logger.warning(f"[IBTRACS WARM] startup warm failed (non-fatal): {e}")

    asyncio.create_task(warm_ibtracs())

    # --- STARTUP: warm per-storm track snapshot cache ---
    # Pre-fetches track snapshots for preset + active storms and persists them
    # to $PERSISTENT_DATA_DIR/cache/tracks/. Eliminates the cold-start NOAA
    # round-trip on the first /ibtracs/track/{sid} request.
    async def warm_tracks():
        try:
            result = await warm_track_cache()
            logger.info(f"[TRACK WARM] complete: {result}")
        except Exception as e:
            logger.warning(f"[TRACK WARM] startup warm failed (non-fatal): {e}")

    asyncio.create_task(warm_tracks())

    # --- STARTUP: hourly refresh for live tropics ---
    # Keeps active-storm DPS fresh between deploys. Loop cancels on shutdown.
    app.state.dps_refresh_task = asyncio.create_task(
        refresh_active_dps_loop(app.state, interval_seconds=3600)
    )

    yield
    # --- SHUTDOWN: cancel DPS refresh loop ---
    task = getattr(app.state, "dps_refresh_task", None)
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
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

# CORS — allow all origins for development.
# In production, set ALLOWED_ORIGINS env var to a comma-separated list.
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Request-Id", "X-Response-Time"],
    max_age=600,  # Cache preflight for 10 min (reduces OPTIONS roundtrips on mobile)
)

# GZip — compress API responses for mobile bandwidth efficiency
app.add_middleware(GZipMiddleware, minimum_size=500)

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

app.include_router(router, prefix="/api/v1")
app.include_router(weather_router, prefix="/api/v1")
app.include_router(satellite_router, prefix="/api/v1")
app.include_router(wind_router, prefix="/api/v1")
app.include_router(pressure_router, prefix="/api/v1")
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
    return FileResponse(FRONTEND_DIR / "index.html")


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
            headers={"Cache-Control": "public, max-age=60"},
        )
    baked = FRONTEND_DIR / "compiled_bundle.json"
    if baked.exists():
        return FileResponse(
            baked,
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=60"},
        )
    raise HTTPException(status_code=404, detail="compiled_bundle.json not found")


# Serve any other static assets from the frontend folder
app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")
