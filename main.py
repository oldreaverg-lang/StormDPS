"""
Hurricane IKE Valuation API — main FastAPI application.

Run with:
    uvicorn main:app --reload --port 8000

API docs available at:
    http://localhost:8000/docs
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.routes import router, generate_preload_bundle

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle. Pre-computes IKE data for preset storms."""
    # --- STARTUP: warm the preload cache in the background ---
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
            logger.warning(f"[PRELOAD] Warm-up failed: {e}")

    # Run in background so the server starts accepting requests immediately
    asyncio.create_task(warm_preload())
    yield
    # --- SHUTDOWN: nothing to clean up ---

app = FastAPI(
    title="Hurricane IKE Valuation API",
    description=(
        "Compute Integrated Kinetic Energy (IKE) and destructive value scores "
        "for tropical cyclones using NOAA/NHC data. IKE integrates wind speed "
        "over the entire storm area, providing a more complete measure of "
        "destructive potential than max wind speed alone."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Allow cross-origin requests for frontend app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "hurricane-ike-api"}


# Serve the frontend at root
FRONTEND_DIR = Path(__file__).parent / "frontend"


@app.get("/")
async def serve_frontend():
    return FileResponse(FRONTEND_DIR / "index.html")


# Serve any other static assets from the frontend folder
app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")
