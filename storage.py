"""
Centralized persistent storage manager for StormDPS.

Every module that reads/writes to the Railway persistent volume should
import paths from here instead of constructing its own.  This gives us:

  1. One place to change the base directory
  2. Automatic directory creation on import
  3. Disk-usage helpers for the /health endpoint
  4. Cache eviction utilities

Environment variable:
    PERSISTENT_DATA_DIR  — absolute path to the Railway volume mount.
                           Falls back to <repo_root>/data for local dev.

Directory layout on the persistent volume:

    $PERSISTENT_DATA_DIR/
    ├── cache/
    │   ├── ike/                 # Pre-computed IKE results (JSON per storm)
    │   ├── ibtracs_catalog.json # Parsed IBTrACS global catalog
    │   ├── hurdat2.txt          # HURDAT2 historical database
    │   └── preload_bundle.json  # Compiled DPS bundle for frontend
    ├── validation/
    │   ├── validation.db        # SQLite — per-snapshot accuracy log
    │   └── <season>/            # JSONL outcome logs per year
    └── audit/
        └── wind_radii/          # Wind radii QA artifacts
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Base directory ──────────────────────────────────────────────────────────
_FALLBACK = str(Path(__file__).parent / "data")
PERSISTENT_DATA_DIR = Path(os.environ.get("PERSISTENT_DATA_DIR", _FALLBACK))

# ── Subdirectory definitions ────────────────────────────────────────────────
CACHE_DIR = PERSISTENT_DATA_DIR / "cache"
IKE_CACHE_DIR = CACHE_DIR / "ike"
DPS_CACHE_DIR = CACHE_DIR / "dps"  # Per-storm DPS bundles (one JSON per storm_id)
TRACK_CACHE_DIR = CACHE_DIR / "tracks"  # Parsed IBTrACS track snapshots (one JSON per sid)
IBTRACS_CACHE_FILE = CACHE_DIR / "ibtracs_catalog.json"
IBTRACS_INDEX_FILE = CACHE_DIR / "ibtracs_index_v1.json"  # Metadata-only index
ACTIVE_STORMS_FILE = CACHE_DIR / "active_storms.json"  # Persistent snapshot of NHC/JTWC list
HURDAT2_CACHE_FILE = CACHE_DIR / "hurdat2.txt"
PRELOAD_BUNDLE_FILE = CACHE_DIR / "preload_bundle.json"
# Compiled bundle on the persistent volume — served ahead of the frontend/
# directory copy so redeploys don't drop the latest compiled scores until
# compile_cache runs again.
COMPILED_BUNDLE_FILE = CACHE_DIR / "compiled_bundle.json"

VALIDATION_DIR = PERSISTENT_DATA_DIR / "validation"
VALIDATION_DB = VALIDATION_DIR / "validation.db"

AUDIT_DIR = PERSISTENT_DATA_DIR / "audit"
WIND_RADII_AUDIT_DIR = AUDIT_DIR / "wind_radii"

# Satellite tile cache — Web Mercator slippy tiles fetched from NASA GIBS.
# Layout: SATELLITE_CACHE_DIR/<satellite>/<YYYYMMDDTHHMM>/z/x/y.png
# Eviction: keep last 48 hours of frames per satellite (see satellite_routes).
SATELLITE_CACHE_DIR = CACHE_DIR / "satellite"

# Wind field JSON cache — leaflet-velocity-formatted U/V grids fetched from
# Open-Meteo. Layout: WIND_CACHE_DIR/<bbox_key>/<YYYYMMDDTHH>.json
# Eviction: 48h TTL via wind_routes.evict_old_wind_frames.
WIND_CACHE_DIR = CACHE_DIR / "wind"

# Pressure (MSLP) field JSON cache — gridded pressure_msl values from Open-Meteo
# plus contoured isobar GeoJSON. Layout: PRESSURE_CACHE_DIR/<bbox_key>/<YYYYMMDDTHH>.json
# Eviction: 48h TTL via pressure_routes.evict_old_pressure_frames.
PRESSURE_CACHE_DIR = CACHE_DIR / "pressure"

# METAR station observation cache — short TTL (15 min) since observations
# update hourly. Layout: METAR_CACHE_DIR/<bbox_key>.json
METAR_CACHE_DIR = CACHE_DIR / "metar"

# Precipitation + cloud-cover field JSON cache — gridded precipitation_mm and
# cloudcover_% values from Open-Meteo. Layout: PRECIP_CACHE_DIR/<bbox_key>/<YYYYMMDDTHH>.json
# Eviction: 48h TTL via precip_routes.evict_old_precip_frames.
PRECIP_CACHE_DIR = CACHE_DIR / "precip"

# ── Create all directories on import ────────────────────────────────────────
for _d in (IKE_CACHE_DIR, DPS_CACHE_DIR, TRACK_CACHE_DIR, SATELLITE_CACHE_DIR,
           WIND_CACHE_DIR, PRESSURE_CACHE_DIR, METAR_CACHE_DIR, PRECIP_CACHE_DIR,
           VALIDATION_DIR, WIND_RADII_AUDIT_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ── Atomic JSON write helper ────────────────────────────────────────────────
# Used by any module that writes to the persistent volume. Writes to a
# temp file in the same directory, then renames — guarantees readers
# never see a partially-written file even if the writer crashes mid-write.

def atomic_write_json(path: Path, data, *, indent: int | None = None) -> None:
    """Write JSON atomically: tmp file + rename."""
    import json as _json
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        _json.dump(data, f, indent=indent)
    os.replace(tmp, path)

logger.info("StormDPS storage root: %s", PERSISTENT_DATA_DIR)


# ── Disk usage helpers ──────────────────────────────────────────────────────

def _dir_size(path: Path) -> int:
    """Total bytes consumed by *path* (recursively)."""
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except OSError:
        pass
    return total


def _file_count(path: Path) -> int:
    """Number of files under *path* (recursively)."""
    try:
        return sum(1 for f in path.rglob("*") if f.is_file())
    except OSError:
        return 0


def storage_summary() -> dict:
    """Return a JSON-serialisable summary of persistent storage usage.

    Useful for the ``/health`` endpoint and operational monitoring.
    """
    sections = {
        "ike_cache": IKE_CACHE_DIR,
        "validation": VALIDATION_DIR,
        "audit": AUDIT_DIR,
    }
    result: dict = {
        "root": str(PERSISTENT_DATA_DIR),
    }

    total_bytes = 0
    for key, path in sections.items():
        sz = _dir_size(path)
        total_bytes += sz
        result[key] = {
            "path": str(path),
            "size_mb": round(sz / 1_048_576, 2),
            "files": _file_count(path),
        }

    # Add pre-cache directories
    for key, path in {
        "dps_cache": DPS_CACHE_DIR,
        "track_cache": TRACK_CACHE_DIR,
        "satellite_cache": SATELLITE_CACHE_DIR,
        "wind_cache": WIND_CACHE_DIR,
        "pressure_cache": PRESSURE_CACHE_DIR,
        "metar_cache": METAR_CACHE_DIR,
        "precip_cache": PRECIP_CACHE_DIR,
    }.items():
        sz = _dir_size(path)
        total_bytes += sz
        result[key] = {
            "path": str(path),
            "size_mb": round(sz / 1_048_576, 2),
            "files": _file_count(path),
        }

    # Top-level files (ibtracs, hurdat2, preload bundle)
    for label, fp in [
        ("ibtracs_catalog", IBTRACS_CACHE_FILE),
        ("ibtracs_index", IBTRACS_INDEX_FILE),
        ("active_storms", ACTIVE_STORMS_FILE),
        ("hurdat2", HURDAT2_CACHE_FILE),
        ("preload_bundle", PRELOAD_BUNDLE_FILE),
    ]:
        if fp.exists():
            sz = fp.stat().st_size
            total_bytes += sz
            result[label] = {"size_mb": round(sz / 1_048_576, 2)}

    result["total_mb"] = round(total_bytes / 1_048_576, 2)

    # Disk free (may not be available in all environments)
    try:
        usage = shutil.disk_usage(str(PERSISTENT_DATA_DIR))
        result["volume_total_mb"] = round(usage.total / 1_048_576, 2)
        result["volume_free_mb"] = round(usage.free / 1_048_576, 2)
        result["volume_used_pct"] = round(
            (usage.used / usage.total) * 100, 1
        )
    except OSError:
        pass

    return result


# ── IKE cache eviction ──────────────────────────────────────────────────────

IKE_MAX_FILES = 500
IKE_MAX_BYTES = 200 * 1_048_576  # 200 MB


def evict_ike_cache() -> int:
    """Remove oldest 25 % of IKE cache files when limits are exceeded.

    Returns the number of files removed.
    """
    files = sorted(IKE_CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime)
    if len(files) <= IKE_MAX_FILES and _dir_size(IKE_CACHE_DIR) <= IKE_MAX_BYTES:
        return 0

    to_remove = max(1, len(files) // 4)
    removed = 0
    for f in files[:to_remove]:
        try:
            f.unlink()
            removed += 1
        except OSError:
            pass
    logger.info("IKE cache eviction: removed %d / %d files", removed, len(files))
    return removed
