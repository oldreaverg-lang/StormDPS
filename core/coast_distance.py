"""Distance to land, Atlantic and East / Central Pacific (0-66N, 180W-0).

Reads data/coast_distance.bin (built by scripts/build_coast_distance.py from
Natural Earth 1:50m land plus the app's own coast points): a 0.1 degree grid
whose cells hold 0 over land, else the distance to the nearest land in 2 km
units, capped at 510 km.

The app's coast POINT sets (coastline waypoints, US towns, Pacific and
Canadian points) have holes: nothing between Veracruz and Brownsville, little
in the Bahamas, and no notion of inland. A fix 300 km into Honduras or over
the southern Bahamas looked "far from any coast". This grid answers "is this
over open water, and how far from land" everywhere in those waters.

Stdlib only; ~1.2 MB in memory once loaded.
"""
from __future__ import annotations

import json
import logging
import math
import zlib
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PATH = Path(__file__).resolve().parent.parent / "data" / "coast_distance.bin"
_grid = None   # (header, bytes) once loaded; False if unavailable


def _load():
    global _grid
    if _grid is None:
        try:
            raw = _PATH.read_bytes()
            nl = raw.index(b"\n")
            head = json.loads(raw[:nl].decode("ascii"))
            cells = zlib.decompress(raw[nl + 1:])
            if len(cells) != head["rows"] * head["cols"]:
                raise ValueError("coast distance grid has the wrong size")
            _grid = (head, cells)
        except Exception as e:  # fail soft: callers treat None as "unknown"
            logger.warning(f"[coast_distance] grid unavailable: {e}")
            _grid = False
    return _grid or None


def available() -> bool:
    return _load() is not None


def coast_km(lat: Optional[float], lon: Optional[float]) -> Optional[float]:
    """0.0 over land, else km to the nearest land (510 means 510 or more).
    None outside the grid or when the grid is missing."""
    g = _load()
    if g is None or lat is None or lon is None:
        return None
    head, cells = g
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lat) and math.isfinite(lon)):
        return None
    lon = ((lon + 180.0) % 360.0) - 180.0
    r = int(math.floor((lat - head["lat0"]) / head["step"]))
    c = int(math.floor((lon - head["lon0"]) / head["step"]))
    if not (0 <= r < head["rows"] and 0 <= c < head["cols"]):
        return None
    return cells[r * head["cols"] + c] * head["unit_km"]
