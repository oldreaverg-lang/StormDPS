"""Build data/coast_distance.bin: distance to land for the Atlantic and the
East / Central Pacific, on a 0.1 degree grid.

Each cell holds 0 over land, else the distance from its centre to the nearest
land cell in 2 km units (1..255, capped at 510 km). core/coast_distance.py
reads it; the scoring path's offshore test and the Impact Area's near-land
test use it (see core/cumulative_dpi.offshore_fixes).

Land = Natural Earth 1:50m land polygons (public domain; exterior rings, so
lakes count as land) plus every coast point the app already has (coastline
waypoints, US coastal towns, Pacific coast points, Atlantic Canada), so small
islands the 50m layer drops (Bermuda) are still land.

    curl -4 -o ne_50m_land.geojson \\
      https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_land.geojson
    python scripts/build_coast_distance.py ne_50m_land.geojson

Needs numpy and Pillow (build time only; the reader is stdlib).
"""
from __future__ import annotations

import json
import math
import sys
import zlib
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "coast_distance.bin"

LAT0, LON0, STEP = 0.0, -180.0, 0.1
ROWS, COLS = 660, 1800            # 0..66N, 180W..0
UNIT_KM = 2.0
KM_PER_DEG = 111.195


def _rings(geom):
    if geom["type"] == "Polygon":
        yield geom["coordinates"][0]
    elif geom["type"] == "MultiPolygon":
        for poly in geom["coordinates"]:
            yield poly[0]


def land_mask(geojson_path: Path) -> np.ndarray:
    img = Image.new("1", (COLS, ROWS), 0)
    draw = ImageDraw.Draw(img)
    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    for feat in data["features"]:
        for ring in _rings(feat["geometry"]):
            if max(lon for lon, _ in ring) < LON0 or min(lon for lon, _ in ring) > LON0 + COLS * STEP:
                continue
            if max(lat for _, lat in ring) < LAT0 or min(lat for _, lat in ring) > LAT0 + ROWS * STEP:
                continue
            # pixel centres sit at integer coordinates: cell (r, c) centre is
            # lat0 + (r + 0.5) * step, lon0 + (c + 0.5) * step
            pts = [((lon - LON0) / STEP - 0.5, (lat - LAT0) / STEP - 0.5) for lon, lat in ring]
            if len(pts) >= 3:
                draw.polygon(pts, fill=1)
    mask = np.array(img, dtype=bool)

    sys.path.insert(0, str(ROOT))
    pts = []
    from core.land_proximity import _get_coastline_db
    pts += [(w.lat, w.lon) for w in _get_coastline_db().waypoints if w.region_key != "open_ocean"]
    from core.coastal_places import US_COASTAL_PLACES
    from core.pacific_coast import PACIFIC_COAST_POINTS
    from core.atlantic_canada import ATLANTIC_CANADA_POINTS
    pts += [(la, lo) for la, lo, _ in US_COASTAL_PLACES + PACIFIC_COAST_POINTS + ATLANTIC_CANADA_POINTS]
    for la, lo in pts:
        r, c = int((la - LAT0) / STEP), int((lo - LON0) / STEP)
        if 0 <= r < ROWS and 0 <= c < COLS:
            mask[r, c] = True
    return mask


def distance_km(mask: np.ndarray) -> np.ndarray:
    """Chamfer distance (8-neighbour, per-row longitude spacing) from each
    water cell to the nearest land cell centre. Exact along rows; within a
    few percent of the great-circle distance otherwise."""
    d = np.where(mask, 0.0, np.inf)
    lat_c = LAT0 + (np.arange(ROWS) + 0.5) * STEP
    dx = STEP * KM_PER_DEG * np.cos(np.radians(lat_c))
    dy = STEP * KM_PER_DEG
    idx = np.arange(COLS, dtype=float)

    def sweep_row(row, step):
        # min over k of row[k] + |j - k| * step, both directions
        left = np.minimum.accumulate(row - idx * step) + idx * step
        right = (np.minimum.accumulate((row + idx * step)[::-1]))[::-1] - idx * step
        return np.minimum(row, np.minimum(left, right))

    for _ in range(2):
        for order in (range(ROWS), range(ROWS - 1, -1, -1)):
            prev = None
            for i in order:
                row = d[i]
                if prev is not None:
                    j = prev
                    diag = math.hypot(dy, (dx[i] + dx[j]) / 2.0)
                    up = d[j]
                    cand = np.minimum(up + dy, np.minimum(np.concatenate(([np.inf], up[:-1])) + diag,
                                                          np.concatenate((up[1:], [np.inf])) + diag))
                    row = np.minimum(row, cand)
                d[i] = sweep_row(row, dx[i])
                prev = i
    return d


def main(path: str) -> None:
    mask = land_mask(Path(path))
    d = distance_km(mask)
    q = np.where(mask, 0, np.clip(np.ceil(d / UNIT_KM), 1, 255)).astype(np.uint8)
    header = {"lat0": LAT0, "lon0": LON0, "step": STEP, "rows": ROWS, "cols": COLS,
              "unit_km": UNIT_KM, "row_order": "south_to_north",
              "source": "Natural Earth 1:50m land + StormDPS coast points; scripts/build_coast_distance.py"}
    OUT.write_bytes(json.dumps(header).encode("ascii") + b"\n" + zlib.compress(q.tobytes(), 9))
    land = int(mask.sum())
    print(f"wrote {OUT.relative_to(ROOT)}: {OUT.stat().st_size // 1024} KB, land cells {land}, water {mask.size - land}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
