#!/usr/bin/env python3
"""
imerg_storm_rainfall.py — pull observed storm-total rainfall from NASA GPM IMERG
and emit a ground-truth record the DPS engine can consume.

WHY
---
The DPS rainfall term is currently a kinematic proxy (peak_rain_rate ×
effective_rain_hours; see core/rainfall_warning.py). It produced an *identical*
411 mm / "Normal" for both Kalmaegi and Fung-Wong 2025 even though Kalmaegi's
catastrophic damage was rainfall-flood driven. The engine already has an
override hook (core/dps_engine.py:109) that prefers an authoritative observed
peak rainfall (`GroundTruth.peak_rainfall_in`) over the heuristic. This script
fills that hook with a *measured* number for any storm, anywhere — including the
West Pacific, where there is no NHC TCR to transcribe.

WHAT IT DOES
------------
  1. Pulls the storm track (timestamp, lat, lon, r34) from the StormDPS API
     (or a local --track-json file).
  2. Determines the UTC day span and a bounding box around the track.
  3. Reads GPM IMERG Final *Daily* precipitation (GPM_3IMERGDF V07) for those
     days, subset to the box, and accumulates mm over the storm window.
  4. Masks the accumulation to cells within --buffer-km of the track so we
     attribute rain to *this* storm rather than background weather.
  5. Reports the peak grid-cell total (the station-peak proxy), the areal
     mean, and the area over 100/250/500 mm thresholds.
  6. Emits a ready-to-paste `GroundTruth(...)` snippet keyed by storm_id and,
     optionally, appends a JSON sidecar (data/imerg_rainfall.json).

DATA / ACCESS
-------------
  * Dataset: GPM_3IMERGDF (IMERG Final Run, daily, 0.1°, V07), via NASA GES DISC.
  * Auth: a free NASA Earthdata Login. One-time: in your Earthdata profile,
    authorize the "NASA GESDISC DATA ARCHIVE" application. Then either
      - export EARTHDATA_USERNAME / EARTHDATA_PASSWORD, or
      - run `earthaccess.login(strategy="interactive")` once to write ~/.netrc.
  * Deps (the fetch path only):  pip install earthaccess xarray h5netcdf numpy
    The --selftest and track-parsing paths need only numpy + the stdlib.

CAVEATS
-------
  * IMERG is a ~0.1° (~11 km) grid. A grid-cell maximum smooths over true
    point-gauge peaks, so `peak_cell_mm` is a slight UNDER-estimate of the
    highest station total. It is consistent storm-to-storm, which is what we
    need for calibration; treat it as a lower bound, not a record gauge value.
  * Final Run lags real time by ~3.5 months. For in-season storms use the Late
    Run (GPM_3IMERGDL) by passing --short-name GPM_3IMERGDL (less accurate).
  * Daily product attributes rain to whole UTC days the storm was present. For
    finer attribution use the half-hourly product (not implemented here).

USAGE
-----
  python scripts/imerg_storm_rainfall.py --storm-id 2025305N10138 \
        --name Kalmaegi --year 2025
  python scripts/imerg_storm_rainfall.py --storm-id 2025308N10143 \
        --name Fung-Wong --year 2025 --out-json data/imerg_rainfall.json
  python scripts/imerg_storm_rainfall.py --selftest
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Optional

MM_PER_INCH = 25.4
DEFAULT_API = "https://stormdps.com"
DEFAULT_SHORT_NAME = "GPM_3IMERGDF"   # IMERG Final, daily
DEFAULT_VERSION = "07"


# ---------------------------------------------------------------------------
# 1. Track ingestion
# ---------------------------------------------------------------------------
def fetch_track(api_base: str, storm_id: str) -> list[dict]:
    """Return [{time: datetime, lat, lon, r34_nm}] from the StormDPS track API."""
    url = f"{api_base.rstrip('/')}/api/v1/storms/{storm_id}/track"
    req = urllib.request.Request(url, headers={"User-Agent": "imerg-rain/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    return _parse_track(raw)


def load_track_json(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return _parse_track(json.load(f))


def _parse_track(raw: list[dict]) -> list[dict]:
    pts: list[dict] = []
    for p in raw:
        lat, lon = p.get("lat"), p.get("lon")
        ts = p.get("timestamp") or p.get("time")
        if lat is None or lon is None or ts is None:
            continue
        try:
            t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except ValueError:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        r34 = p.get("r34_nm")
        try:
            r34 = float(r34) if r34 not in (None, "") else None
        except (TypeError, ValueError):
            r34 = None
        pts.append({"time": t, "lat": float(lat), "lon": float(lon), "r34_nm": r34})
    if not pts:
        raise ValueError("track had no usable lat/lon/timestamp points")
    pts.sort(key=lambda d: d["time"])
    return pts


def storm_days(track: list[dict]) -> list[date]:
    """Distinct UTC dates spanned by the track (inclusive)."""
    d0 = track[0]["time"].astimezone(timezone.utc).date()
    d1 = track[-1]["time"].astimezone(timezone.utc).date()
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


# ---------------------------------------------------------------------------
# 2. Geometry helpers (pure numpy — exercised by --selftest)
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
    flat = np.nanargmax(masked)
    iy, ix = np.unravel_index(flat, masked.shape)
    peak_mm = float(masked[iy, ix])

    # Approximate per-cell area (0.1° grid) for threshold-area reporting.
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
# 3. IMERG accumulation (heavy path — earthaccess + xarray imported lazily)
# ---------------------------------------------------------------------------
def accumulate_imerg(
    track: list[dict],
    days: list[date],
    bbox: tuple[float, float, float, float],
    short_name: str,
    version: str,
) -> tuple["object", "object", "object"]:
    """Sum IMERG daily precip over `days`, subset to bbox. Returns
    (accum_mm[y,x], lat2d, lon2d) as numpy arrays."""
    import numpy as np
    import earthaccess
    import xarray as xr

    earthaccess.login(persist=True)  # env vars > ~/.netrc > interactive

    lat_min, lat_max, lon_min, lon_max = bbox
    t0 = datetime.combine(days[0], datetime.min.time())
    t1 = datetime.combine(days[-1], datetime.max.time())

    results = earthaccess.search_data(
        short_name=short_name,
        version=version,
        temporal=(t0.strftime("%Y-%m-%d"), t1.strftime("%Y-%m-%d")),
        bounding_box=(lon_min, lat_min, lon_max, lat_max),
    )
    if not results:
        raise SystemExit(
            f"No {short_name} v{version} granules for {days[0]}..{days[-1]}. "
            "Final Run lags ~3.5 months — try --short-name GPM_3IMERGDL (Late Run)."
        )

    files = earthaccess.open(results)
    accum = None
    lat2d = lon2d = None
    used = 0
    for fh in files:
        ds = xr.open_dataset(fh, engine="h5netcdf")
        var = "precipitation" if "precipitation" in ds else "precipitationCal"
        da = ds[var].squeeze()
        # IMERG daily dims are (lon, lat); normalize to (lat, lon).
        if "lat" in da.dims and "lon" in da.dims:
            da = da.transpose("lat", "lon")
        sub = da.sel(
            lat=slice(lat_min, lat_max), lon=slice(lon_min, lon_max)
        )
        vals = np.asarray(sub.values, dtype="float64")
        vals[vals < 0] = 0.0  # IMERG fill is -9999.9
        if accum is None:
            accum = np.zeros_like(vals)
            lons = np.asarray(sub["lon"].values)
            lats = np.asarray(sub["lat"].values)
            lon2d, lat2d = np.meshgrid(lons, lats)
        accum += vals
        used += 1
        ds.close()

    if accum is None:
        raise SystemExit("granules found but none overlapped the bounding box")
    print(f"  accumulated {used} daily granule(s) over {accum.shape} grid")
    return accum, lat2d, lon2d


# ---------------------------------------------------------------------------
# 4. Output
# ---------------------------------------------------------------------------
def ground_truth_snippet(storm_id, name, year, summary, days) -> str:
    loc = f"{summary['peak_cell_lat']}, {summary['peak_cell_lon']}"
    src = (
        f"NASA GPM IMERG Final daily (0.1°), storm-total over "
        f"{days[0]}..{days[-1]}"
    )
    return (
        f'    "{storm_id}": GroundTruth(\n'
        f'        storm_id="{storm_id}",\n'
        f'        name="{name or storm_id}",\n'
        f"        year={year or 0},\n"
        f"        peak_rainfall_in={summary['peak_cell_in']},   "
        f"# IMERG 0.1° grid peak ({summary['peak_cell_mm']} mm) — lower bound\n"
        f'        peak_rainfall_location="IMERG grid peak @ {loc}",\n'
        f'        sources=["{src}"],\n'
        f"    ),"
    )


def write_sidecar(path, storm_id, name, year, summary, days):
    try:
        with open(path, "r", encoding="utf-8") as f:
            db = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        db = {}
    db[storm_id] = {
        "storm_id": storm_id,
        "name": name,
        "year": year,
        "peak_rainfall_in": summary["peak_cell_in"],
        "peak_rainfall_mm": summary["peak_cell_mm"],
        "peak_rainfall_location": [summary["peak_cell_lat"], summary["peak_cell_lon"]],
        "window": [str(days[0]), str(days[-1])],
        "mean_footprint_mm": summary["mean_footprint_mm"],
        "area_gt_250mm_km2": summary["area_gt_250mm_km2"],
        "source": "NASA GPM IMERG Final daily V07",
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, sort_keys=True)
    print(f"  wrote {path} ({len(db)} storm(s))")


# ---------------------------------------------------------------------------
# 4b. Batch driver — process the whole catalog into ONE shared sidecar
# ---------------------------------------------------------------------------
def iter_catalog(api_base: str, basins: set[str], min_year: int):
    """Yield (storm_id, name, year) for named catalog storms matching the
    basin filter and >= min_year (IMERG V07 starts 2000-06)."""
    url = f"{api_base.rstrip('/')}/api/v1/storms/catalog"
    req = urllib.request.Request(url, headers={"User-Agent": "imerg-rain/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        cat = json.loads(resp.read().decode("utf-8"))
    for s in cat:
        sid, name, yr = s.get("id"), s.get("name"), s.get("year")
        if not sid or not name or not isinstance(yr, int):
            continue
        if yr < min_year:
            continue
        if basins and (s.get("basin") or "").upper() not in basins:
            continue
        yield sid, name, yr


def process_storm(api_base: str, storm_id: str, name, year, args, *, quiet=False) -> dict:
    """Full pipeline for one storm: track -> IMERG accumulation -> summary."""
    track = (load_track_json(args.track_json) if args.track_json
             else fetch_track(api_base, storm_id))
    days = storm_days(track)
    bbox = track_bbox(track, args.pad_deg)
    if not quiet:
        print(f"{name or storm_id}: {len(track)} pts, {days[0]}..{days[-1]} "
              f"({len(days)}d), bbox={tuple(round(b, 1) for b in bbox)}")
    accum, lat2d, lon2d = accumulate_imerg(track, days, bbox, args.short_name, args.version)
    mask = track_mask(lat2d, lon2d, track, args.buffer_km)
    summary = summarize(accum, lat2d, lon2d, mask)
    summary["_days"] = days
    return summary


def run_batch(args) -> int:
    """Process many storms into one shared sidecar. Resumable (skips storms
    already present unless --force); per-storm failures are logged and skipped
    so one bad storm never aborts a multi-hour backfill."""
    if not args.out_json:
        raise SystemExit("--out-json is required in batch mode (the shared sidecar)")

    targets: list[tuple[str, Optional[str], Optional[int]]] = []
    if args.ids_file:
        with open(args.ids_file, "r", encoding="utf-8") as f:
            targets += [(ln.strip(), None, None) for ln in f if ln.strip()
                        and not ln.startswith("#")]
    if args.catalog:
        basins = {b.strip().upper() for b in args.basins.split(",") if b.strip()}
        targets += list(iter_catalog(args.api_base, basins, args.min_year))

    try:
        with open(args.out_json, "r", encoding="utf-8") as f:
            done = set(json.load(f).keys())
    except (FileNotFoundError, json.JSONDecodeError):
        done = set()

    todo = [(sid, nm, yr) for (sid, nm, yr) in targets if args.force or sid not in done]
    print(f"batch: {len(targets)} target(s), {len(done)} already done, "
          f"{len(todo)} to process (product={args.short_name})\n")

    ok = fail = 0
    for i, (sid, nm, yr) in enumerate(todo, 1):
        tag = f"[{i}/{len(todo)}] {nm or sid}"
        try:
            summary = process_storm(args.api_base, sid, nm, yr, args, quiet=False)
            write_sidecar(args.out_json, sid, nm, yr, summary, summary["_days"])
            print(f"  {tag}: peak {summary['peak_cell_mm']} mm "
                  f"({summary['peak_cell_in']} in)\n")
            ok += 1
        except KeyboardInterrupt:
            print("interrupted — progress saved to sidecar"); break
        except Exception as e:  # one storm's failure must not kill the batch
            print(f"  {tag}: SKIP ({type(e).__name__}: {e})\n")
            fail += 1
    print(f"batch done: {ok} written, {fail} failed/skipped")
    return 0


# ---------------------------------------------------------------------------
# 5. Self-test (no network / no earthaccess) — validates the core geometry
# ---------------------------------------------------------------------------
def selftest() -> int:
    import numpy as np

    # Synthetic 0.1° grid over the Philippines, two rain blobs:
    #   A: 600 mm centered ON the track (should be picked up)
    #   B: 900 mm but 400 km OFF-track (should be masked out)
    lats = np.arange(8.0, 16.0, 0.1)
    lons = np.arange(120.0, 130.0, 0.1)
    lon2d, lat2d = np.meshgrid(lons, lats)
    track = [
        {"time": datetime(2025, 11, 4, tzinfo=timezone.utc), "lat": 11.0, "lon": 124.0, "r34_nm": 90},
        {"time": datetime(2025, 11, 4, 6, tzinfo=timezone.utc), "lat": 11.5, "lon": 123.0, "r34_nm": 90},
    ]

    def blob(c_lat, c_lon, peak, sigma_km):
        d = haversine_km(c_lat, c_lon, lat2d, lon2d)
        return peak * np.exp(-(d ** 2) / (2 * sigma_km ** 2))

    accum = blob(11.2, 123.5, 600, 40) + blob(14.5, 127.0, 900, 40)

    mask = track_mask(lat2d, lon2d, track, buffer_km=150)
    s = summarize(accum, lat2d, lon2d, mask)

    ok = True
    if not (560 <= s["peak_cell_mm"] <= 605):
        print(f"  FAIL peak_cell_mm={s['peak_cell_mm']} (expected ~600, on-track blob)")
        ok = False
    if s["peak_cell_lat"] > 13.0:
        print(f"  FAIL picked the off-track blob (lat={s['peak_cell_lat']})")
        ok = False
    if abs(s["peak_cell_in"] - s["peak_cell_mm"] / MM_PER_INCH) > 0.01:
        print("  FAIL inch conversion")
        ok = False
    print("  selftest result:", json.dumps(s))
    print("  PASS" if ok else "  FAILED")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--storm-id", help="IBTrACS SID or ATCF id, e.g. 2025305N10138")
    ap.add_argument("--name", default=None, help="display name for the ground-truth record")
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--api-base", default=DEFAULT_API)
    ap.add_argument("--track-json", default=None, help="local track file instead of the API")
    ap.add_argument("--buffer-km", type=float, default=150.0, help="attribute rain within this radius of the track")
    ap.add_argument("--pad-deg", type=float, default=2.0, help="bbox padding around the track")
    ap.add_argument("--short-name", default=DEFAULT_SHORT_NAME, help="GPM_3IMERGDF (Final) or GPM_3IMERGDL (Late)")
    ap.add_argument("--version", default=DEFAULT_VERSION)
    ap.add_argument("--out-json", default=None, help="sidecar JSON (e.g. data/imerg_rainfall.json); required for batch")
    ap.add_argument("--selftest", action="store_true")
    # Batch mode (across-the-board backfill into one shared sidecar)
    ap.add_argument("--catalog", action="store_true", help="process every catalog storm matching --basins/--min-year")
    ap.add_argument("--ids-file", default=None, help="newline-separated storm ids to process")
    ap.add_argument("--basins", default="WP,IO,SH,EP,NA,SI,SP", help="basin filter for --catalog")
    ap.add_argument("--min-year", type=int, default=2000, help="skip storms before IMERG (V07 starts 2000-06)")
    ap.add_argument("--force", action="store_true", help="reprocess storms already in the sidecar")
    args = ap.parse_args(argv)

    if args.selftest:
        return selftest()
    if args.catalog or args.ids_file:
        return run_batch(args)
    if not args.storm_id:
        ap.error("--storm-id is required (or use --catalog / --ids-file / --selftest)")

    summary = process_storm(args.api_base, args.storm_id, args.name, args.year, args)
    days = summary["_days"]
    print("\n=== IMERG observed rainfall ===")
    for k, v in summary.items():
        if k != "_days":
            print(f"  {k:20} = {v}")
    print("\n=== paste into core/ground_truth.py _REGISTRY ===")
    print(ground_truth_snippet(args.storm_id, args.name, args.year, summary, days))

    if args.out_json:
        write_sidecar(args.out_json, args.storm_id, args.name, args.year, summary, days)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
