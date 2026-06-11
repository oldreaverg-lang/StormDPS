#!/usr/bin/env python3
"""
imerg_storm_rainfall.py — pull observed storm-total rainfall from NASA GPM IMERG
and emit a ground-truth record the DPS engine can consume.

The reusable core (track geometry + NASA fetch) lives in
``services/imerg_rainfall.py`` so the live ingest loop shares one implementation.
This file is the offline CLI: it pulls the track from the StormDPS API, runs the
core, and writes a ground-truth sidecar.

WHY
---
The DPS rainfall term is a kinematic proxy (core/rainfall_warning.py) that gave
an identical 411 mm / "Normal" to both Kalmaegi and Fung-Wong 2025, then gets
gated out (rain_inland needs warning_score > 30, which it never clears). The
engine already prefers an authoritative observed peak rainfall when present
(core/dps_engine.py override). This fills that hook with a *measured* number for
any storm, anywhere — including the West Pacific, where there is no NHC TCR.

MODES
-----
  single : one storm -> prints a paste-ready GroundTruth snippet
  batch  : --catalog / --ids-file -> backfills the whole catalog into one
           shared data/imerg_rainfall.json (resumable, fault-tolerant)

DATA / ACCESS
-------------
  * Dataset: GPM_3IMERGDF (Final, daily, 0.1°, V07) via NASA GES DISC. For
    in-season storms use --short-name GPM_3IMERGDL (Late Run, ~14 h lag).
  * Auth: a free NASA Earthdata Login; authorize "NASA GESDISC DATA ARCHIVE" in
    your profile, then export EARTHDATA_USERNAME / EARTHDATA_PASSWORD (or run
    `earthaccess.login(strategy="interactive")` once to write ~/.netrc).
  * Deps (fetch path only):  pip install earthaccess xarray h5netcdf numpy
    --selftest and track parsing need only numpy + the stdlib.

CAVEATS
-------
  * IMERG is a ~0.1° (~11 km) grid; a cell maximum smooths over true point-gauge
    peaks, so peak_cell_mm is a lower bound — consistent storm-to-storm, which is
    what calibration needs.

USAGE
-----
  python scripts/imerg_storm_rainfall.py --storm-id 2025305N10138 --name Kalmaegi --year 2025
  python scripts/imerg_storm_rainfall.py --catalog --min-year 2000 --out-json data/imerg_rainfall.json
  python scripts/imerg_storm_rainfall.py --selftest
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Make `services` importable when run as `python scripts/imerg_storm_rainfall.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.imerg_rainfall import (  # noqa: E402
    MM_PER_INCH,
    FINAL_SHORT_NAME,
    DEFAULT_VERSION,
    accumulate_imerg,
    haversine_km,
    storm_days,
    summarize,
    track_bbox,
    track_mask,
)

DEFAULT_API = "https://stormdps.com"


# ---------------------------------------------------------------------------
# Track ingestion (CLI side)
# ---------------------------------------------------------------------------
def fetch_track(api_base: str, storm_id: str) -> list[dict]:
    """Return [{time, lat, lon, r34_nm}] from the StormDPS track API."""
    url = f"{api_base.rstrip('/')}/api/v1/storms/{storm_id}/track"
    req = urllib.request.Request(url, headers={"User-Agent": "imerg-rain/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return _parse_track(json.loads(resp.read().decode("utf-8")))


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


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def ground_truth_snippet(storm_id, name, year, summary, days) -> str:
    loc = f"{summary['peak_cell_lat']}, {summary['peak_cell_lon']}"
    src = f"NASA GPM IMERG Final daily (0.1°), storm-total over {days[0]}..{days[-1]}"
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
# Batch driver — process the whole catalog into ONE shared sidecar
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
        if not sid or not name or not isinstance(yr, int) or yr < min_year:
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
            targets += [(ln.strip(), None, None) for ln in f
                        if ln.strip() and not ln.startswith("#")]
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
# Self-test (no network / no earthaccess) — validates the core geometry
# ---------------------------------------------------------------------------
def selftest() -> int:
    import numpy as np

    # Synthetic 0.1° grid, two rain blobs: A=600 mm ON the track (kept),
    # B=900 mm but 400 km OFF-track (must be masked out).
    lats = np.arange(8.0, 16.0, 0.1)
    lons = np.arange(120.0, 130.0, 0.1)
    lon2d, lat2d = np.meshgrid(lons, lats)
    track = [
        {"time": datetime(2025, 11, 4, tzinfo=timezone.utc), "lat": 11.0, "lon": 124.0},
        {"time": datetime(2025, 11, 4, 6, tzinfo=timezone.utc), "lat": 11.5, "lon": 123.0},
    ]

    def blob(c_lat, c_lon, peak, sigma_km):
        d = haversine_km(c_lat, c_lon, lat2d, lon2d)
        return peak * np.exp(-(d ** 2) / (2 * sigma_km ** 2))

    accum = blob(11.2, 123.5, 600, 40) + blob(14.5, 127.0, 900, 40)
    mask = track_mask(lat2d, lon2d, track, buffer_km=150)
    s = summarize(accum, lat2d, lon2d, mask)

    ok = True
    if not (560 <= s["peak_cell_mm"] <= 605):
        print(f"  FAIL peak_cell_mm={s['peak_cell_mm']} (expected ~600 on-track)"); ok = False
    if s["peak_cell_lat"] > 13.0:
        print(f"  FAIL picked the off-track blob (lat={s['peak_cell_lat']})"); ok = False
    print("  selftest result:", json.dumps(s))
    print("  PASS" if ok else "  FAILED")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--storm-id", help="IBTrACS SID or ATCF id, e.g. 2025305N10138")
    ap.add_argument("--name", default=None)
    ap.add_argument("--year", type=int, default=None)
    ap.add_argument("--api-base", default=DEFAULT_API)
    ap.add_argument("--track-json", default=None, help="local track file instead of the API")
    ap.add_argument("--buffer-km", type=float, default=150.0, help="attribute rain within this radius of the track")
    ap.add_argument("--pad-deg", type=float, default=2.0, help="bbox padding around the track")
    ap.add_argument("--short-name", default=FINAL_SHORT_NAME, help="GPM_3IMERGDF (Final) or GPM_3IMERGDL (Late)")
    ap.add_argument("--version", default=DEFAULT_VERSION)
    ap.add_argument("--out-json", default=None, help="sidecar JSON; required for batch")
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
