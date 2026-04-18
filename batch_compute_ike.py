#!/usr/bin/env python3
"""
Batch-compute IKE data for all Atlantic storms using local IBTrACS CSV cache.

This script bypasses the API and directly:
1. Parses the cached ibtracs_all.csv for each storm
2. Computes IKE snapshots using the core engine
3. Saves cache files to data/cache/ike/

After running, use:
    python build_preload.py --all
    python compile_cache.py
"""

import csv
import hashlib
import io
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime as dt_class
from core.ike import compute_ike_from_snapshot
from models.hurricane import HurricaneSnapshot

CACHE_DIR = Path(__file__).parent / "data" / "cache" / "ike"
CSV_PATH = Path(__file__).parent / "data" / "cache" / "ibtracs_all.csv"
IKE_CACHE_VERSION = "v2"
GRID_RES_KM = 15.0
SKIP_POINTS = 1


def _safe_float(val):
    """Convert IBTrACS CSV value to float, handling blanks and sentinels."""
    if val is None:
        return None
    val = str(val).strip()
    if val == "" or val == " ":
        return None
    try:
        f = float(val)
        if f < -900:  # IBTrACS sentinel for missing
            return None
        return f
    except (ValueError, TypeError):
        return None


def _ike_cache_key(storm_id: str) -> str:
    """Generate cache filename matching the API convention."""
    raw = f"{storm_id}_{GRID_RES_KM}_{SKIP_POINTS}_v2"
    h = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{storm_id}_{h}.json"


def is_cached(storm_id: str) -> bool:
    """Check if IKE cache already exists."""
    return len(list(CACHE_DIR.glob(f"{storm_id}_*.json"))) > 0


def estimate_rmw(vmax_ms, lat):
    """Estimate radius of maximum winds (meters) from Vmax and latitude."""
    if vmax_ms is None or vmax_ms <= 0:
        return 40 * 1852  # 40 nm default
    vmax_kt = vmax_ms / 0.514444
    lat_abs = abs(lat) if lat else 25
    # Knaff & Zehr (2007) simplified
    rmw_nm = 46.4 * (vmax_kt ** -0.33) * (1 + 0.006 * (lat_abs - 25))
    rmw_nm = max(5, min(150, rmw_nm))
    return rmw_nm * 1852  # to meters


def estimate_r34(vmax_ms, rmw_m, lat):
    """Estimate R34 (meters) when not available."""
    if vmax_ms is None or vmax_ms < 17.5:
        return None
    vmax_kt = vmax_ms / 0.514444
    rmw_nm = rmw_m / 1852
    # Simple scaling from Knaff et al.
    r34_nm = rmw_nm * (2.0 + 0.015 * (vmax_kt - 34))
    r34_nm = max(30, min(300, r34_nm))
    return r34_nm * 1852  # to meters


def parse_csv_into_storm_dict(csv_text: str, min_year=2015, max_year=2025):
    """
    Parse entire IBTrACS CSV once into a dict of ATCF_ID → list of raw row dicts.
    Only includes Atlantic basin (NA) storms in the year range.
    Returns: (atcf_to_sid, atcf_to_rows) where atcf_to_rows maps ATCF_ID → [rows].
    """
    # Group rows by SID first (efficient since CSV is ordered by SID)
    sid_to_rows = {}
    sid_to_atcf = {}
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        basin = (row.get("BASIN") or "").strip()
        if basin != "NA":
            continue
        season = (row.get("SEASON") or "").strip()
        try:
            year = int(season)
        except (ValueError, TypeError):
            continue
        if year < min_year or year > max_year:
            continue

        sid = (row.get("SID") or "").strip()
        if not sid:
            continue

        if sid not in sid_to_rows:
            sid_to_rows[sid] = []
        sid_to_rows[sid].append(row)

        atcf = (row.get("USA_ATCF_ID") or "").strip()
        if atcf and atcf.startswith("AL") and sid not in sid_to_atcf:
            sid_to_atcf[sid] = atcf

    # Build ATCF → rows mapping
    atcf_to_rows = {}
    atcf_to_sid = {}
    for sid, rows in sid_to_rows.items():
        atcf = sid_to_atcf.get(sid)
        if atcf:
            atcf_to_rows[atcf] = rows
            atcf_to_sid[atcf] = sid

    return atcf_to_sid, atcf_to_rows


def rows_to_snapshots(sid: str, rows: list) -> list:
    """Convert raw CSV rows into HurricaneSnapshot objects."""
    NM = 1852.0
    snapshots = []

    for row in rows:
        timestamp = (row.get("ISO_TIME") or "").strip()
        if not timestamp:
            continue

        lat = _safe_float(row.get("USA_LAT") or row.get("LAT"))
        lon = _safe_float(row.get("USA_LON") or row.get("LON"))
        if lat is None or lon is None:
            continue

        wind_kt = _safe_float(row.get("USA_WIND"))
        if wind_kt is None:
            wind_kt = _safe_float(row.get("WMO_WIND"))
        wind_ms = wind_kt * 0.514444 if wind_kt else 0

        pres = _safe_float(row.get("USA_PRES"))
        if pres is None:
            pres = _safe_float(row.get("WMO_PRES"))
        if pres is None:
            pres = 1013

        r34_ne = _safe_float(row.get("USA_R34_NE"))
        r34_se = _safe_float(row.get("USA_R34_SE"))
        r34_sw = _safe_float(row.get("USA_R34_SW"))
        r34_nw = _safe_float(row.get("USA_R34_NW"))

        r50_ne = _safe_float(row.get("USA_R50_NE"))
        r50_se = _safe_float(row.get("USA_R50_SE"))
        r50_sw = _safe_float(row.get("USA_R50_SW"))
        r50_nw = _safe_float(row.get("USA_R50_NW"))

        r64_ne = _safe_float(row.get("USA_R64_NE"))
        r64_se = _safe_float(row.get("USA_R64_SE"))
        r64_sw = _safe_float(row.get("USA_R64_SW"))
        r64_nw = _safe_float(row.get("USA_R64_NW"))

        rmw_nm = _safe_float(row.get("USA_RMW"))

        r34_quads = None
        if any(v is not None for v in [r34_ne, r34_se, r34_sw, r34_nw]):
            r34_quads = {
                "NE": (r34_ne or 0) * NM, "SE": (r34_se or 0) * NM,
                "SW": (r34_sw or 0) * NM, "NW": (r34_nw or 0) * NM,
            }

        r50_quads = None
        if any(v is not None for v in [r50_ne, r50_se, r50_sw, r50_nw]):
            r50_quads = {
                "NE": (r50_ne or 0) * NM, "SE": (r50_se or 0) * NM,
                "SW": (r50_sw or 0) * NM, "NW": (r50_nw or 0) * NM,
            }

        r64_quads = None
        if any(v is not None for v in [r64_ne, r64_se, r64_sw, r64_nw]):
            r64_quads = {
                "NE": (r64_ne or 0) * NM, "SE": (r64_se or 0) * NM,
                "SW": (r64_sw or 0) * NM, "NW": (r64_nw or 0) * NM,
            }

        rmw_m = rmw_nm * NM if rmw_nm else None
        if rmw_m is None:
            rmw_m = estimate_rmw(wind_ms, lat)

        r34_avg_m = None
        if r34_quads:
            vals = [v for v in r34_quads.values() if v > 0]
            r34_avg_m = sum(vals) / len(vals) if vals else None
        if r34_avg_m is None and wind_ms >= 17.5:
            r34_avg_m = estimate_r34(wind_ms, rmw_m, lat)

        storm_speed = _safe_float(row.get("STORM_SPEED"))
        storm_dir = _safe_float(row.get("STORM_DIR"))

        try:
            ts_dt = dt_class.fromisoformat(timestamp.replace(" ", "T"))
        except ValueError:
            continue

        storm_name = (row.get("NAME") or "UNNAMED").strip()

        snap = HurricaneSnapshot(
            storm_id=sid, name=storm_name, timestamp=ts_dt,
            lat=lat, lon=lon, max_wind_ms=wind_ms, min_pressure_hpa=pres,
            rmw_m=rmw_m, r34_m=r34_avg_m, r50_m=None, r64_m=None,
            r34_quadrants_m=r34_quads, r50_quadrants_m=r50_quads,
            r64_quadrants_m=r64_quads,
            forward_speed_ms=storm_speed, forward_direction_deg=storm_dir,
        )
        snapshots.append(snap)

    return snapshots


def compute_storm_ike(storm_id: str, sid: str, rows: list) -> tuple:
    """Compute IKE for a single storm from pre-parsed rows. Returns (storm_id, success, message, results)."""
    t0 = time.time()

    snapshots = rows_to_snapshots(sid, rows)

    if not snapshots:
        return (storm_id, False, f"SID {sid} — no valid snapshots from {len(rows)} rows", [])

    # Skip points (every other) for efficiency, matching API behavior
    if SKIP_POINTS > 0:
        snapshots = snapshots[::SKIP_POINTS + 1]

    results = []
    grid_res_m = GRID_RES_KM * 1000

    for snap in snapshots:
        try:
            ike_result = compute_ike_from_snapshot(snap, grid_resolution_m=grid_res_m)

            # Convert to cache format
            ike_tj = round(ike_result.ike_total_tj, 2) if ike_result.ike_total_tj else 0
            ike_hur = round(ike_result.ike_hurricane_tj, 2) if ike_result.ike_hurricane_tj else 0
            ike_ts = round(ike_result.ike_tropical_storm_tj, 2) if ike_result.ike_tropical_storm_tj else 0

            # Pretty string
            if ike_tj >= 1000:
                pretty = f"{ike_tj / 1000:.1f} PJ"
            else:
                pretty = f"{ike_tj:.1f} TJ"

            # R34 average for display
            r34_nm = None
            if snap.r34_m:
                r34_nm = round(snap.r34_m / 1852, 1)
            elif snap.r34_quadrants_m:
                vals = [v for v in snap.r34_quadrants_m.values() if v and v > 0]
                if vals:
                    r34_nm = round(sum(vals) / len(vals) / 1852, 1)

            r64_nm = None
            if snap.r64_quadrants_m:
                vals = [v for v in snap.r64_quadrants_m.values() if v and v > 0]
                if vals:
                    r64_nm = round(sum(vals) / len(vals) / 1852, 1)

            def _quads_nm(qmap):
                if not qmap:
                    return None
                return {k: round(v / 1852, 1) for k, v in qmap.items() if v}

            r34_quads_nm = _quads_nm(snap.r34_quadrants_m)
            r50_quads_nm = _quads_nm(snap.r50_quadrants_m)
            r64_quads_nm = _quads_nm(snap.r64_quadrants_m)

            result = {
                "storm_id": snap.storm_id,
                "timestamp": snap.timestamp.isoformat() if hasattr(snap.timestamp, 'isoformat') else str(snap.timestamp),
                "ike_total_tj": ike_tj,
                "ike_hurricane_tj": ike_hur,
                "ike_tropical_storm_tj": ike_ts,
                "ike_pretty": pretty,
                "lat": round(snap.lat, 2),
                "lon": round(snap.lon, 2),
                "wind_field_source": ike_result.wind_field_source or "unknown",
                "max_wind_ms": round(snap.max_wind_ms, 1) if snap.max_wind_ms else 0,
                "min_pressure_hpa": round(snap.min_pressure_hpa, 1) if snap.min_pressure_hpa else 1013,
                "rmw_nm": round(snap.rmw_m / 1852, 1) if snap.rmw_m else None,
                "r34_nm": r34_nm,
                "r64_nm": r64_nm,
                "r34_quadrants": r34_quads_nm,
                "r50_quadrants": r50_quads_nm,
                "r64_quadrants": r64_quads_nm,
                "forward_speed_knots": round(snap.forward_speed_ms * 1.94384, 1) if snap.forward_speed_ms else None,
                "forward_direction_deg": round(snap.forward_direction_deg, 0) if snap.forward_direction_deg else None,
            }
            results.append(result)

        except Exception as e:
            # Skip bad snapshots
            continue

    if not results:
        return (storm_id, False, f"SID {target_sid}, {len(snapshots)} snapshots but all failed IKE", [])

    # Save cache file
    compute_ms = (time.time() - t0) * 1000
    fname = _ike_cache_key(storm_id)
    path = CACHE_DIR / fname

    payload = {
        "_version": IKE_CACHE_VERSION,
        "_storm_id": storm_id,
        "_source": "ibtracs",
        "_grid_res_km": GRID_RES_KM,
        "_skip_points": SKIP_POINTS,
        "_compute_ms": round(compute_ms, 1),
        "_cached_at": datetime.utcnow().isoformat(),
        "_obs_count": len(results),
        "results": results,
    }

    with open(path, "w") as f:
        json.dump(payload, f, default=str)

    return (storm_id, True, f"{len(results)} snapshots, {compute_ms:.0f}ms", results)


def scan_atlantic_atcf_ids(csv_text: str, min_year=2015, max_year=2025) -> list:
    """Scan IBTrACS CSV to find all unique ATCF IDs for Atlantic basin storms."""
    atcf_ids = set()
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        basin = (row.get("BASIN") or "").strip()
        if basin != "NA":
            continue
        season = (row.get("SEASON") or "").strip()
        if not season:
            continue
        try:
            year = int(season)
        except ValueError:
            continue
        if year < min_year or year > max_year:
            continue
        atcf = (row.get("USA_ATCF_ID") or "").strip()
        if atcf and atcf.startswith("AL"):
            atcf_ids.add(atcf)
    return sorted(atcf_ids)


def main():
    print("=" * 70)
    print("BATCH IKE COMPUTATION — All Atlantic Storms 2015-2025")
    print("=" * 70)

    # Load IBTrACS CSV
    print(f"\nLoading IBTrACS CSV: {CSV_PATH}")
    t0 = time.time()
    csv_text = CSV_PATH.read_text(encoding="utf-8", errors="replace")
    print(f"Loaded {len(csv_text) / 1e6:.1f} MB in {time.time() - t0:.1f}s")

    # Parse CSV once into memory (indexed by ATCF ID)
    print("Parsing CSV into storm index (one-time pass)...")
    t1 = time.time()
    atcf_to_sid, atcf_to_rows = parse_csv_into_storm_dict(csv_text)
    print(f"Indexed {len(atcf_to_rows)} Atlantic storms in {time.time() - t1:.1f}s")

    # Free CSV text to save memory
    del csv_text

    all_ids = sorted(atcf_to_rows.keys())
    print(f"\nFound {len(all_ids)} Atlantic storms (2015-2025)")

    # Check which are already cached
    to_compute = []
    already_cached = []
    for atcf_id in all_ids:
        if is_cached(atcf_id):
            already_cached.append(atcf_id)
        else:
            to_compute.append(atcf_id)

    print(f"Already cached: {len(already_cached)}")
    print(f"Need to compute: {len(to_compute)}")

    if not to_compute:
        print("\nAll storms already cached!")
        return

    # Process storms sequentially
    print(f"\nComputing IKE for {len(to_compute)} storms...\n")
    success = 0
    failed = []
    t_start = time.time()

    for i, atcf_id in enumerate(to_compute):
        sid = atcf_to_sid[atcf_id]
        rows = atcf_to_rows[atcf_id]
        storm_id, ok, msg, results = compute_storm_ike(atcf_id, sid, rows)
        status = "OK" if ok else "FAIL"
        pct = (i + 1) / len(to_compute) * 100
        print(f"  [{i+1}/{len(to_compute)} {pct:.0f}%] {storm_id}: {status} — {msg}")

        if ok:
            success += 1
        else:
            failed.append((storm_id, msg))

    elapsed = time.time() - t_start
    print(f"\n{'=' * 70}")
    print(f"Done in {elapsed:.1f}s ({elapsed / max(success, 1):.1f}s/storm)")
    print(f"Success: {success}/{len(to_compute)}")
    print(f"Total cached: {success + len(already_cached)}/{len(all_ids)}")

    if failed:
        print(f"\nFailed ({len(failed)}):")
        for sid, msg in sorted(failed):
            print(f"  {sid}: {msg}")


if __name__ == "__main__":
    os.chdir(Path(__file__).parent)
    main()
