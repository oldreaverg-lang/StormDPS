"""
Re-bake IKE for every cached storm in-place with the current engine
(canonical Powell & Reinhold (2007) regression via compute_ike_from_snapshot).

Each cache file in data/cache/ike/ already stores the per-snapshot inputs
(quadrant radii in nm, max wind, lat/lon, RMW, forward motion), so we
reconstruct each snapshot and recompute IKE — basin-uniform, no CSV re-parse.

Reports old->new peak IKE for presets + across-the-board change/source stats,
then leave build_preload.py --all and compile_cache.py to propagate to bundles.
"""
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
os.chdir(Path(__file__).parent)

from core.ike import compute_ike_from_snapshot
from models.hurricane import HurricaneSnapshot

CACHE_DIR = Path("data/cache/ike")
NM = 1852.0

PRESETS = {
    "AL122005": "Katrina", "AL092024": "Helene", "AL152017": "Maria",
    "AL112017": "Irma", "AL092022": "Ian", "AL142018": "Michael",
    "AL142024": "Milton", "AL182012": "Sandy", "AL092008": "Ike",
    "AL092017": "Harvey", "AL052019": "Dorian", "AL062018": "Florence",
    "AL102023": "Idalia", "AL022024": "Beryl",
}


def _quads_m(q):
    return {k: (v * NM) for k, v in q.items() if v} if q else None


def _parse_ts(ts):
    if not isinstance(ts, str):
        return ts
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return datetime(2000, 1, 1)


def recompute_one(r):
    """Recompute IKE for one cached snapshot dict; returns (new_ike_total, source) or None."""
    if r.get("lat") is None or r.get("lon") is None:
        return None
    snap = HurricaneSnapshot(
        storm_id=r.get("storm_id", "REBAKE"),
        name="REBAKE",
        timestamp=_parse_ts(r.get("timestamp")),
        lat=r["lat"], lon=r["lon"],
        max_wind_ms=r.get("max_wind_ms") or 0.0,
        min_pressure_hpa=r.get("min_pressure_hpa"),
        rmw_m=(r["rmw_nm"] * NM) if r.get("rmw_nm") else None,
        r34_m=(r["r34_nm"] * NM) if r.get("r34_nm") else None,
        r50_m=None, r64_m=None,
        r34_quadrants_m=_quads_m(r.get("r34_quadrants")),
        r50_quadrants_m=_quads_m(r.get("r50_quadrants")),
        r64_quadrants_m=_quads_m(r.get("r64_quadrants")),
        forward_speed_ms=(r["forward_speed_knots"] * 0.514444) if r.get("forward_speed_knots") else None,
        forward_direction_deg=r.get("forward_direction_deg"),
    )
    res = compute_ike_from_snapshot(snap)
    return res


def main():
    files = sorted(CACHE_DIR.glob("*.json"))
    print(f"Re-baking {len(files)} cache files...\n")
    preset_rows = []
    n_changed = n_same = n_err = 0
    src_counter = Counter()
    max_ike = (0.0, None)
    neg = []

    for fp in files:
        sid = fp.stem.rsplit("_", 1)[0]
        data = json.load(open(fp, encoding="utf-8"))
        results = data.get("results", [])
        old_peak = max((r.get("ike_total_tj") or 0) for r in results) if results else 0.0
        for r in results:
            try:
                res = recompute_one(r)
                if res is None:
                    continue
                r["ike_total_tj"] = round(res.ike_total_tj, 2)
                r["ike_hurricane_tj"] = round(res.ike_hurricane_tj, 2)
                r["ike_tropical_storm_tj"] = round(res.ike_tropical_storm_tj, 2)
                r["ike_pretty"] = (f"{r['ike_total_tj']/1000:.1f} PJ"
                                   if r["ike_total_tj"] >= 1000 else f"{r['ike_total_tj']:.1f} TJ")
                r["wind_field_source"] = res.wind_field_source
                src_counter[res.wind_field_source] += 1
                if r["ike_total_tj"] < 0:
                    neg.append((sid, r["ike_total_tj"]))
            except Exception as e:
                n_err += 1
        new_peak = max((r.get("ike_total_tj") or 0) for r in results) if results else 0.0
        if new_peak > max_ike[0]:
            max_ike = (new_peak, sid)
        if abs(new_peak - old_peak) > 0.05:
            n_changed += 1
        else:
            n_same += 1
        data["_rebaked_at"] = datetime.utcnow().isoformat()
        json.dump(data, open(fp, "w", encoding="utf-8"), default=str)
        if sid in PRESETS:
            srcs = {r.get("wind_field_source") for r in results}
            preset_rows.append((sid, PRESETS[sid], old_peak, new_peak, srcs))

    print("PRESETS (peak IKE_TS, TJ):")
    print(f"  {'id':10} {'name':9} {'old':>8} {'new':>8}  change   source")
    for sid, name, ov, nv, srcs in sorted(preset_rows, key=lambda x: -x[3]):
        pct = (nv - ov) / ov * 100 if ov else float("inf")
        srcstr = ",".join(sorted(s for s in srcs if s))
        print(f"  {sid:10} {name:9} {ov:8.1f} {nv:8.1f}  {pct:+6.0f}%   {srcstr}")

    print(f"\nAcross all {len(files)} storms:")
    print(f"  changed: {n_changed} | unchanged: {n_same} | snapshot errors: {n_err}")
    print(f"  max peak IKE: {max_ike[0]:.0f} TJ ({max_ike[1]})")
    print(f"  negative IKE snapshots: {len(neg)}")
    print(f"  wind_field_source distribution: {dict(src_counter)}")


if __name__ == "__main__":
    main()
