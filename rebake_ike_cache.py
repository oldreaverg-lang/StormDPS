"""
Re-bake IKE for every cached storm in-place with the current engine (canonical
Powell & Reinhold (2007) regression via compute_ike_from_snapshot), and flag
each snapshot's extratropical status (`et`) from IBTrACS NATURE/USA_STATUS.

Each cache file in data/cache/ike/ stores the per-snapshot inputs (quadrant
radii, wind, lat/lon, RMW), so we reconstruct each snapshot and recompute IKE
(basin-uniform, no CSV re-parse needed for the physics). We additionally join
the IBTrACS CSV to tag each snapshot `et=True` when NATURE==ET / USA_STATUS==EX,
so downstream peak IKE/SDP can exclude the post-tropical phase — which otherwise
dominates the peak for recurving storms (e.g. Michael's mid-Atlantic ET tail,
IKE 155 TJ at 48 N vs ~23 TJ at its FL Cat-5 landfall).

Pipeline after this: build_preload.py --all -> compile_cache.py. Verify with
verify_rebake.py / the audit scripts.
"""
import csv
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
os.chdir(Path(__file__).parent)

from core.ike import compute_ike_from_snapshot
from core.ike_coaps import sdp_from_ike_ts
from models.hurricane import HurricaneSnapshot

CACHE_DIR = Path("data/cache/ike")
CSV_PATH = Path("data/cache/ibtracs_all.csv")
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


def _iso19(ts):
    return str(ts).replace("T", " ")[:19] if ts else ""


def _parse_ts(ts):
    if not isinstance(ts, str):
        return ts
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        return datetime(2000, 1, 1)


def recompute_one(r):
    if r.get("lat") is None or r.get("lon") is None:
        return None
    snap = HurricaneSnapshot(
        storm_id=r.get("storm_id", "REBAKE"), name="REBAKE",
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
    return compute_ike_from_snapshot(snap)


def build_et_lookup(needed):
    """{(id, iso19): is_extratropical} from IBTrACS NATURE/USA_STATUS for the
    given storm ids (keyed by both SID and ATCF id). Extratropical iff
    USA_STATUS == 'EX' or NATURE == 'ET'."""
    et = {}
    if not CSV_PATH.exists():
        print("  WARNING: IBTrACS CSV not found — no snapshots flagged extratropical")
        return et
    with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
        rdr = csv.reader(f)
        hdr = next(rdr)
        col = {c: hdr.index(c) for c in
               ["SID", "USA_ATCF_ID", "ISO_TIME", "NATURE", "USA_STATUS"] if c in hdr}
        for row in rdr:
            n = len(row)
            sid = row[col["SID"]].strip() if "SID" in col and col["SID"] < n else ""
            atcf = row[col["USA_ATCF_ID"]].strip() if "USA_ATCF_ID" in col and col["USA_ATCF_ID"] < n else ""
            if sid not in needed and atcf not in needed:
                continue
            iso = _iso19(row[col["ISO_TIME"]]) if "ISO_TIME" in col else ""
            nat = row[col["NATURE"]].strip() if "NATURE" in col and col["NATURE"] < n else ""
            sta = row[col["USA_STATUS"]].strip() if "USA_STATUS" in col and col["USA_STATUS"] < n else ""
            is_et = (sta == "EX") or (nat == "ET")
            if sid:
                et[(sid, iso)] = is_et
            if atcf:
                et[(atcf, iso)] = is_et
    return et


def main():
    files = sorted(CACHE_DIR.glob("*.json"))
    loaded, needed = [], set()
    for fp in files:
        atcf = fp.stem.rsplit("_", 1)[0]
        data = json.load(open(fp, encoding="utf-8"))
        loaded.append((fp, atcf, data))
        needed.add(atcf)
        for r in data.get("results", []):
            if r.get("storm_id"):
                needed.add(r["storm_id"])

    print(f"Re-baking {len(files)} cache files; building ET lookup from IBTrACS...")
    et_lookup = build_et_lookup(needed)
    print(f"  {len(et_lookup)} (id,time) status entries\n")

    n_changed = n_same = n_err = et_count = 0
    src_counter = Counter()
    preset_rows = []
    max_trop = (0.0, None)

    for fp, atcf, data in loaded:
        results = data.get("results", [])
        old_peak = max((r.get("ike_total_tj") or 0) for r in results) if results else 0.0
        for r in results:
            try:
                res = recompute_one(r)
                if res is not None:
                    r["ike_total_tj"] = round(res.ike_total_tj, 2)
                    r["ike_hurricane_tj"] = round(res.ike_hurricane_tj, 2)
                    r["ike_tropical_storm_tj"] = round(res.ike_tropical_storm_tj, 2)
                    r["ike_pretty"] = (f"{r['ike_total_tj']/1000:.1f} PJ"
                                       if r["ike_total_tj"] >= 1000 else f"{r['ike_total_tj']:.1f} TJ")
                    r["wind_field_source"] = res.wind_field_source
                    src_counter[res.wind_field_source] += 1
            except Exception:
                n_err += 1
            iso = _iso19(r.get("timestamp"))
            is_et = et_lookup.get((r.get("storm_id", ""), iso))
            if is_et is None:
                is_et = et_lookup.get((atcf, iso))
            r["et"] = bool(is_et)
            if r["et"]:
                et_count += 1

        def peak(trop_only):
            vals = [r.get("ike_total_tj") or 0 for r in results if (not trop_only or not r.get("et"))]
            return max(vals) if vals else 0.0
        new_all, new_trop = peak(False), peak(True)
        if new_trop > max_trop[0]:
            max_trop = (new_trop, atcf)
        if abs(new_all - old_peak) > 0.05:
            n_changed += 1
        else:
            n_same += 1
        data["_rebaked_at"] = datetime.utcnow().isoformat()
        json.dump(data, open(fp, "w", encoding="utf-8"), default=str)
        if atcf in PRESETS:
            preset_rows.append((atcf, PRESETS[atcf], new_all, new_trop))

    print("PRESETS — peak IKE_TS all-track vs tropical-only (gate effect):")
    print(f"  {'id':10} {'name':9} {'all':>8} {'tropical':>9} {'SDP_trop':>9}")
    for atcf, nm, pa, pt in sorted(preset_rows, key=lambda x: -x[3]):
        flag = "  <-- ET-inflated" if (pa - pt) > 10 else ""
        print(f"  {atcf:10} {nm:9} {pa:8.1f} {pt:9.1f} {sdp_from_ike_ts(pt):9.2f}{flag}")

    print(f"\nAcross all {len(files)} storms:")
    print(f"  IKE changed: {n_changed} | unchanged: {n_same} | errors: {n_err}")
    print(f"  extratropical snapshots flagged: {et_count}")
    print(f"  max tropical-phase peak IKE: {max_trop[0]:.0f} TJ ({max_trop[1]})")
    print(f"  source distribution: {dict(src_counter)}")


if __name__ == "__main__":
    main()
