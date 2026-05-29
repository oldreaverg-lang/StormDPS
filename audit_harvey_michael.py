"""
Full audit: Harvey (AL092017) vs Michael (AL142018).

Dumps every bundle field side-by-side, then analyses each storm's per-snapshot
IKE cache to find WHERE/when IKE peaks (landfall vs post-landfall ET phase),
the wind/radii at that point, and the IKE+wind track. Ends with sanity flags.
"""
import json
import math
import glob
from pathlib import Path

ROOT = Path(__file__).parent
BUN = json.load(open(ROOT / "frontend" / "compiled_bundle.json", encoding="utf-8"))["storms"]
NM = 1852.0
IDS = {"AL092017": "Harvey 2017", "AL142018": "Michael 2018"}


def sdp_from_ike(ike):
    if not ike or ike <= 0:
        return 0.0
    r = math.sqrt(ike)
    return max(0.0, min(5.99, 0.676 + 0.43 * r - 0.0176 * (r - 6.5) ** 2))


def cat(kt):
    if not kt:
        return "?"
    for t, l in [(137, "5"), (113, "4"), (96, "3"), (83, "2"), (64, "1"), (34, "TS")]:
        if kt >= t:
            return l
    return "TD"


# ---- 1. Bundle summary side-by-side ----
fields = ["name", "year", "category", "category_lifetime", "peak_wind_kt", "min_pressure_hpa",
          "dps", "peak_dps", "dps_original", "basin_name",
          "peak_ike_tj", "snapshot_count", "track_hours", "coastal_hours",
          "us_landfall_count", "stall_hours", "stall_bonus", "duration_factor",
          "breadth_factor", "rain_inland_factor", "inland_pen_factor",
          "exposure_factor", "exposure_region", "perp_factor",
          "rainfall_level", "rainfall_est_mm", "rainfall_stall_hours", "rainfall_anomalous",
          "adjustment_notes"]
H, M = BUN["AL092017"], BUN["AL142018"]
print("=" * 78)
print(f"{'FIELD':22} {'HARVEY 2017':>26} {'MICHAEL 2018':>26}")
print("=" * 78)
for f in fields:
    hv, mv = H.get(f), M.get(f)
    fmt = lambda v: (f"{v:.2f}" if isinstance(v, float) else str(v))[:26]
    print(f"{f:22} {fmt(hv):>26} {fmt(mv):>26}")
print(f"{'SDP (from peak IKE)':22} {sdp_from_ike(H.get('peak_ike_tj') or 0):>26.2f} {sdp_from_ike(M.get('peak_ike_tj') or 0):>26.2f}")

# IAS / ERS may live in dpi_timeseries — pull peak per-snapshot if present
for label, key in [("peak IAS", "ias"), ("peak ERS", "ers")]:
    def peak(st):
        ts = st.get("dpi_timeseries") or []
        vals = []
        for s in ts:
            v = s.get(key)
            vals.append(v.get("score") if isinstance(v, dict) else (v or 0))
        return max(vals) if vals else None
    print(f"{label:22} {str(peak(H)):>26} {str(peak(M)):>26}")


# ---- 2. Per-snapshot IKE cache analysis ----
def analyze(atcf):
    fp = glob.glob(str(ROOT / "data" / "cache" / "ike" / f"{atcf}_*.json"))
    if not fp:
        return None
    d = json.load(open(fp[0], encoding="utf-8"))
    rs = [r for r in d.get("results", []) if r.get("ike_total_tj") is not None]
    if not rs:
        return None
    peak = max(rs, key=lambda r: r["ike_total_tj"])
    pw = max(rs, key=lambda r: r.get("max_wind_ms") or 0)

    def r34avg(r):
        q = r.get("r34_quadrants") or {}
        v = [x for x in q.values() if x]
        return round(sum(v) / len(v), 0) if v else None
    return {"n": len(rs), "peak": peak, "pw": pw, "r34_at_peak": r34avg(peak),
            "r64_at_peak": (r.get("r64_quadrants") if False else (peak.get("r64_quadrants"))),
            "first": rs[0], "last": rs[-1]}


print("\n" + "=" * 78)
print("PER-SNAPSHOT IKE CACHE — where does IKE peak?")
print("=" * 78)
for atcf, nm in IDS.items():
    a = analyze(atcf)
    if not a:
        print(f"{nm}: no cache"); continue
    pk, pw = a["peak"], a["pw"]
    print(f"\n{nm}  ({a['n']} snapshots)")
    print(f"  PEAK-IKE snapshot:  {pk.get('timestamp')}  ({pk.get('lat')},{pk.get('lon')})")
    print(f"     IKE={pk.get('ike_total_tj')}TJ  wind={pk.get('max_wind_ms')}m/s ({(pk.get('max_wind_ms') or 0)/0.514444:.0f}kt, Cat{cat((pk.get('max_wind_ms') or 0)/0.514444)})  "
          f"R34avg={a['r34_at_peak']}nm  R64q={pk.get('r64_quadrants')}  src={pk.get('wind_field_source')}")
    print(f"  PEAK-WIND snapshot: {pw.get('timestamp')}  wind={(pw.get('max_wind_ms') or 0)/0.514444:.0f}kt  IKE_here={pw.get('ike_total_tj')}TJ")
    same = pk.get('timestamp') == pw.get('timestamp')
    print(f"  -> IKE peak {'COINCIDES with' if same else 'DIFFERS from'} wind peak")
