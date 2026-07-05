"""WP DPS audit v2 (2026-07) — sandbox variants against the 8 baked WP storms.

Companion script for docs/audits/WP_DPS_AUDIT_V2.md. Read-only against live
code: variants are applied by monkeypatching module attributes at runtime and
restored between runs. Inputs are frontend/preload_bundle.json snapshots (the
same inputs the bake uses), so V0 must reproduce compiled_bundle.json exactly.

Variants:
  V0  current formula (parity check vs compiled_bundle.json)
  V1  WP cumulative tuning: DPI threshold 25->12, duration/breadth caps 0.10->0.15
  V2  sub-basin by hurricane-force fixes (intensity-gated tally, density fallback)
  V3  de-spike IKE: single-snapshot IKE spikes >1.6x both neighbors clamped
  V4  V1+V2+V3 combined

Ground truth: commonly cited damage/death totals (JTWC/JMA/NDRRMC/CMA press
figures; same convention as WP_DPS_AUDIT.md 2026-04-15).
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from core import cumulative_dpi as cd
import compile_cache as cc
from core.dps_engine import compute_storm_dps

PRE = json.loads((REPO / "frontend" / "preload_bundle.json").read_text(encoding="utf-8"))["storms"]
COMP = json.loads((REPO / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))["storms"]

# sid: (name, year, damage_$B, deaths)  — commonly cited totals
STORMS = {
    "2024244N09137": ("Yagi", 2024, 14.0, 844),
    "2025262N16133": ("Ragasa", 2025, 3.8, 30),
    "2024201N12133": ("Gaemi", 2024, 3.5, 150),
    "2015180N09160": ("Chan-Hom", 2015, 1.6, 7),
    "2025305N10138": ("Kalmaegi", 2025, 0.45, 266),
    "2024270N24128": ("Krathon", 2024, 0.31, 10),
    "2025308N10143": ("Fung-Wong", 2025, 0.30, 33),
    "2024298N13150": ("Kong-Rey", 2024, 0.18, 3),
}


def spearman(xs, ys):
    def rank(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r
    rx, ry = rank(xs), rank(ys)
    n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    den = (sum((x - mx) ** 2 for x in rx) * sum((y - my) ** 2 for y in ry)) ** 0.5
    return num / den if den else 0.0


def despike_ike(snaps):
    """Clamp single-snapshot IKE spikes: a value >1.6x BOTH neighbors is
    replaced by the neighbor mean (radii glitch, not a real wind-field
    doubling that vanishes 6h later)."""
    out = [dict(s) for s in snaps]
    for i in range(1, len(out) - 1):
        a = out[i - 1].get("ike_total_tj") or 0
        b = out[i].get("ike_total_tj") or 0
        c = out[i + 1].get("ike_total_tj") or 0
        if b > 40 and a > 0 and c > 0 and b > 1.6 * a and b > 1.6 * c:
            out[i]["ike_total_tj"] = round((a + c) / 2.0, 2)
            out[i]["_despiked_from"] = b
    return out


def wp_sub_basin_intensity_gated(snapshots):
    """Sub-basin from hurricane-force (>=33 m/s) fixes only; falls back to the
    current density tally when no HF fix lands in any box. Measures where the
    storm HIT, not where the track loitered."""
    hf = [s for s in snapshots
          if (s.get("max_wind_ms", 0) or 0) >= 33.0]
    if hf:
        counts = {}
        for s in hf:
            lat, lon = s.get("lat", 0), s.get("lon", 0)
            for key, lat_min, lat_max, lon_min, lon_max in _WP_REGION_BOXES:
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    counts[key] = counts.get(key, 0) + 1
        if counts:
            return max(counts, key=counts.get)
    return _ORIG_SUB_BASIN(snapshots)


# Snapshot of the live region boxes + original classifier for the patch
_WP_REGION_BOXES = [
    ("WP_MARIANA", 13.0, 20.5, 144.0, 146.5),
    ("WP_TAIWAN", 21.5, 25.5, 119.5, 122.5),
    ("WP_PHILIPPINES", 5.0, 20.0, 117.0, 127.0),
    ("WP_VIETNAM", 8.0, 22.0, 102.0, 112.0),
    ("WP_HAINAN", 17.5, 20.5, 108.0, 111.5),
    ("WP_SOUTH_CHINA", 20.0, 26.0, 108.0, 118.0),
    ("WP_NORTH_CHINA", 26.0, 41.0, 117.0, 124.5),
    ("WP_KOREA", 33.0, 39.0, 124.5, 131.5),
    ("WP_JAPAN", 24.0, 45.5, 128.0, 146.0),
]
_ORIG_SUB_BASIN = cc.determine_wp_sub_basin
_ORIG_TUNING = dict(cd._BASIN_CUM_TUNING)

WP_TUNING = {"threshold": 12.0, "duration_cap": 0.15, "breadth_cap": 0.15}


def run_variant(tag, *, tuning=False, subbasin=False, despike=False):
    if tuning:
        cd._BASIN_CUM_TUNING["WESTERN_PACIFIC"] = WP_TUNING
    if subbasin:
        cc.determine_wp_sub_basin = wp_sub_basin_intensity_gated
    results = {}
    try:
        for sid, (name, year, dmg, deaths) in STORMS.items():
            snaps = PRE[sid]
            if despike:
                snaps = despike_ike(snaps)
            b = compute_storm_dps(storm_id=sid, snapshots=snaps,
                                  storm_name=name, storm_year=year)
            results[sid] = b
    finally:
        cd._BASIN_CUM_TUNING.clear()
        cd._BASIN_CUM_TUNING.update(_ORIG_TUNING)
        cc.determine_wp_sub_basin = _ORIG_SUB_BASIN
    print(f"\n=== {tag} ===")
    order = sorted(STORMS, key=lambda s: -results[s]["dps"])
    for sid in order:
        name, year, dmg, deaths = STORMS[sid]
        b = results[sid]
        print(f"  {name:9s} {year}: dps={b['dps']:5.1f}  peak={b['peak_dps']:5.1f} "
              f"dur={b['duration_factor']:.3f} brd={b['breadth_factor']:.3f} "
              f"coast_h={b['coastal_hours']:5.1f}  [{b['adjustment_notes']}]  "
              f"(dmg ${dmg}B, {deaths} deaths)")
    dps = [results[s]["dps"] for s in STORMS]
    dmgs = [STORMS[s][2] for s in STORMS]
    dths = [STORMS[s][3] for s in STORMS]
    r_d, r_k = spearman(dps, dmgs), spearman(dps, dths)
    print(f"  Spearman rho vs damage: {r_d:+.3f}   vs deaths: {r_k:+.3f}")
    return results, r_d, r_k


# --- Diagnostic: locate the 196.3 IKE spikes -------------------------------
print("=== IKE spike diagnostic ===")
for sid, (name, *_rest) in STORMS.items():
    snaps = PRE[sid]
    for i, s in enumerate(snaps):
        ike = s.get("ike_total_tj") or 0
        if 195.0 <= ike <= 198.5:
            prev_i = (snaps[i - 1].get("ike_total_tj") or 0) if i else 0
            next_i = (snaps[i + 1].get("ike_total_tj") or 0) if i + 1 < len(snaps) else 0
            print(f"  {name}: snap#{i} t={s.get('timestamp')} ike={ike} "
                  f"(prev={prev_i}, next={next_i}) vmax={s.get('max_wind_ms')} "
                  f"r34={s.get('r34_nm')} rmw={s.get('rmw_nm')} "
                  f"quads={s.get('r34_quadrants')}")

# --- Variants ---------------------------------------------------------------
v0, d0, k0 = run_variant("V0 current (parity vs compiled_bundle)")
par = max(abs(v0[s]["dps"] - COMP[s]["dps"]) for s in STORMS)
print(f"  parity vs compiled_bundle: max |delta| = {par:.4f}")

v1, d1, k1 = run_variant("V1 WP cum tuning (thr 12, caps 0.15)", tuning=True)
v2, d2, k2 = run_variant("V2 sub-basin by HF fixes", subbasin=True)
v3, d3, k3 = run_variant("V3 IKE de-spike", despike=True)
v4, d4, k4 = run_variant("V4 combined (V1+V2+V3)", tuning=True, subbasin=True, despike=True)

print("\n=== Summary (Spearman rho) ===")
print(f"  {'variant':32s} {'vs damage':>10s} {'vs deaths':>10s}")
for tag, rd, rk in [("V0 current", d0, k0), ("V1 cum tuning", d1, k1),
                    ("V2 sub-basin HF", d2, k2), ("V3 IKE de-spike", d3, k3),
                    ("V4 combined", d4, k4)]:
    print(f"  {tag:32s} {rd:+10.3f} {rk:+10.3f}")

print("\n=== Per-storm current vs combined ===")
for sid in sorted(STORMS, key=lambda s: -v0[s]["dps"]):
    name = STORMS[sid][0]
    print(f"  {name:9s}: {v0[sid]['dps']:5.1f} -> {v4[sid]['dps']:5.1f}   "
          f"[{v4[sid]['adjustment_notes']}]")

# --- V5 HEADROOM PROBE: what if the surge/econ legs were alive over WP land?
# Substitute analog US region profiles for WP coastal snapshots. NOT a
# proposal to ship US profiles — an upper-bound demonstration of how much
# peak_dpi signal the dead legs are suppressing (rec #1: WP region profiles).
_WP_ANALOG = [
    # lat_min, lat_max, lon_min, lon_max, analog US region_key
    (21.0, 26.0, 119.0, 123.0, "atl_fl_east"),    # Taiwan ~ dense linear coast
    (5.0, 21.0, 117.0, 127.0, "carib_pr"),        # Philippines ~ vulnerable island arc
    (20.0, 26.0, 108.0, 118.0, "gulf_la"),        # S China / PRD ~ surge-prone delta metro
    (8.0, 22.0, 102.0, 112.0, "gulf_central_tx"), # Vietnam coast
    (24.0, 45.5, 128.0, 146.0, "atl_ne"),         # Japan ~ dense hardened coast
]
_ORIG_REGION_FN = cd._estimate_region_from_coords

def _region_with_wp_analogs(lat, lon):
    r = _ORIG_REGION_FN(lat, lon)
    if r is not None:
        return r
    for lat_min, lat_max, lon_min, lon_max, key in _WP_ANALOG:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return key
    return None

cd._estimate_region_from_coords = _region_with_wp_analogs
try:
    v5, d5, k5 = run_variant("V5 HEADROOM PROBE (WP analog region profiles)")
finally:
    cd._estimate_region_from_coords = _ORIG_REGION_FN
print("\n=== peak_dpi: current vs living-legs probe ===")
for sid in sorted(STORMS, key=lambda s: -v5[s]["dps"]):
    name = STORMS[sid][0]
    print(f"  {name:9s}: peak {v0[sid]['peak_dps']:5.1f} -> {v5[sid]['peak_dps']:5.1f}   "
          f"dps {v0[sid]['dps']:5.1f} -> {v5[sid]['dps']:5.1f}")
print(f"  probe rho: damage {d5:+.3f}, deaths {k5:+.3f}  (V0: {d0:+.3f}, {k0:+.3f})")
