"""
Validate IKE methodology against Powell & Reinhold (2007) published reference
values (Table 1) — the ground truth the operational regressions were fit to.

Per storm we compare three columns:
  pub   = published reference (P&R 2007 Table 1, from gridded H*Wind analyses)
  coaps = canonical COAPS regression (core.ike_coaps) — the formula the
          calculator uses
  code  = current production path (core.ike.compute_ike_from_quadrants)

Inputs are taken VERBATIM from P&R 2007 Table 1 (Vms in m/s; Rmax, R18/R26/R33
in km, quadrant-average), so the comparison uses the paper's own ground-truth
wind structure rather than our data pipeline. R18/R26/R33 = radii of 34/50/64-kt
winds. The current code wants per-quadrant radii in METERS, so we apply each
quadrant-average symmetrically to all four quadrants.

Run:  venv\\Scripts\\python.exe validate_ike_coaps.py
"""
from core.ike import compute_ike_from_quadrants
from core.ike_coaps import compute_ike_coaps

QUADS = ("NE", "SE", "SW", "NW")

# (name, Vms_ms, Rmax_km, R18_km, R26_km, R33_km, pub_IKE_TS, pub_IKE_H, pub_SDP)
TABLE1 = [
    ("Charley 2004 LF",   63,  7, 156,  81,  40,  11,  2, 1.9),
    ("Andrew 1992 LF",    68, 19, 191, 142,  77,  20,  7, 2.5),
    ("Camille 1969 LF",   65, 15, 230, 163, 109,  63, 31, 4.0),
    ("Frances 2004 LF",   46, 52, 319, 217, 139,  94, 29, 4.7),
    ("Wilma 2005 Mexico", 59, 20, 394, 220, 121, 121, 28, 5.1),
    ("Katrina 2005 LA",   52, 65, 454, 311, 217, 122, 49, 5.1),
    ("Katrina 2005 peak", 71, 26, 349, 218, 139, 124, 45, 5.1),
    ("Isabel 2003 LF",    47, 87, 532, 322, 214, 174, 42, 5.6),
]


def quads_km(r_km):
    return {q: r_km for q in QUADS} if r_km and r_km > 0 else None


def quads_m(r_km):
    return {q: r_km * 1000.0 for q in QUADS} if r_km and r_km > 0 else None


def pct_err(est, ref):
    return abs(est - ref) / ref * 100.0 if ref else float("nan")


print()
print(f"{'Storm':18} | {'IKE_TS (TJ)':^25} | {'IKE_H (TJ)':^21} | {'SDP':^17}")
print(f"{'':18} | {'pub':>5} {'coaps':>6} {'code':>6} {'err%':>4} | "
      f"{'pub':>4} {'coaps':>5} {'code':>5} | {'pub':>4} {'coaps':>5} {'A8':>4}")
print("-" * 92)

coaps_errs, code_errs = [], []
for name, vms, rmax, r18, r26, r33, p_ts, p_h, p_sdp in TABLE1:
    c = compute_ike_coaps(vms, quads_km(r18), quads_km(r26), quads_km(r33), rmax)
    code_total, code_hur, _ = compute_ike_from_quadrants(
        vmax_ms=vms,
        r34_quadrants_m=quads_m(r18),
        r50_quadrants_m=quads_m(r26),
        r64_quadrants_m=quads_m(r33),
    )
    e_coaps = pct_err(c["ike_ts_tj"], p_ts)
    e_code = pct_err(code_total, p_ts)
    coaps_errs.append(e_coaps)
    code_errs.append(e_code)
    print(f"{name:18} | {p_ts:5.0f} {c['ike_ts_tj']:6.0f} {code_total:6.0f} "
          f"{e_coaps:3.0f}% | {p_h:4.0f} {c['ike_h_tj']:5.0f} {code_hur:5.0f} | "
          f"{p_sdp:4.1f} {c['sdp']:5.2f} {c['sdp_radii']:4.1f}")

print("-" * 92)
n = len(TABLE1)
print(f"\nMean abs error on headline IKE_TS vs published reference:")
print(f"  canonical COAPS regression : {sum(coaps_errs)/n:5.1f}%")
print(f"  current band code (w/ fudges): {sum(code_errs)/n:5.1f}%")


# --- Basin routing test (exercises the new compute_ike_from_snapshot plumbing) ---
from types import SimpleNamespace
from core.ike import _ike_result_from_quadrants, _is_atlantic_basin


def _snap(lat, lon, vms=52.0, rmw_m=65000.0):
    return SimpleNamespace(lat=lat, lon=lon, max_wind_ms=vms, rmw_m=rmw_m,
                           storm_id="TEST", timestamp="2005-08-29T12:00:00Z")


# Katrina-LA wind structure (R18=454, R26=311, R33=217 km) placed in two basins.
# Both now use the regression; non-Atlantic is tagged 'coaps_extrabasin'.
r34, r50, r64 = quads_m(454), quads_m(311), quads_m(217)
print("\nBasin routing (identical Katrina-LA wind structure in two basins):")
for label, lat, lon in [("Atlantic  (29N, 89.6W)", 29.0, -89.6),
                        ("W-Pacific (20N, 135E) ", 20.0, 135.0)]:
    res = _ike_result_from_quadrants(_snap(lat, lon), r34, r50, r64, "noaa_quadrant")
    print(f"  {label}: atlantic={str(_is_atlantic_basin(lat, lon)):5} "
          f"source={res.wind_field_source:26} IKE_TS={res.ike_total_tj:6.0f}  "
          f"IKE_H={res.ike_hurricane_tj:5.0f}")
print("  (published Katrina-LA reference: IKE_TS=122, IKE_H=49 TJ)")
