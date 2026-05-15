"""
Duration / Stall / Coastal-Tracking Stacking Audit  (v2 — bundle-anchored)
==========================================================================

v2 fix vs v1: the original sandbox used hand-estimated peak_DPI proxies and
hand-estimated bonus magnitudes, then reported "Final" scores that were
PRE-compression — i.e. uncompressed sums that didn't match the displayed
values on the live site. v2 loads the actual production values straight
from frontend/compiled_bundle.json (peak_dps, duration_factor,
breadth_factor, exposure_factor, perp_factor, stall_bonus,
rain_inland_factor, inland_pen_factor) and applies the Stage-5 sqrt
compression at the end so "Final" matches what /storm/{id} displays.

The proposed compact-storm bonus is then evaluated against this corrected
baseline, with attention to how much of any additive bonus actually
survives the compression at each storm's score band.

Companion writeup: DURATION_STALL_COASTAL_AUDIT.md
"""
from __future__ import annotations
import json
import math
import os
from typing import Optional

BUNDLE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "frontend", "compiled_bundle.json"
)

# Stage-5 sqrt compression params, copied from compile_cache.py (v7-audit / v9):
T_COMPRESS = 70.0
S_COMPRESS = 2.5
HARD_CAP   = 99.0


def compress(adjusted_dps: float) -> float:
    """Apply the Stage-5 sqrt compression + hard cap. Mirrors apply_basin_dps_adjustment."""
    if adjusted_dps > T_COMPRESS:
        adjusted_dps = T_COMPRESS + S_COMPRESS * math.sqrt(adjusted_dps - T_COMPRESS)
    return min(adjusted_dps, HARD_CAP)


def decompress(displayed: float) -> float:
    """Invert sqrt compression — what pre-compression value yields this displayed?"""
    if displayed <= T_COMPRESS:
        return displayed
    return T_COMPRESS + ((displayed - T_COMPRESS) / S_COMPRESS) ** 2


# Storms to audit. ATCF IDs as keys in compiled_bundle.json.
# bucket label is mine; "fast" = perpendicular fast landfall (compact-Cat-5
# archetype); "stall" = slow-moving / coast-grinding; "tracker" = large
# wind-field coast-tracker; "outlier" = doesn't fit cleanly.
STORM_IDS = [
    ("AL122005", "stall",   "Katrina"),    # giant slow surge event
    ("AL092008", "tracker", "Ike"),        # huge low-intensity slow tracker
    ("AL182012", "tracker", "Sandy"),      # huge slow NE corridor
    ("AL112017", "tracker", "Irma"),       # FL east coast parallel
    ("AL152017", "fast",    "Maria"),      # compact Cat 5 PR (vulnerability)
    ("AL092017", "stall",   "Harvey"),     # 5-day TX stall
    ("AL142018", "fast",    "Michael"),    # compact Cat 5 FL Panhandle
    ("AL062018", "stall",   "Florence"),   # slow NC grind
    ("AL112019", "stall",   "Imelda"),     # TS-strength TX flood-stall
    ("AL052019", "stall",   "Dorian"),     # Bahamas stall + intense
    ("AL092022", "fast",    "Ian"),        # compact-ish Cat 4 SW FL
    ("AL092024", "fast",    "Helene"),     # fast Big Bend Cat 4
    ("AL142024", "fast",    "Milton"),     # compact Cat 5 SW FL
]


def load_bundle() -> dict:
    with open(BUNDLE_PATH, "r", encoding="utf-8") as f:
        return json.load(f).get("storms", {})


def evaluate(storm_id: str, bucket: str, bundle: dict) -> Optional[dict]:
    s = bundle.get(storm_id)
    if not s:
        return None

    peak = float(s.get("peak_dps", 0))
    dur  = float(s.get("duration_factor", 0))
    brd  = float(s.get("breadth_factor", 0))
    exp_f = float(s.get("exposure_factor",  0))
    perp  = float(s.get("perp_factor",      0))
    stall = float(s.get("stall_bonus",      0))
    rain  = float(s.get("rain_inland_factor", 0))
    inland = float(s.get("inland_pen_factor", 0))

    cum_dpi = peak * (1.0 + dur + brd)
    combined_boost = exp_f + perp + stall + rain + inland
    boosted = peak * ((cum_dpi / peak if peak > 0 else 1.0) + combined_boost) \
              if combined_boost > 0 else cum_dpi
    displayed_recompute = compress(boosted)

    # Per-bonus pre-compression contribution (in peak-scaled points)
    dur_pts   = peak * dur
    brd_pts   = peak * brd
    cb_pts    = peak * combined_boost
    bonus_total_precomp = dur_pts + brd_pts + cb_pts

    # How much of those bonus points survives compression at this storm's band?
    # Displayed with bonus minus displayed without any bonus.
    displayed_no_bonus = compress(peak)
    displayed_with_bonus = displayed_recompute
    bonus_survives_displayed = displayed_with_bonus - displayed_no_bonus

    return {
        "id":         storm_id,
        "name":       s.get("name", storm_id),
        "year":       s.get("year"),
        "bucket":     bucket,
        "peak_dps":   peak,
        "cum_dpi":    cum_dpi,
        "boosted":    boosted,
        "displayed_bundle":    float(s.get("dps", 0)),
        "displayed_recompute": displayed_recompute,
        "dur_pts":    dur_pts,
        "brd_pts":    brd_pts,
        "cb_pts":     cb_pts,
        "bonus_pts_precomp":      bonus_total_precomp,
        "bonus_survives_displayed": bonus_survives_displayed,
        "peak_wind_ms": float(s.get("peak_wind_ms", 0)),
        "peak_ike_tj":  float(s.get("peak_ike_tj", 0)),
        "coastal_hours": float(s.get("coastal_hours", 0)),
        "stall_hours":   float(s.get("stall_hours", 0)),
    }


def proposed_compact_bonus(r: dict, exposure_norm_estimate: float) -> float:
    """≤ 10 pts pre-compression. Same shape as v1. exposure_norm comes from
    a hand-estimate because economic_score isn't in the bundle. Caller
    passes it.

    Gates:
      peak_wind > 55 m/s     (≥ Cat 3)
      exposure_norm > 0      (rural fails)
    """
    vmax = r["peak_wind_ms"]
    if vmax <= 55.0:
        return 0.0
    if exposure_norm_estimate <= 0.0:
        return 0.0
    intensity_frac = min(1.0, vmax / 75.0) ** 2
    expected_ike   = intensity_frac * 100.0
    ike_deficit    = max(0.0, expected_ike - r["peak_ike_tj"])
    deficit_norm   = min(1.0, ike_deficit / 50.0)
    intense_norm   = min(1.0, max(0.0, (vmax - 55.0) / 20.0))
    return min(10.0, deficit_norm * intense_norm * exposure_norm_estimate * 16.0)


# Hand-estimated landfall exposure normalization, 0..1. Sourced from
# COASTAL_EXPOSURE_WEIGHTS at the landfall region. PR is the canonical
# "high vulnerability, lower asset value" case that complicates the
# compact-storm bonus design.
EXPOSURE_NORM = {
    "Katrina":  0.90,  # New Orleans + LA / MS
    "Ike":      0.85,  # Houston / Galveston
    "Sandy":    0.95,  # NE corridor
    "Irma":     0.90,  # SE Florida
    "Maria":    0.40,  # PR — low asset, high vulnerability (not captured here)
    "Harvey":   0.85,  # Houston
    "Michael":  0.50,  # FL Panhandle rural-ish
    "Florence": 0.55,  # NC Carolinas
    "Imelda":   0.80,  # TX
    "Dorian":   0.20,  # Bahamas
    "Ian":      0.75,  # SW Florida
    "Helene":   0.45,  # Big Bend FL — very rural at coast
    "Milton":   0.75,  # SW Florida
}


def spearman(a: list[float], b: list[float]) -> float:
    n = len(a)
    if n < 2:
        return 0.0
    def ranks(xs):
        order = sorted(range(n), key=lambda i: -xs[i])
        r = [0]*n
        for rank, idx in enumerate(order, 1):
            r[idx] = rank
        return r
    ra, rb = ranks(a), ranks(b)
    d_sq = sum((ra[i]-rb[i])**2 for i in range(n))
    return 1.0 - (6.0 * d_sq) / (n * (n*n - 1))


# Published damage / fatalities for correlation, 2024 USD billions.
GROUND_TRUTH = {
    "Katrina":  (200.0, 1392),
    "Ike":      ( 50.0,  195),
    "Sandy":    ( 90.0,  233),
    "Irma":     ( 80.0,  134),
    "Maria":    (115.0, 2975),
    "Harvey":   (160.0,  107),
    "Michael":  ( 32.0,   16),
    "Florence": ( 29.0,   51),
    "Imelda":   (  5.0,    5),
    "Dorian":   (  5.0,   74),
    "Ian":      (119.6,  157),
    "Helene":   ( 78.7,  241),
    "Milton":   ( 34.3,   24),
}


def main():
    bundle = load_bundle()
    results = []
    for sid, bucket, _ in STORM_IDS:
        r = evaluate(sid, bucket, bundle)
        if r is None:
            print(f"  WARNING: {sid} not in bundle, skipping")
            continue
        results.append(r)

    # ─── Pre-flight sanity: do we reproduce the bundle's displayed dps? ──
    print("=" * 110)
    print("  v2 BUNDLE-ANCHORED AUDIT — sanity check (recompute vs bundle)")
    print("=" * 110)
    print(f"\n  {'Storm':<10} {'Peak':>5} {'Cum':>6} {'Boost':>6} "
          f"{'Recompute':>10} {'Bundle':>8}  {'Diff':>6}")
    print("  " + "-" * 70)
    for r in results:
        diff = r["displayed_recompute"] - r["displayed_bundle"]
        print(f"  {r['name']:<10} {r['peak_dps']:>5.1f} "
              f"{r['cum_dpi']:>6.1f} {r['boosted']:>6.1f} "
              f"{r['displayed_recompute']:>10.2f} {r['displayed_bundle']:>8.2f}  "
              f"{diff:>+6.2f}")

    # ─── Decomposition: how much of each storm's score is each bonus? ──
    print("\n" + "=" * 110)
    print("  BONUS DECOMPOSITION (in pre-compression points on peak)")
    print("=" * 110)
    print(f"\n  {'Storm':<10} {'Bucket':<8} {'Dmg$B':>6} {'Peak':>5} "
          f"{'DurPt':>6} {'BrdPt':>6} {'Stg3Pt':>7} {'BonTot':>7}  "
          f"{'NoBon':>6} {'WithBon':>8} {'BonDisp':>7}")
    print("  " + "-" * 100)
    for r in sorted(results, key=lambda x: -x["peak_dps"]):
        dmg = GROUND_TRUTH.get(r["name"], (0,0))[0]
        # "no bonus" path: just compress peak
        no_bon = compress(r["peak_dps"])
        print(f"  {r['name']:<10} {r['bucket']:<8} {dmg:>6.1f} {r['peak_dps']:>5.1f} "
              f"{r['dur_pts']:>6.1f} {r['brd_pts']:>6.1f} {r['cb_pts']:>7.1f} "
              f"{r['bonus_pts_precomp']:>7.1f}  "
              f"{no_bon:>6.1f} {r['displayed_recompute']:>8.1f} "
              f"{r['bonus_survives_displayed']:>+7.1f}")

    # ─── Compression drag analysis ──────────────────────────────────────
    print("\n" + "=" * 110)
    print("  COMPRESSION DRAG — how many displayed points does each pre-comp point buy?")
    print("=" * 110)
    print("\n  At T=70, S=2.5: derivative of compress(x) is 2.5/(2·sqrt(x-70)) for x>70.")
    print("  A storm at pre-comp 100 gets ~0.456 displayed per pre-comp point.")
    print("  A storm at pre-comp 120 gets ~0.354 displayed per pre-comp point.")
    print("  A storm at pre-comp 80 gets ~0.395 displayed per pre-comp point  (close to peak).")
    print("  Below pre-comp 70 the multiplier is exactly 1.0.")
    print()
    print(f"  {'Storm':<10} {'Boosted':>8} {'+1pt buys':>10}  Δ(displayed) for +10 pre-comp")
    print("  " + "-" * 75)
    for r in sorted(results, key=lambda x: -x["boosted"]):
        if r["boosted"] <= T_COMPRESS:
            local_slope = 1.0
        else:
            local_slope = S_COMPRESS / (2.0 * math.sqrt(r["boosted"] - T_COMPRESS))
        plus_10 = compress(r["boosted"] + 10.0) - r["displayed_recompute"]
        print(f"  {r['name']:<10} {r['boosted']:>8.1f} {local_slope:>10.3f}      +{plus_10:>4.1f} displayed")

    # ─── Proposed compact-storm bonus, applied PRE-compression ─────────
    print("\n" + "=" * 110)
    print("  PROPOSED COMPACT-STORM BONUS — applied PRE-compression")
    print("=" * 110)
    print()
    print(f"  {'Storm':<10} {'Bucket':<8} {'Dmg$B':>6} {'Vmax':>5} {'IKE':>5} "
          f"{'ExpN':>5} {'CompBon':>8}  {'BEFORE':>7} -> {'AFTER':>6}  {'Δ':>5}")
    print("  " + "-" * 100)
    for r in sorted(results, key=lambda x: -GROUND_TRUTH.get(x["name"], (0,0))[0]):
        dmg = GROUND_TRUTH.get(r["name"], (0,0))[0]
        exp_norm = EXPOSURE_NORM.get(r["name"], 0.5)
        cb = proposed_compact_bonus(r, exp_norm)
        after = compress(r["boosted"] + cb)
        delta = after - r["displayed_recompute"]
        mark = "" if abs(delta) < 0.05 else f"  +{delta:.1f}"
        print(f"  {r['name']:<10} {r['bucket']:<8} {dmg:>6.1f} "
              f"{r['peak_wind_ms']:>5.1f} {r['peak_ike_tj']:>5.0f} "
              f"{exp_norm:>5.2f} {cb:>8.1f}  "
              f"{r['displayed_recompute']:>7.2f} -> {after:>6.2f}{mark}")

    # ─── Correlations before vs after proposed bonus ────────────────────
    print("\n" + "=" * 110)
    print("  RANK CORRELATION (Spearman rho)")
    print("=" * 110)
    damages = [GROUND_TRUTH.get(r["name"], (0,0))[0] for r in results]
    deaths  = [GROUND_TRUTH.get(r["name"], (0,0))[1] for r in results]
    before_disp = [r["displayed_recompute"] for r in results]
    after_disp  = [
        compress(r["boosted"] + proposed_compact_bonus(r, EXPOSURE_NORM.get(r["name"], 0.5)))
        for r in results
    ]
    peaks_only  = [compress(r["peak_dps"]) for r in results]
    print()
    print(f"  rho(damage, peak alone compressed)         = {spearman(damages, peaks_only):>+.3f}")
    print(f"  rho(damage, BEFORE proposed bonus)         = {spearman(damages, before_disp):>+.3f}")
    print(f"  rho(damage, AFTER  proposed bonus)         = {spearman(damages, after_disp):>+.3f}")
    print()
    print(f"  rho(deaths, peak alone compressed)         = {spearman(deaths,  peaks_only):>+.3f}")
    print(f"  rho(deaths, BEFORE proposed bonus)         = {spearman(deaths,  before_disp):>+.3f}")
    print(f"  rho(deaths, AFTER  proposed bonus)         = {spearman(deaths,  after_disp):>+.3f}")

    # ─── Key per-storm comparisons users will ask about ────────────────
    print("\n" + "=" * 110)
    print("  KEY STORM COMPARISONS")
    print("=" * 110)
    by_name = {r["name"]: r for r in results}
    def row(name):
        r = by_name.get(name)
        if not r:
            return None
        dmg, dth = GROUND_TRUTH.get(name, (0,0))
        cb = proposed_compact_bonus(r, EXPOSURE_NORM.get(name, 0.5))
        after = compress(r["boosted"] + cb)
        return (name, dmg, dth, r["peak_dps"], r["displayed_recompute"], cb, after)

    print()
    print(f"  {'Storm':<10} {'$B':>6} {'Deaths':>7}  {'Peak':>5} {'Displayed':>10} "
          f"{'+CompBon':>9}  {'After':>6}")
    print("  " + "-" * 70)
    for name in ("Katrina","Maria","Harvey","Ike","Sandy","Irma","Ian",
                 "Michael","Helene","Milton","Florence","Dorian","Imelda"):
        rr = row(name)
        if rr is None: continue
        n, d, k, p, b, cb, a = rr
        print(f"  {n:<10} {d:>6.1f} {k:>7}  {p:>5.1f} {b:>10.2f} {cb:>+9.2f}  {a:>6.2f}")

    print()


if __name__ == "__main__":
    main()
