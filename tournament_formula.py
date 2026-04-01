"""
DPI Formula Architecture Tournament
====================================

Compares three formula architectures across the top economic-impact storms
of the last ~15 years (2010-2024):

  A) CURRENT:     DPI = 0.30×IKE + 0.35×Surge/Rain + 0.35×Economic  (additive)
  B) ECON_MULT:   DPI = (0.45×IKE + 0.55×Surge/Rain) × econ_multiplier
  C) FULL_MULT:   DPI = IKE_score × env_multiplier × econ_multiplier

For B and C, we sweep multiplier scales from 1.0× to 2.0× in 0.1 steps
to find where diminishing returns or over-exaggeration begins.

Validation metric: rank-order correlation with actual economic damage (USD).
A good formula should rank storms by destructive potential in roughly the
same order as their actual damage, without compressing or inflating the spread.

Sources for damage figures: NOAA NCEI Billion-Dollar Disasters, NHC TCRs,
FEMA SHELDUS, EM-DAT.
"""

import sys, os, math
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.dpi import compute_dpi_simple
from dataclasses import dataclass
from typing import List, Tuple, Optional

# ============================================================================
#  TOP ECONOMIC IMPACT STORMS 2005-2024
# ============================================================================
# Ranked by CPI-adjusted total damage (2024 USD, billions).
# Sources: NOAA NCEI Billion-Dollar Disasters (CPI-adjusted), NHC TCRs,
#          Britannica costliest hurricanes list, FEMA SHELDUS.
# We include every >$5B Atlantic storm for discrimination.

TOURNAMENT_STORMS = [
    {
        "name": "Katrina",
        "year": 2005,
        "region_key": "gulf_la",
        "vmax_ms": 56.6, "min_pressure_hpa": 920,
        "lat": 29.3, "lon": -89.6,
        "forward_speed_ms": 5.8, "rmw_m": 30*1852, "r34_m": 200*1852,
        "r34_quadrants_m": {"NE": 200*1852, "SE": 200*1852, "SW": 150*1852, "NW": 150*1852},
        "r50_quadrants_m": {"NE": 90*1852, "SE": 90*1852, "SW": 80*1852, "NW": 50*1852},
        "r64_quadrants_m": {"NE": 60*1852, "SE": 60*1852, "SW": 40*1852, "NW": 30*1852},
        "approach_angle_deg": 20.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 198.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 1,
    },
    {
        "name": "Harvey",
        "year": 2017,
        "region_key": "gulf_central_tx",
        "vmax_ms": 58.1, "min_pressure_hpa": 937,
        "lat": 27.9, "lon": -96.9,
        "forward_speed_ms": 2.6, "rmw_m": 15*1852, "r34_m": 100*1852,
        "r34_quadrants_m": {"NE": 100*1852, "SE": 90*1852, "SW": 60*1852, "NW": 90*1852},
        "r50_quadrants_m": {"NE": 50*1852, "SE": 50*1852, "SW": 30*1852, "NW": 40*1852},
        "r64_quadrants_m": {"NE": 25*1852, "SE": 25*1852, "SW": 15*1852, "NW": 20*1852},
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 160.0,  # NOAA NCEI CPI-adjusted (stall + flooding)
        "damage_rank": 2,
    },
    {
        "name": "Ian",
        "year": 2022,
        "region_key": "gulf_fl_west",
        "vmax_ms": 72.0, "min_pressure_hpa": 937,
        "lat": 26.7, "lon": -82.2,
        "forward_speed_ms": 4.1, "rmw_m": 20*1852, "r34_m": 150*1852,
        "r34_quadrants_m": {"NE": 150*1852, "SE": 130*1852, "SW": 90*1852, "NW": 120*1852},
        "r50_quadrants_m": {"NE": 80*1852, "SE": 70*1852, "SW": 45*1852, "NW": 60*1852},
        "r64_quadrants_m": {"NE": 45*1852, "SE": 40*1852, "SW": 25*1852, "NW": 35*1852},
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 113.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 4,
    },
    {
        "name": "Maria",
        "year": 2017,
        "region_key": "carib_pr",
        "vmax_ms": 72.0, "min_pressure_hpa": 908,
        "lat": 18.2, "lon": -65.9,
        "forward_speed_ms": 4.6, "rmw_m": 15*1852, "r34_m": 130*1852,
        "r34_quadrants_m": {"NE": 130*1852, "SE": 110*1852, "SW": 80*1852, "NW": 120*1852},
        "r50_quadrants_m": {"NE": 60*1852, "SE": 50*1852, "SW": 40*1852, "NW": 50*1852},
        "r64_quadrants_m": {"NE": 30*1852, "SE": 30*1852, "SW": 20*1852, "NW": 25*1852},
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 115.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 3,
    },
    {
        "name": "Sandy",
        "year": 2012,
        "region_key": "atl_ne",
        "vmax_ms": 36.0, "min_pressure_hpa": 940,
        "lat": 39.5, "lon": -74.0,
        "forward_speed_ms": 11.0, "rmw_m": 80*1852, "r34_m": 450*1852,
        "r34_quadrants_m": {"NE": 450*1852, "SE": 400*1852, "SW": 300*1852, "NW": 350*1852},
        "r50_quadrants_m": {"NE": 250*1852, "SE": 200*1852, "SW": 150*1852, "NW": 180*1852},
        "r64_quadrants_m": {"NE": 120*1852, "SE": 90*1852, "SW": 60*1852, "NW": 80*1852},
        "approach_angle_deg": -15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 90.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 5,
    },
    {
        "name": "Helene",
        "year": 2024,
        "region_key": "gulf_fl_panhandle",  # Big Bend FL landfall
        "vmax_ms": 64.3, "min_pressure_hpa": 938,  # 125kt Cat 4
        "lat": 29.8, "lon": -83.8,
        "forward_speed_ms": 9.3, "rmw_m": 25*1852, "r34_m": 240*1852,
        "r34_quadrants_m": {"NE": 240*1852, "SE": 200*1852, "SW": 130*1852, "NW": 180*1852},
        "r50_quadrants_m": {"NE": 120*1852, "SE": 100*1852, "SW": 60*1852, "NW": 90*1852},
        "r64_quadrants_m": {"NE": 50*1852, "SE": 45*1852, "SW": 25*1852, "NW": 35*1852},
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 79.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 8,
    },
    {
        "name": "Irma",
        "year": 2017,
        "region_key": "atl_fl_east",
        "vmax_ms": 58.1, "min_pressure_hpa": 929,
        "lat": 25.3, "lon": -80.7,
        "forward_speed_ms": 6.2, "rmw_m": 30*1852, "r34_m": 200*1852,
        "r34_quadrants_m": {"NE": 200*1852, "SE": 180*1852, "SW": 120*1852, "NW": 150*1852},
        "r50_quadrants_m": {"NE": 100*1852, "SE": 90*1852, "SW": 60*1852, "NW": 80*1852},
        "r64_quadrants_m": {"NE": 50*1852, "SE": 45*1852, "SW": 30*1852, "NW": 40*1852},
        "approach_angle_deg": -10.0, "track_parallel_factor": 0.55,
        "actual_damage_B": 80.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 7,
    },
    {
        "name": "Ida",
        "year": 2021,
        "region_key": "gulf_la",
        "vmax_ms": 69.4, "min_pressure_hpa": 930,  # 135kt Cat 4
        "lat": 29.2, "lon": -90.1,
        "forward_speed_ms": 5.1, "rmw_m": 18*1852, "r34_m": 140*1852,
        "r34_quadrants_m": {"NE": 140*1852, "SE": 120*1852, "SW": 80*1852, "NW": 110*1852},
        "r50_quadrants_m": {"NE": 70*1852, "SE": 60*1852, "SW": 35*1852, "NW": 55*1852},
        "r64_quadrants_m": {"NE": 40*1852, "SE": 35*1852, "SW": 20*1852, "NW": 30*1852},
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 84.0,  # NOAA NCEI; includes NE remnant flooding (NYC, NJ)
        "damage_rank": 6,
    },
    {
        "name": "Milton",
        "year": 2024,
        "region_key": "gulf_fl_west",
        "vmax_ms": 59.2, "min_pressure_hpa": 950,  # 115kt Cat 3 at FL landfall
        "lat": 27.6, "lon": -82.6,
        "forward_speed_ms": 7.7, "rmw_m": 15*1852, "r34_m": 175*1852,
        "r34_quadrants_m": {"NE": 175*1852, "SE": 150*1852, "SW": 100*1852, "NW": 130*1852},
        "r50_quadrants_m": {"NE": 90*1852, "SE": 75*1852, "SW": 45*1852, "NW": 65*1852},
        "r64_quadrants_m": {"NE": 40*1852, "SE": 35*1852, "SW": 20*1852, "NW": 30*1852},
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 34.0,  # NOAA NCEI
        "damage_rank": 9,
    },
    {
        "name": "Michael",
        "year": 2018,
        "region_key": "gulf_fl_panhandle",
        "vmax_ms": 72.0, "min_pressure_hpa": 919,
        "lat": 30.0, "lon": -85.5,
        "forward_speed_ms": 7.2, "rmw_m": 12*1852, "r34_m": 100*1852,
        "r34_quadrants_m": {"NE": 120*1852, "SE": 100*1852, "SW": 70*1852, "NW": 90*1852},
        "r50_quadrants_m": {"NE": 60*1852, "SE": 50*1852, "SW": 35*1852, "NW": 45*1852},
        "r64_quadrants_m": {"NE": 30*1852, "SE": 25*1852, "SW": 15*1852, "NW": 20*1852},
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 32.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 10,
    },
    {
        "name": "Florence",
        "year": 2018,
        "region_key": "atl_nc",
        "vmax_ms": 46.3, "min_pressure_hpa": 958,
        "lat": 34.2, "lon": -77.8,
        "forward_speed_ms": 2.6, "rmw_m": 30*1852, "r34_m": 200*1852,
        "r34_quadrants_m": {"NE": 200*1852, "SE": 180*1852, "SW": 130*1852, "NW": 170*1852},
        "r50_quadrants_m": {"NE": 90*1852, "SE": 80*1852, "SW": 50*1852, "NW": 70*1852},
        "r64_quadrants_m": {"NE": 30*1852, "SE": 25*1852, "SW": 15*1852, "NW": 20*1852},
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.10,
        "actual_damage_B": 29.0,  # NOAA NCEI CPI-adjusted
        "damage_rank": 11,
    },
    {
        "name": "Dorian",
        "year": 2019,
        "region_key": "carib_bahamas",
        "vmax_ms": 82.3, "min_pressure_hpa": 910,
        "lat": 26.5, "lon": -77.1,
        "forward_speed_ms": 1.5, "rmw_m": 15*1852, "r34_m": 130*1852,
        "r34_quadrants_m": {"NE": 130*1852, "SE": 120*1852, "SW": 80*1852, "NW": 100*1852},
        "r50_quadrants_m": {"NE": 65*1852, "SE": 60*1852, "SW": 40*1852, "NW": 50*1852},
        "r64_quadrants_m": {"NE": 35*1852, "SE": 35*1852, "SW": 20*1852, "NW": 25*1852},
        "approach_angle_deg": 12.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 5.0,  # Small Bahamas economy despite Cat 5
        "damage_rank": 12,
    },
]


def spearman_rank_correlation(ranks_a: List[int], ranks_b: List[int]) -> float:
    """Compute Spearman's rank correlation coefficient."""
    n = len(ranks_a)
    if n < 2:
        return 0.0
    d_sq = sum((a - b) ** 2 for a, b in zip(ranks_a, ranks_b))
    return 1.0 - (6.0 * d_sq) / (n * (n * n - 1))


def rank_list(values: List[float]) -> List[int]:
    """Convert values to ranks (1 = highest value)."""
    indexed = sorted(enumerate(values), key=lambda x: -x[1])
    ranks = [0] * len(values)
    for rank, (idx, _) in enumerate(indexed, 1):
        ranks[idx] = rank
    return ranks


def compute_base_scores():
    """Compute the three sub-formula scores for each storm."""
    results = []
    for storm in TOURNAMENT_STORMS:
        r = compute_dpi_simple(
            vmax_ms=storm["vmax_ms"],
            min_pressure_hpa=storm["min_pressure_hpa"],
            lat=storm["lat"], lon=storm["lon"],
            r34_m=storm.get("r34_m"),
            rmw_m=storm.get("rmw_m"),
            forward_speed_ms=storm.get("forward_speed_ms"),
            r34_quadrants_m=storm.get("r34_quadrants_m"),
            r50_quadrants_m=storm.get("r50_quadrants_m"),
            r64_quadrants_m=storm.get("r64_quadrants_m"),
            region_key=storm["region_key"],
            storm_id=f"AL{storm['year']}",
            name=storm["name"],
            storm_year=storm["year"],
            approach_angle_deg=storm.get("approach_angle_deg"),
            track_parallel_factor=storm.get("track_parallel_factor"),
            apply_land_dampening=False,  # Disable for fair comparison
        )
        results.append({
            "name": storm["name"],
            "year": storm["year"],
            "actual_damage_B": storm["actual_damage_B"],
            "damage_rank": storm["damage_rank"],
            "ike_score": r.ike_score,
            "surge_rain_score": r.surge_rain_score,
            "economic_score": r.economic_score,
            "current_dpi": r.dpi_score,
        })
    return results


def formula_A_additive(r, w_ike=0.30, w_sr=0.35, w_econ=0.35):
    """Current: weighted additive sum."""
    return w_ike * r["ike_score"] + w_sr * r["surge_rain_score"] + w_econ * r["economic_score"]


def formula_B_econ_mult(r, econ_scale):
    """
    Physics-first with economic multiplier.
    Base = 0.45×IKE + 0.55×Surge/Rain (the 'environmental' part)
    Then multiply by an economic factor derived from the economic sub-score.

    econ_multiplier = 1.0 + (economic_score / 100) × (econ_scale - 1.0)

    At econ_scale=1.0: no amplification (purely physical)
    At econ_scale=1.5: a storm scoring 100/100 economic gets 1.5× amplification
    At econ_scale=2.0: a storm scoring 100/100 economic gets 2.0× amplification
    """
    base = 0.45 * r["ike_score"] + 0.55 * r["surge_rain_score"]
    econ_frac = r["economic_score"] / 100.0
    multiplier = 1.0 + econ_frac * (econ_scale - 1.0)
    return min(100.0, base * multiplier)


def formula_C_full_mult(r, env_scale, econ_scale):
    """
    Full multiplicative: IKE × environment_multiplier × economic_multiplier

    env_multiplier = 1.0 + (surge_rain_score / 100) × (env_scale - 1.0)
    econ_multiplier = 1.0 + (economic_score / 100) × (econ_scale - 1.0)

    Base is IKE_score (0-100), amplified by both environmental and economic factors.
    """
    base = r["ike_score"]
    env_frac = r["surge_rain_score"] / 100.0
    econ_frac = r["economic_score"] / 100.0
    env_mult = 1.0 + env_frac * (env_scale - 1.0)
    econ_mult = 1.0 + econ_frac * (econ_scale - 1.0)
    return min(100.0, base * env_mult * econ_mult)


def evaluate_formula(results, formula_fn, **kwargs):
    """
    Evaluate a formula variant against actual damage rankings.

    Returns:
        spearman: Spearman rank correlation with damage
        spread: Score spread (max - min), measures discrimination
        top3_accuracy: How many of top 3 damage storms are in top 3 DPI
        scores: List of (name, dpi_score, damage_rank)
    """
    scores = [formula_fn(r, **kwargs) for r in results]
    dpi_ranks = rank_list(scores)
    damage_ranks = [r["damage_rank"] for r in results]

    rho = spearman_rank_correlation(dpi_ranks, damage_ranks)
    spread = max(scores) - min(scores)

    # Top-3 accuracy: how many of the 3 highest damage storms are ranked top 3 by DPI
    top3_damage_names = {r["name"] for r in sorted(results, key=lambda x: x["damage_rank"])[:3]}
    top3_dpi_indices = sorted(range(len(scores)), key=lambda i: -scores[i])[:3]
    top3_dpi_names = {results[i]["name"] for i in top3_dpi_indices}
    top3_hit = len(top3_damage_names & top3_dpi_names)

    named_scores = [(results[i]["name"], scores[i], results[i]["damage_rank"], dpi_ranks[i])
                     for i in range(len(results))]

    return {
        "spearman": rho,
        "spread": spread,
        "top3_accuracy": top3_hit,
        "scores": sorted(named_scores, key=lambda x: -x[1]),
    }


def run_tournament():
    """Run the full formula architecture tournament."""
    print("=" * 100)
    print("DPI FORMULA ARCHITECTURE TOURNAMENT")
    print("Comparing additive vs multiplicative formula structures")
    print("Validation: rank correlation with actual economic damage (2024 USD)")
    print("=" * 100)

    results = compute_base_scores()

    # Print base scores
    print("\n┌─ BASE SUB-FORMULA SCORES ─────────────────────────────────────────────────────┐")
    print(f"│ {'Storm':<12} {'Year':>4}  {'IKE':>6}  {'Surge/R':>7}  {'Econ':>6}  {'Current DPI':>11}  {'Damage($B)':>10}  {'Dmg Rank':>8} │")
    print(f"├{'─'*93}┤")
    for r in sorted(results, key=lambda x: x["damage_rank"]):
        print(f"│ {r['name']:<12} {r['year']:>4}  {r['ike_score']:>6.1f}  {r['surge_rain_score']:>7.1f}  "
              f"{r['economic_score']:>6.1f}  {r['current_dpi']:>11.1f}  ${r['actual_damage_B']:>8.0f}B  "
              f"#{r['damage_rank']:>6} │")
    print(f"└{'─'*93}┘")

    # ── Architecture A: Current additive ──
    print("\n\n" + "=" * 100)
    print("ARCHITECTURE A: CURRENT ADDITIVE (0.30×IKE + 0.35×SR + 0.35×Econ)")
    print("=" * 100)
    eval_a = evaluate_formula(results, formula_A_additive)
    _print_eval("Additive (current)", eval_a)

    # ── Architecture B: Economic Multiplier ──
    print("\n\n" + "=" * 100)
    print("ARCHITECTURE B: ECONOMIC MULTIPLIER — (0.45×IKE + 0.55×SR) × econ_mult")
    print("Sweeping econ_scale from 1.0× to 2.0×")
    print("=" * 100)

    best_b = {"spearman": -2.0}
    for scale_10x in range(10, 21):  # 1.0 to 2.0 in 0.1 steps
        scale = scale_10x / 10.0
        ev = evaluate_formula(results, formula_B_econ_mult, econ_scale=scale)
        tag = ""
        if ev["spearman"] > best_b["spearman"]:
            best_b = {**ev, "scale": scale}
        if scale_10x % 2 == 0 or scale == 1.0:  # Print every 0.2 steps
            print(f"  scale={scale:.1f}×  ρ={ev['spearman']:.4f}  spread={ev['spread']:.1f}  top3={ev['top3_accuracy']}/3")

    print(f"\n  ★ BEST: scale={best_b['scale']:.1f}×  ρ={best_b['spearman']:.4f}  spread={best_b['spread']:.1f}  top3={best_b['top3_accuracy']}/3")
    _print_eval(f"Econ Mult (best: {best_b['scale']:.1f}×)", best_b)

    # ── Architecture C: Full Multiplicative ──
    print("\n\n" + "=" * 100)
    print("ARCHITECTURE C: FULL MULTIPLICATIVE — IKE × env_mult × econ_mult")
    print("Sweeping env_scale × econ_scale (1.0-2.0 each)")
    print("=" * 100)

    best_c = {"spearman": -2.0}
    header = "env\\econ"
    print(f"\n  {header:<8}", end="")
    for es in range(10, 21, 2):
        print(f"  {es/10:.1f}×  ", end="")
    print()
    print("  " + "─" * 60)

    for env_10x in range(10, 21, 2):
        env_s = env_10x / 10.0
        print(f"  {env_s:.1f}×    ", end="")
        for econ_10x in range(10, 21, 2):
            econ_s = econ_10x / 10.0
            ev = evaluate_formula(results, formula_C_full_mult, env_scale=env_s, econ_scale=econ_s)
            print(f" {ev['spearman']:+.3f} ", end="")
            if ev["spearman"] > best_c["spearman"]:
                best_c = {**ev, "env_scale": env_s, "econ_scale": econ_s}
        print()

    print(f"\n  ★ BEST: env={best_c['env_scale']:.1f}× econ={best_c['econ_scale']:.1f}×  "
          f"ρ={best_c['spearman']:.4f}  spread={best_c['spread']:.1f}  top3={best_c['top3_accuracy']}/3")
    _print_eval(f"Full Mult (env={best_c['env_scale']:.1f}× econ={best_c['econ_scale']:.1f}×)", best_c)

    # ── Final comparison ──
    print("\n\n" + "=" * 100)
    print("FINAL COMPARISON")
    print("=" * 100)
    print(f"\n  {'Architecture':<45} {'Spearman ρ':>10}  {'Spread':>7}  {'Top-3':>5}")
    print(f"  {'─'*75}")
    print(f"  {'A) Additive (current: 30/35/35)':<45} {eval_a['spearman']:>+10.4f}  {eval_a['spread']:>7.1f}  {eval_a['top3_accuracy']:>3}/3")
    b_label = f"B) Econ Mult (best: {best_b['scale']:.1f}x)"
    c_label = f"C) Full Mult (env={best_c['env_scale']:.1f}x econ={best_c['econ_scale']:.1f}x)"
    print(f"  {b_label:<45} {best_b['spearman']:>+10.4f}  {best_b['spread']:>7.1f}  {best_b['top3_accuracy']:>3}/3")
    print(f"  {c_label:<45} {best_c['spearman']:>+10.4f}  {best_c['spread']:>7.1f}  {best_c['top3_accuracy']:>3}/3")

    # Diminishing returns analysis for Architecture B
    print("\n\n" + "=" * 100)
    print("DIMINISHING RETURNS ANALYSIS (Architecture B)")
    print("=" * 100)
    prev_rho = None
    for scale_10x in range(10, 21):
        scale = scale_10x / 10.0
        ev = evaluate_formula(results, formula_B_econ_mult, econ_scale=scale)
        delta = f"  Δρ={ev['spearman'] - prev_rho:+.4f}" if prev_rho is not None else ""
        marker = ""
        if prev_rho is not None and ev["spearman"] < prev_rho:
            marker = "  ◄ DIMINISHING"
        prev_rho = ev["spearman"]
        print(f"  {scale:.1f}×  ρ={ev['spearman']:+.4f}{delta}{marker}")

    # Score compression check — are high-damage storms sufficiently separated from low-damage?
    print("\n\n" + "=" * 100)
    print("SCORE DISCRIMINATION CHECK")
    print("Top 3 vs Bottom 3 average DPI gap (bigger = better discrimination)")
    print("=" * 100)

    for label, fn, kw in [
        ("A) Additive", formula_A_additive, {}),
        (f"B) Econ Mult {best_b['scale']:.1f}×", formula_B_econ_mult, {"econ_scale": best_b['scale']}),
        (f"C) Full Mult env={best_c['env_scale']:.1f}× econ={best_c['econ_scale']:.1f}×",
         formula_C_full_mult, {"env_scale": best_c['env_scale'], "econ_scale": best_c['econ_scale']}),
    ]:
        scores_by_damage = [(r["damage_rank"], fn(r, **kw)) for r in results]
        scores_by_damage.sort(key=lambda x: x[0])
        top3_avg = sum(s for _, s in scores_by_damage[:3]) / 3
        bot3_avg = sum(s for _, s in scores_by_damage[-3:]) / 3
        gap = top3_avg - bot3_avg
        print(f"  {label:<50}  top3_avg={top3_avg:.1f}  bot3_avg={bot3_avg:.1f}  gap={gap:.1f}")

    print("\n" + "=" * 100)
    print("RECOMMENDATION")
    print("=" * 100)

    all_evals = [
        ("A) Additive (current)", eval_a),
        (f"B) Econ Mult ({best_b['scale']:.1f}×)", best_b),
        (f"C) Full Mult (env={best_c['env_scale']:.1f}× econ={best_c['econ_scale']:.1f}×)", best_c),
    ]
    all_evals.sort(key=lambda x: -x[1]["spearman"])
    winner = all_evals[0]
    runner = all_evals[1]

    print(f"\n  Winner: {winner[0]}")
    print(f"    ρ={winner[1]['spearman']:.4f}, spread={winner[1]['spread']:.1f}, top3={winner[1]['top3_accuracy']}/3")
    print(f"\n  Runner-up: {runner[0]}")
    print(f"    ρ={runner[1]['spearman']:.4f}, spread={runner[1]['spread']:.1f}, top3={runner[1]['top3_accuracy']}/3")

    rho_diff = winner[1]["spearman"] - runner[1]["spearman"]
    if rho_diff < 0.02:
        print(f"\n  Δρ = {rho_diff:.4f} — marginal improvement. Recommend SHELVING until")
        print(f"  environmental formula (F2) is also evaluated as a multiplier candidate.")
        print(f"  Current additive architecture is well-calibrated and should not be")
        print(f"  changed for a <0.02 improvement in rank correlation.")
    elif rho_diff < 0.05:
        print(f"\n  Δρ = {rho_diff:.4f} — modest improvement. Worth investigating but")
        print(f"  the additive architecture should remain default until more storms")
        print(f"  validate the multiplicative approach.")
    else:
        print(f"\n  Δρ = {rho_diff:.4f} — significant improvement. Consider adopting")
        print(f"  the winning architecture as the new default.")


def _print_eval(label, ev):
    """Print detailed evaluation results."""
    print(f"\n  {label}:")
    print(f"  {'Storm':<12} {'DPI Score':>9}  {'DPI Rank':>8}  {'Damage Rank':>11}  {'Δ Rank':>6}")
    print(f"  {'─'*55}")
    for name, score, dmg_rank, dpi_rank in ev["scores"]:
        delta = dpi_rank - dmg_rank
        marker = "  ✓" if delta == 0 else f"  {'↑' if delta < 0 else '↓'}{abs(delta)}"
        print(f"  {name:<12} {score:>9.1f}  #{dpi_rank:>7}  #{dmg_rank:>10}{marker}")
    print(f"\n  Spearman ρ = {ev['spearman']:.4f}  |  Spread = {ev['spread']:.1f}  |  Top-3 = {ev['top3_accuracy']}/3")


if __name__ == "__main__":
    run_tournament()
