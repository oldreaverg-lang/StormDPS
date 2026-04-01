"""
Comprehensive DPI ranking of all preloaded storms.
Computes the current DPI score for every storm in the system and ranks them.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.dpi import compute_dpi_simple

NM_TO_M = 1852.0
KT_TO_MS = 0.514444

# =============================================================================
# ALL STORMS — merged from validate_dpi.py, validate_formula.py, and presets
# Using validate_formula.py parameters where available (more recent calibration)
# =============================================================================

ALL_STORMS = [
    # ── 2024 Season ──
    {
        "name": "Helene", "year": 2024,
        "vmax_ms": 122 * KT_TO_MS, "min_pressure_hpa": 938,
        "lat": 29.8, "lon": -83.7,
        "forward_speed_ms": 21.0 * KT_TO_MS, "rmw_m": 35 * NM_TO_M,
        "r34_m": 300 * NM_TO_M,
        "r34_quadrants_m": {"NE": 300*NM_TO_M, "SE": 250*NM_TO_M, "SW": 160*NM_TO_M, "NW": 220*NM_TO_M},
        "r50_quadrants_m": {"NE": 150*NM_TO_M, "SE": 120*NM_TO_M, "SW": 70*NM_TO_M, "NW": 100*NM_TO_M},
        "r64_quadrants_m": {"NE": 80*NM_TO_M, "SE": 65*NM_TO_M, "SW": 35*NM_TO_M, "NW": 55*NM_TO_M},
        "region_key": "gulf_fl_panhandle",
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 79.0, "category": 4,
    },
    {
        "name": "Milton", "year": 2024,
        "vmax_ms": 104 * KT_TO_MS, "min_pressure_hpa": 958,
        "lat": 27.2, "lon": -82.5,
        "forward_speed_ms": 14.0 * KT_TO_MS, "rmw_m": 25 * NM_TO_M,
        "r34_m": 250 * NM_TO_M,
        "r34_quadrants_m": {"NE": 250*NM_TO_M, "SE": 200*NM_TO_M, "SW": 130*NM_TO_M, "NW": 180*NM_TO_M},
        "r50_quadrants_m": {"NE": 120*NM_TO_M, "SE": 100*NM_TO_M, "SW": 60*NM_TO_M, "NW": 85*NM_TO_M},
        "r64_quadrants_m": {"NE": 55*NM_TO_M, "SE": 45*NM_TO_M, "SW": 25*NM_TO_M, "NW": 40*NM_TO_M},
        "region_key": "gulf_fl_west",
        "approach_angle_deg": 5.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 34.0, "category": 5,
    },
    {
        "name": "Beryl", "year": 2024,
        "vmax_ms": 80 * KT_TO_MS, "min_pressure_hpa": 979,  # TX landfall
        "lat": 28.9, "lon": -95.3,
        "forward_speed_ms": 11.0 * KT_TO_MS, "rmw_m": 25 * NM_TO_M,
        "r34_m": 120 * NM_TO_M,
        "r34_quadrants_m": {"NE": 120*NM_TO_M, "SE": 100*NM_TO_M, "SW": 60*NM_TO_M, "NW": 90*NM_TO_M},
        "r50_quadrants_m": {"NE": 50*NM_TO_M, "SE": 40*NM_TO_M, "SW": 25*NM_TO_M, "NW": 35*NM_TO_M},
        "r64_quadrants_m": {"NE": 25*NM_TO_M, "SE": 20*NM_TO_M, "SW": 10*NM_TO_M, "NW": 15*NM_TO_M},
        "region_key": "gulf_central_tx",
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 6.0, "category": 1,
    },
    # ── 2023 Season ──
    {
        "name": "Idalia", "year": 2023,
        "vmax_ms": 110 * KT_TO_MS, "min_pressure_hpa": 949,
        "lat": 29.9, "lon": -83.4,  # Big Bend FL
        "forward_speed_ms": 16.0 * KT_TO_MS, "rmw_m": 20 * NM_TO_M,
        "r34_m": 150 * NM_TO_M,
        "r34_quadrants_m": {"NE": 150*NM_TO_M, "SE": 120*NM_TO_M, "SW": 70*NM_TO_M, "NW": 110*NM_TO_M},
        "r50_quadrants_m": {"NE": 60*NM_TO_M, "SE": 50*NM_TO_M, "SW": 30*NM_TO_M, "NW": 45*NM_TO_M},
        "r64_quadrants_m": {"NE": 30*NM_TO_M, "SE": 25*NM_TO_M, "SW": 15*NM_TO_M, "NW": 20*NM_TO_M},
        "region_key": "gulf_fl_panhandle",
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 3.6, "category": 3,
    },
    # ── 2022 Season ──
    {
        "name": "Ian", "year": 2022,
        "vmax_ms": 72.0, "min_pressure_hpa": 937,
        "lat": 26.7, "lon": -82.2,
        "forward_speed_ms": 4.1, "rmw_m": 20 * NM_TO_M,
        "r34_m": 150 * NM_TO_M,
        "r34_quadrants_m": {"NE": 150*NM_TO_M, "SE": 130*NM_TO_M, "SW": 90*NM_TO_M, "NW": 120*NM_TO_M},
        "r50_quadrants_m": {"NE": 80*NM_TO_M, "SE": 70*NM_TO_M, "SW": 45*NM_TO_M, "NW": 60*NM_TO_M},
        "r64_quadrants_m": {"NE": 45*NM_TO_M, "SE": 40*NM_TO_M, "SW": 25*NM_TO_M, "NW": 35*NM_TO_M},
        "region_key": "gulf_fl_west",
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 113.0, "category": 4,
    },
    # ── 2021 Season ──
    {
        "name": "Ida", "year": 2021,
        "vmax_ms": 130 * KT_TO_MS, "min_pressure_hpa": 930,
        "lat": 29.0, "lon": -90.1,
        "forward_speed_ms": 10.0 * KT_TO_MS, "rmw_m": 15 * NM_TO_M,
        "r34_m": 140 * NM_TO_M,
        "r34_quadrants_m": {"NE": 140*NM_TO_M, "SE": 120*NM_TO_M, "SW": 80*NM_TO_M, "NW": 110*NM_TO_M},
        "r50_quadrants_m": {"NE": 70*NM_TO_M, "SE": 60*NM_TO_M, "SW": 35*NM_TO_M, "NW": 55*NM_TO_M},
        "r64_quadrants_m": {"NE": 40*NM_TO_M, "SE": 35*NM_TO_M, "SW": 20*NM_TO_M, "NW": 30*NM_TO_M},
        "region_key": "gulf_la",
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 84.0, "category": 4,
    },
    # ── 2019 Season ──
    {
        "name": "Dorian", "year": 2019,
        "vmax_ms": 82.3, "min_pressure_hpa": 910,
        "lat": 26.5, "lon": -77.1,
        "forward_speed_ms": 1.5, "rmw_m": 15 * NM_TO_M,
        "r34_m": 130 * NM_TO_M,
        "r34_quadrants_m": {"NE": 130*NM_TO_M, "SE": 120*NM_TO_M, "SW": 80*NM_TO_M, "NW": 100*NM_TO_M},
        "r50_quadrants_m": {"NE": 65*NM_TO_M, "SE": 60*NM_TO_M, "SW": 40*NM_TO_M, "NW": 50*NM_TO_M},
        "r64_quadrants_m": {"NE": 35*NM_TO_M, "SE": 35*NM_TO_M, "SW": 20*NM_TO_M, "NW": 25*NM_TO_M},
        "region_key": "carib_bahamas",
        "approach_angle_deg": 12.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 5.0, "category": 5,
    },
    # ── 2018 Season ──
    {
        "name": "Michael", "year": 2018,
        "vmax_ms": 72.0, "min_pressure_hpa": 919,
        "lat": 30.0, "lon": -85.5,
        "forward_speed_ms": 7.2, "rmw_m": 12 * NM_TO_M,
        "r34_m": 100 * NM_TO_M,
        "r34_quadrants_m": {"NE": 120*NM_TO_M, "SE": 100*NM_TO_M, "SW": 70*NM_TO_M, "NW": 90*NM_TO_M},
        "r50_quadrants_m": {"NE": 60*NM_TO_M, "SE": 50*NM_TO_M, "SW": 35*NM_TO_M, "NW": 45*NM_TO_M},
        "r64_quadrants_m": {"NE": 30*NM_TO_M, "SE": 25*NM_TO_M, "SW": 15*NM_TO_M, "NW": 20*NM_TO_M},
        "region_key": "gulf_fl_panhandle",
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 32.0, "category": 5,
    },
    {
        "name": "Florence", "year": 2018,
        "vmax_ms": 46.3, "min_pressure_hpa": 958,
        "lat": 34.2, "lon": -77.8,
        "forward_speed_ms": 2.6, "rmw_m": 30 * NM_TO_M,
        "r34_m": 200 * NM_TO_M,
        "r34_quadrants_m": {"NE": 200*NM_TO_M, "SE": 180*NM_TO_M, "SW": 130*NM_TO_M, "NW": 170*NM_TO_M},
        "r50_quadrants_m": {"NE": 90*NM_TO_M, "SE": 80*NM_TO_M, "SW": 50*NM_TO_M, "NW": 70*NM_TO_M},
        "r64_quadrants_m": {"NE": 30*NM_TO_M, "SE": 25*NM_TO_M, "SW": 15*NM_TO_M, "NW": 20*NM_TO_M},
        "region_key": "atl_nc",
        "approach_angle_deg": 10.0, "track_parallel_factor": 0.10,
        "actual_damage_B": 29.0, "category": 1,
    },
    # ── 2017 Season ──
    {
        "name": "Harvey", "year": 2017,
        "vmax_ms": 58.1, "min_pressure_hpa": 937,
        "lat": 27.9, "lon": -96.9,
        "forward_speed_ms": 2.6, "rmw_m": 15 * NM_TO_M,
        "r34_m": 100 * NM_TO_M,
        "r34_quadrants_m": {"NE": 100*NM_TO_M, "SE": 90*NM_TO_M, "SW": 60*NM_TO_M, "NW": 90*NM_TO_M},
        "r50_quadrants_m": {"NE": 50*NM_TO_M, "SE": 50*NM_TO_M, "SW": 30*NM_TO_M, "NW": 40*NM_TO_M},
        "r64_quadrants_m": {"NE": 25*NM_TO_M, "SE": 25*NM_TO_M, "SW": 15*NM_TO_M, "NW": 20*NM_TO_M},
        "region_key": "gulf_central_tx",
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 160.0, "category": 4,
    },
    {
        "name": "Irma", "year": 2017,
        "vmax_ms": 58.1, "min_pressure_hpa": 929,
        "lat": 25.3, "lon": -80.7,
        "forward_speed_ms": 6.2, "rmw_m": 30 * NM_TO_M,
        "r34_m": 200 * NM_TO_M,
        "r34_quadrants_m": {"NE": 200*NM_TO_M, "SE": 180*NM_TO_M, "SW": 120*NM_TO_M, "NW": 150*NM_TO_M},
        "r50_quadrants_m": {"NE": 100*NM_TO_M, "SE": 90*NM_TO_M, "SW": 60*NM_TO_M, "NW": 80*NM_TO_M},
        "r64_quadrants_m": {"NE": 50*NM_TO_M, "SE": 45*NM_TO_M, "SW": 30*NM_TO_M, "NW": 40*NM_TO_M},
        "region_key": "atl_fl_east",
        "approach_angle_deg": -10.0, "track_parallel_factor": 0.55,
        "actual_damage_B": 80.0, "category": 4,
    },
    {
        "name": "Maria", "year": 2017,
        "vmax_ms": 72.0, "min_pressure_hpa": 908,
        "lat": 18.2, "lon": -65.9,
        "forward_speed_ms": 4.6, "rmw_m": 15 * NM_TO_M,
        "r34_m": 130 * NM_TO_M,
        "r34_quadrants_m": {"NE": 130*NM_TO_M, "SE": 110*NM_TO_M, "SW": 80*NM_TO_M, "NW": 120*NM_TO_M},
        "r50_quadrants_m": {"NE": 60*NM_TO_M, "SE": 50*NM_TO_M, "SW": 40*NM_TO_M, "NW": 50*NM_TO_M},
        "r64_quadrants_m": {"NE": 30*NM_TO_M, "SE": 30*NM_TO_M, "SW": 20*NM_TO_M, "NW": 25*NM_TO_M},
        "region_key": "carib_pr",
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 115.0, "category": 5,
    },
    # ── 2012 Season ──
    {
        "name": "Sandy", "year": 2012,
        "vmax_ms": 36.0, "min_pressure_hpa": 940,
        "lat": 39.5, "lon": -74.0,
        "forward_speed_ms": 11.0, "rmw_m": 80 * NM_TO_M,
        "r34_m": 450 * NM_TO_M,
        "r34_quadrants_m": {"NE": 450*NM_TO_M, "SE": 400*NM_TO_M, "SW": 300*NM_TO_M, "NW": 350*NM_TO_M},
        "r50_quadrants_m": {"NE": 250*NM_TO_M, "SE": 200*NM_TO_M, "SW": 150*NM_TO_M, "NW": 180*NM_TO_M},
        "r64_quadrants_m": {"NE": 120*NM_TO_M, "SE": 90*NM_TO_M, "SW": 60*NM_TO_M, "NW": 80*NM_TO_M},
        "region_key": "atl_ne",
        "approach_angle_deg": -15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 90.0, "category": 1,
    },
    # ── 2008 Season ──
    {
        "name": "Ike", "year": 2008,
        "vmax_ms": 95 * KT_TO_MS, "min_pressure_hpa": 950,
        "lat": 29.3, "lon": -94.7,  # Galveston TX landfall
        "forward_speed_ms": 12.0 * KT_TO_MS, "rmw_m": 45 * NM_TO_M,
        "r34_m": 275 * NM_TO_M,
        "r34_quadrants_m": {"NE": 275*NM_TO_M, "SE": 250*NM_TO_M, "SW": 180*NM_TO_M, "NW": 200*NM_TO_M},
        "r50_quadrants_m": {"NE": 120*NM_TO_M, "SE": 100*NM_TO_M, "SW": 70*NM_TO_M, "NW": 90*NM_TO_M},
        "r64_quadrants_m": {"NE": 50*NM_TO_M, "SE": 45*NM_TO_M, "SW": 30*NM_TO_M, "NW": 35*NM_TO_M},
        "region_key": "gulf_central_tx",
        "approach_angle_deg": 15.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 44.0, "category": 2,
    },
    # ── 2005 Season ──
    {
        "name": "Katrina", "year": 2005,
        "vmax_ms": 56.6, "min_pressure_hpa": 920,
        "lat": 29.3, "lon": -89.6,
        "forward_speed_ms": 5.8, "rmw_m": 30 * NM_TO_M,
        "r34_m": 200 * NM_TO_M,
        "r34_quadrants_m": {"NE": 200*NM_TO_M, "SE": 200*NM_TO_M, "SW": 150*NM_TO_M, "NW": 150*NM_TO_M},
        "r50_quadrants_m": {"NE": 90*NM_TO_M, "SE": 90*NM_TO_M, "SW": 80*NM_TO_M, "NW": 50*NM_TO_M},
        "r64_quadrants_m": {"NE": 60*NM_TO_M, "SE": 60*NM_TO_M, "SW": 40*NM_TO_M, "NW": 30*NM_TO_M},
        "region_key": "gulf_la",
        "approach_angle_deg": 20.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 198.0, "category": 3,
    },
    # ── Historical ──
    {
        "name": "Andrew", "year": 1992,
        "vmax_ms": 74.6, "min_pressure_hpa": 922,
        "lat": 25.5, "lon": -80.3,
        "forward_speed_ms": 7.7, "rmw_m": 12 * NM_TO_M,
        "r34_m": 90 * NM_TO_M,
        "r34_quadrants_m": {"NE": 90*NM_TO_M, "SE": 80*NM_TO_M, "SW": 60*NM_TO_M, "NW": 80*NM_TO_M},
        "r50_quadrants_m": {"NE": 40*NM_TO_M, "SE": 40*NM_TO_M, "SW": 25*NM_TO_M, "NW": 35*NM_TO_M},
        "r64_quadrants_m": {"NE": 20*NM_TO_M, "SE": 20*NM_TO_M, "SW": 10*NM_TO_M, "NW": 15*NM_TO_M},
        "region_key": "atl_fl_east",
        "approach_angle_deg": 5.0, "track_parallel_factor": 0.0,
        "actual_damage_B": 55.0, "category": 5, "storm_year": 1992,
    },
]


def main():
    print("=" * 110)
    print("COMPREHENSIVE DPI RANKING — ALL PRELOADED STORMS")
    print("=" * 110)
    print()

    ranked = []
    for storm in ALL_STORMS:
        year_override = storm.get("storm_year", storm["year"])
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
            storm_id=f"RANK_{storm['name'].upper()}",
            name=storm["name"],
            storm_year=year_override,
            approach_angle_deg=storm.get("approach_angle_deg"),
            track_parallel_factor=storm.get("track_parallel_factor"),
            apply_land_dampening=False,
        )
        vmax_kt = storm["vmax_ms"] / KT_TO_MS
        ranked.append({
            "name": storm["name"],
            "year": storm["year"],
            "cat": storm["category"],
            "dpi": r.dpi_score,
            "dpi_cat": r.dpi_category,
            "ike": r.ike_score,
            "surge_rain": r.surge_rain_score,
            "economic": r.economic_score,
            "ike_tj": r.formula1_ike.ike_total_tj,
            "surge_m": r.formula2_surge_rain.surge_height_m,
            "damage_est_B": r.formula3_economic.estimated_damage_billion_usd,
            "actual_damage_B": storm["actual_damage_B"],
            "vmax_kt": vmax_kt,
        })

    # Sort by DPI descending
    ranked.sort(key=lambda x: -x["dpi"])

    print(f"{'#':<3} {'Storm':<12} {'Year':>4}  {'Cat':>3}  {'DPI':>5}  {'Category':<14}  "
          f"{'IKE':>5}  {'SR':>5}  {'Econ':>5}  {'IKE TJ':>7}  {'Surge':>5}  "
          f"{'Est $B':>7}  {'Act $B':>7}  {'Vmax kt':>7}")
    print("─" * 110)

    for i, s in enumerate(ranked, 1):
        actual_str = f"${s['actual_damage_B']:.0f}B" if s['actual_damage_B'] else "N/A"
        print(f"{i:<3} {s['name']:<12} {s['year']:>4}  Cat{s['cat']:>1}  "
              f"{s['dpi']:>5.1f}  {s['dpi_cat']:<14}  "
              f"{s['ike']:>5.1f}  {s['surge_rain']:>5.1f}  {s['economic']:>5.1f}  "
              f"{s['ike_tj']:>7.1f}  {s['surge_m']:>5.1f}  "
              f"${s['damage_est_B']:>5.1f}B  {actual_str:>7}  {s['vmax_kt']:>7.0f}")

    # Print a "believability check" — compare DPI rank vs damage rank
    print("\n\n" + "=" * 110)
    print("BELIEVABILITY CHECK: DPI Rank vs Actual Damage Rank")
    print("=" * 110)

    damage_ranked = sorted(ranked, key=lambda x: -x["actual_damage_B"])
    dpi_ranked = sorted(ranked, key=lambda x: -x["dpi"])

    name_to_dpi_rank = {s["name"]: i+1 for i, s in enumerate(dpi_ranked)}
    name_to_dmg_rank = {s["name"]: i+1 for i, s in enumerate(damage_ranked)}

    print(f"\n{'Storm':<12} {'DPI Rank':>8}  {'DPI':>5}  {'Dmg Rank':>8}  {'Actual $B':>9}  {'Delta':>6}  Note")
    print("─" * 80)
    for s in damage_ranked:
        dr = name_to_dpi_rank[s["name"]]
        ar = name_to_dmg_rank[s["name"]]
        delta = dr - ar
        note = ""
        if abs(delta) <= 1:
            note = "✓ Close match"
        elif abs(delta) <= 3:
            note = "~ Acceptable"
        elif delta > 0:
            note = f"↓ DPI underrates by {delta} ranks"
        else:
            note = f"↑ DPI overrates by {-delta} ranks"
        print(f"{s['name']:<12} #{dr:>7}  {s['dpi']:>5.1f}  #{ar:>7}  ${s['actual_damage_B']:>7.0f}B  {delta:>+5}  {note}")

    # Summary stats
    deltas = [abs(name_to_dpi_rank[s["name"]] - name_to_dmg_rank[s["name"]]) for s in ranked]
    avg_delta = sum(deltas) / len(deltas)
    max_delta = max(deltas)
    within_2 = sum(1 for d in deltas if d <= 2)
    print(f"\n  Average rank displacement: {avg_delta:.1f}")
    print(f"  Max rank displacement: {max_delta}")
    print(f"  Within 2 ranks of actual: {within_2}/{len(deltas)} ({within_2/len(deltas)*100:.0f}%)")


if __name__ == "__main__":
    main()
