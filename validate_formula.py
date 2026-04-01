"""DPI Formula Validation — Tests the production formula against 12 historical Atlantic hurricanes."""

import sys
import os
import math

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.dpi import compute_dpi_simple

# =============================================================================
# STORM DATA (Landfall parameters, constant 2024 USD damage)
# =============================================================================

STORMS = {
    "katrina": {
        "name": "Katrina", "year": 2005,
        "vmax_kt": 110, "pressure_hpa": 920, "fwd_kt": 11.3,
        "r34_nm": 200, "r64_nm": 90, "rmw_nm": 30,
        "lat": 29.3, "lon": -89.6, "region": "gulf_la",
        "expected_dpi_range": (80, 95),
        "damage_2024_usd_b": 200.0,  # $200B inflation-adjusted
        "observed_surge_m": 8.5, "observed_rain_mm": 300,
        "approach_angle": 20.0, "track_parallel": 0.0,
    },
    "sandy": {
        "name": "Sandy", "year": 2012,
        "vmax_kt": 70, "pressure_hpa": 940, "fwd_kt": 21.4,
        "r34_nm": 485, "r64_nm": 80, "rmw_nm": 100,
        "lat": 39.4, "lon": -74.3, "region": "atl_ne",
        "expected_dpi_range": (65, 80),
        "damage_2024_usd_b": 88.5,
        "observed_surge_m": 4.2, "observed_rain_mm": 200,
        "approach_angle": -15.0, "track_parallel": 0.60,
    },
    "harvey": {
        "name": "Harvey", "year": 2017,
        "vmax_kt": 113, "pressure_hpa": 937, "fwd_kt": 5.1,
        "r34_nm": 100, "r64_nm": 35, "rmw_nm": 15,
        "lat": 28.0, "lon": -96.9, "region": "gulf_central_tx",
        "expected_dpi_range": (68, 82),
        "damage_2024_usd_b": 160.0,
        "observed_surge_m": 3.4, "observed_rain_mm": 1539,
        "approach_angle": 15.0, "track_parallel": 0.0,
    },
    "ian": {
        "name": "Ian", "year": 2022,
        "vmax_kt": 130, "pressure_hpa": 937, "fwd_kt": 10.0,
        "r34_nm": 200, "r64_nm": 60, "rmw_nm": 20,
        "lat": 26.6, "lon": -82.2, "region": "gulf_fl_west",
        "expected_dpi_range": (70, 85),
        "damage_2024_usd_b": 119.6,
        "observed_surge_m": 5.5, "observed_rain_mm": 685,
        "approach_angle": 15.0, "track_parallel": 0.0,
    },
    "maria": {
        "name": "Maria", "year": 2017,
        "vmax_kt": 140, "pressure_hpa": 908, "fwd_kt": 9.0,
        "r34_nm": 150, "r64_nm": 40, "rmw_nm": 12,
        "lat": 18.2, "lon": -65.9, "region": "carib_pr",
        "expected_dpi_range": (75, 90),
        "damage_2024_usd_b": 115.2,
        "observed_surge_m": 2.7, "observed_rain_mm": 960,
        "approach_angle": 15.0, "track_parallel": 0.0,
    },
    "helene": {
        "name": "Helene", "year": 2024,
        "vmax_kt": 122, "pressure_hpa": 938, "fwd_kt": 21.0,
        "r34_nm": 300, "r64_nm": 80, "rmw_nm": 35,
        "lat": 29.8, "lon": -83.7, "region": "gulf_fl_panhandle",
        "expected_dpi_range": (72, 90),
        "damage_2024_usd_b": 78.7,
        "observed_surge_m": 5.0, "observed_rain_mm": 782,
        "approach_angle": 10.0, "track_parallel": 0.0,
    },
    "milton": {
        "name": "Milton", "year": 2024,
        "vmax_kt": 104, "pressure_hpa": 958, "fwd_kt": 14.0,
        "r34_nm": 250, "r64_nm": 55, "rmw_nm": 25,
        "lat": 27.2, "lon": -82.5, "region": "gulf_fl_west",
        "expected_dpi_range": (65, 85),
        "damage_2024_usd_b": 34.3,
        "observed_surge_m": 3.0, "observed_rain_mm": 518,
        "approach_angle": 5.0, "track_parallel": 0.0,
    },
    "michael": {
        "name": "Michael", "year": 2018,
        "vmax_kt": 140, "pressure_hpa": 919, "fwd_kt": 14.0,
        "r34_nm": 120, "r64_nm": 35, "rmw_nm": 12,
        "lat": 30.0, "lon": -85.5, "region": "gulf_fl_panhandle",
        "expected_dpi_range": (58, 72),
        "damage_2024_usd_b": 25.0,
        "observed_surge_m": 4.3, "observed_rain_mm": 295,
        "approach_angle": 10.0, "track_parallel": 0.0,
    },
    "irma": {
        "name": "Irma", "year": 2017,
        "vmax_kt": 113, "pressure_hpa": 931, "fwd_kt": 13.0,
        "r34_nm": 185, "r64_nm": 70, "rmw_nm": 30,
        "lat": 24.6, "lon": -81.7, "region": "atl_fl_east",
        "expected_dpi_range": (62, 78),
        "damage_2024_usd_b": 50.0,
        "observed_surge_m": 3.0, "observed_rain_mm": 550,
        "approach_angle": -10.0, "track_parallel": 0.55,
    },
    "ida": {
        "name": "Ida", "year": 2021,
        "vmax_kt": 130, "pressure_hpa": 930, "fwd_kt": 10.0,
        "r34_nm": 140, "r64_nm": 40, "rmw_nm": 15,
        "lat": 29.0, "lon": -90.1, "region": "gulf_la",
        "expected_dpi_range": (72, 88),
        "damage_2024_usd_b": 84.6,
        "observed_surge_m": 3.7, "observed_rain_mm": 400,
        "approach_angle": 15.0, "track_parallel": 0.0,
    },
    "dorian": {
        "name": "Dorian", "year": 2019,
        "vmax_kt": 160, "pressure_hpa": 910, "fwd_kt": 2.9,
        "r34_nm": 150, "r64_nm": 50, "rmw_nm": 15,
        "lat": 26.5, "lon": -78.0, "region": "carib_bahamas",
        "expected_dpi_range": (70, 85),
        "damage_2024_usd_b": 5.0,
        "observed_surge_m": 7.0, "observed_rain_mm": 900,
        "approach_angle": 12.0, "track_parallel": 0.45,
    },
    "florence": {
        "name": "Florence", "year": 2018,
        "vmax_kt": 90, "pressure_hpa": 958, "fwd_kt": 5.1,
        "r34_nm": 220, "r64_nm": 80, "rmw_nm": 45,
        "lat": 34.2, "lon": -77.9, "region": "atl_nc",
        "expected_dpi_range": (40, 58),
        "damage_2024_usd_b": 24.23,
        "observed_surge_m": 3.0, "observed_rain_mm": 913,
        "approach_angle": 10.0, "track_parallel": 0.10,
    },
}


def knots_to_ms(kt):
    return kt * 0.514444


def nm_to_m(nm):
    return nm * 1852.0


def compute_dpi(storm_key):
    """Compute DPI for a storm using the production formula."""
    s = STORMS[storm_key]
    result = compute_dpi_simple(
        vmax_ms=knots_to_ms(s["vmax_kt"]),
        min_pressure_hpa=s["pressure_hpa"],
        lat=s["lat"], lon=s["lon"],
        r34_m=nm_to_m(s["r34_nm"]) if s.get("r34_nm") else None,
        rmw_m=nm_to_m(s["rmw_nm"]) if s.get("rmw_nm") else None,
        forward_speed_ms=knots_to_ms(s["fwd_kt"]) if s.get("fwd_kt") else None,
        r64_quadrants_m=None,
        region_key=s["region"],
        storm_id=storm_key.upper(),
        name=s["name"],
        storm_year=s["year"],
        approach_angle_deg=s.get("approach_angle"),
        track_parallel_factor=s.get("track_parallel"),
        apply_land_dampening=True,
    )
    return result.dpi_score


def compute_error_percent(score, expected_range):
    """Compute percentage error from expected range midpoint."""
    mid = (expected_range[0] + expected_range[1]) / 2
    return abs(score - mid) / mid * 100


def in_range(score, expected_range):
    """Check if score is within expected range."""
    return expected_range[0] <= score <= expected_range[1]


def dpi_to_category(dpi):
    """Map DPI score to damage category."""
    if dpi < 15:
        return "Minor"
    elif dpi < 30:
        return "Moderate"
    elif dpi < 50:
        return "Severe"
    elif dpi < 70:
        return "Extreme"
    elif dpi < 85:
        return "Devastating"
    else:
        return "Catastrophic"


def print_header(text):
    """Print a major section header."""
    print("\n" + "=" * 100)
    print(f"  {text}")
    print("=" * 100)


def print_subheader(text):
    """Print a minor section header."""
    print(f"\n{'─' * 100}")
    print(f"  {text}")
    print(f"{'─' * 100}")


def main():
    """Validate the production formula against 12 historical Atlantic hurricanes."""

    # =====================================================================
    # COMPUTE DPI FOR ALL STORMS
    # =====================================================================
    print_header("DPI FORMULA VALIDATION — ALL 12 STORMS")

    dpi_scores = {}
    for storm_key in STORMS:
        dpi_scores[storm_key] = compute_dpi(storm_key)

    # =====================================================================
    # COMPREHENSIVE VALIDATION TABLE
    # =====================================================================
    print_subheader("Validation Results")

    print(f"\n{'Storm':<12} {'Year':>5} {'DPI':>7} {'Category':<15} "
          f"{'Damage ($B)':>12} {'Range':>10} {'In Range?':>10} {'Error%':>8}")
    print("-" * 100)

    in_range_count = 0
    errors = []

    for storm_key in sorted(STORMS.keys(), key=lambda k: STORMS[k]["year"], reverse=True):
        s = STORMS[storm_key]
        dpi = dpi_scores[storm_key]
        rng = s["expected_dpi_range"]
        category = dpi_to_category(dpi)
        in_rng = in_range(dpi, rng)
        error_pct = compute_error_percent(dpi, rng)

        if in_rng:
            in_range_count += 1
        errors.append(error_pct)

        ir_str = "YES" if in_rng else "NO"
        range_str = f"{rng[0]}-{rng[1]}"

        print(f"{s['name']:<12} {s['year']:>5} {dpi:>7.1f} {category:<15} "
              f"${s['damage_2024_usd_b']:>10.1f}B {range_str:>10} {ir_str:>10} {error_pct:>7.1f}%")

    # =====================================================================
    # SUMMARY STATISTICS
    # =====================================================================
    print_subheader("Summary Statistics")

    avg_error = sum(errors) / len(errors)
    max_error = max(errors)
    min_error = min(errors)

    print(f"\n  Total storms tested:     {len(STORMS)}")
    print(f"  In-range:                {in_range_count} ({in_range_count*100/len(STORMS):.0f}%)")
    print(f"  Out-of-range:            {len(STORMS) - in_range_count} ({(len(STORMS) - in_range_count)*100/len(STORMS):.0f}%)")
    print(f"\n  Average error:           {avg_error:.2f}%")
    print(f"  Max error:               {max_error:.2f}%")
    print(f"  Min error:               {min_error:.2f}%")

    # =====================================================================
    # DAMAGE-RANK CORRELATION
    # =====================================================================
    print_subheader("Damage-Rank Correlation")

    # Sort storms by damage
    by_damage = sorted(STORMS.items(), key=lambda x: x[1]["damage_2024_usd_b"], reverse=True)
    by_dpi = sorted(STORMS.items(), key=lambda x: dpi_scores[x[0]], reverse=True)

    damage_ranks = {storm_key: idx for idx, (storm_key, _) in enumerate(by_damage)}
    dpi_ranks = {storm_key: idx for idx, (storm_key, _) in enumerate(by_dpi)}

    print(f"\n{'Rank':<6} {'By Damage':<20} {'$B':>10} {'DPI Rank':>10} {'By DPI':<20} {'DPI':>8}")
    print("-" * 100)

    rank_mismatch = 0
    for damage_rank, (storm_key, _) in enumerate(by_damage):
        s = STORMS[storm_key]
        dpi = dpi_scores[storm_key]
        dpi_rank = dpi_ranks[storm_key]

        if damage_rank != dpi_rank:
            rank_mismatch += 1

        # Find which storm is at dpi_rank
        dpi_storm = [k for k, v in dpi_ranks.items() if v == damage_rank]
        dpi_storm_key = dpi_storm[0] if dpi_storm else "---"
        dpi_storm_obj = STORMS.get(dpi_storm_key, {})
        dpi_value = dpi_scores.get(dpi_storm_key, 0)

        print(f"{damage_rank+1:<6} {s['name']:<20} ${s['damage_2024_usd_b']:>8.1f}B "
              f"{dpi_rank+1:>10} {dpi_storm_obj.get('name', '---'):<20} {dpi_value:>7.1f}")

    correlation_pct = (1 - rank_mismatch / len(STORMS)) * 100
    print(f"\n  Ranking agreement:      {correlation_pct:.0f}% ({len(STORMS) - rank_mismatch}/{len(STORMS)} storms rank-matched)")

    # =====================================================================
    # NOTES & INTERPRETATION
    # =====================================================================
    print_subheader("Notes & Interpretation")

    print("""
  The formula validation shows how the production DPI formula performs against
  historical Atlantic hurricane damage. Key metrics:

  • In-Range %: Percentage of storms where DPI fell within the expected range
  • Average/Max Error: Distance from expected midpoint (lower is better)
  • Damage-Rank Correlation: Does DPI ranking match damage ranking?

  A well-calibrated formula should:
    - Achieve 70%+ in-range accuracy
    - Keep average error below 10%
    - Correlate strongly with damage ranking (>80%)

  Interpretation:
    - High error typically indicates mismatch between formula inputs and local
      factors (e.g., landfall angle, economic exposure, track duration)
    - Ranking mismatches suggest the formula weights certain hazards differently
      than historical damage patterns
    - Regional variations may be accounted for by the region_key parameter
""")

    print("\n" + "=" * 100)
    print("  VALIDATION COMPLETE")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
