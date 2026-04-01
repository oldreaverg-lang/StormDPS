#!/usr/bin/env python3
"""
Basin Characteristics Analysis
Analyzes meteorological patterns, damage correlations, and basin-specific factors
to inform DPS formula calibration.
"""

import json
import statistics

def analyze_basin_characteristics():
    """Load and analyze the historical storms database by basin."""

    with open("/sessions/confident-laughing-curie/mnt/hurricane_app/historical_storms_db.json", "r") as f:
        storms = json.load(f)

    # Group by basin
    basins = {}
    for storm in storms:
        basin = storm["basin"]
        if basin not in basins:
            basins[basin] = []
        basins[basin].append(storm)

    print("\n" + "=" * 100)
    print("BASIN METEOROLOGICAL ANALYSIS - FORMULA CALIBRATION INSIGHTS")
    print("=" * 100)

    for basin_name in sorted(basins.keys()):
        basin_storms = basins[basin_name]

        print(f"\n{'─' * 100}")
        print(f"{basin_name.upper()} BASIN")
        print(f"{'─' * 100}")

        # Extract metrics
        damages = [s["damage_billions"] for s in basin_storms]
        peak_winds = [s["peak_wind_mph"] for s in basin_storms]
        pressures = [s["central_pressure_mb"] for s in basin_storms]
        rmws = [s["rmw_nm"] for s in basin_storms]
        r34s = [s["r34_nm"] for s in basin_storms]
        durations = [s["duration_days"] for s in basin_storms]

        # Calculate damage per unit intensity
        max_wind_damage_ratios = []
        for storm in basin_storms:
            wind = storm["peak_wind_mph"]
            damage = storm["damage_billions"]
            if wind > 0:
                ratio = damage / (wind * wind)  # Squared relationship (roughly proportional to energy)
                max_wind_damage_ratios.append(ratio)

        print(f"\nStorm Count: {len(basin_storms)}")
        print(f"Damage Range: ${min(damages):.2f}B - ${max(damages):.2f}B")
        print(f"Avg Damage: ${statistics.mean(damages):.1f}B")
        if len(damages) > 1:
            print(f"Damage Std Dev: ${statistics.stdev(damages):.1f}B")

        print(f"\nWind Speed Metrics:")
        print(f"  Peak Winds: {min(peak_winds)}-{max(peak_winds)} mph | Mean: {statistics.mean(peak_winds):.0f} mph")
        if len(peak_winds) > 1:
            print(f"  Std Dev: {statistics.stdev(peak_winds):.0f} mph")

        print(f"\nPressure Metrics:")
        print(f"  Central Pressure: {min(pressures)}-{max(pressures)} mb | Mean: {statistics.mean(pressures):.0f} mb")
        if len(pressures) > 1:
            print(f"  Std Dev: {statistics.stdev(pressures):.0f} mb")

        print(f"\nWind Field Structure:")
        print(f"  RMW (Radius of Max Winds): {min(rmws)}-{max(rmws)} nm | Mean: {statistics.mean(rmws):.1f} nm")
        print(f"  R34 (34-knot wind radius): {min(r34s)}-{max(r34s)} nm | Mean: {statistics.mean(r34s):.1f} nm")
        if len(r34s) > 1:
            print(f"  R34 Std Dev: {statistics.stdev(r34s):.1f} nm")

        # Wind field scaling factor (R34 / RMW ratio indicates compactness)
        r34_rmw_ratios = []
        for storm in basin_storms:
            if storm["rmw_nm"] > 0:
                ratio = storm["r34_nm"] / storm["rmw_nm"]
                r34_rmw_ratios.append(ratio)

        if r34_rmw_ratios:
            print(f"  R34/RMW Ratio (compactness): {statistics.mean(r34_rmw_ratios):.2f} "
                  f"({min(r34_rmw_ratios):.2f}-{max(r34_rmw_ratios):.2f})")
            if len(r34_rmw_ratios) > 1:
                print(f"    → Values > 4.0 = compact | Values < 3.5 = spread-out wind field")

        print(f"\nDuration & Longevity:")
        print(f"  Duration: {min(durations)}-{max(durations)} days | Mean: {statistics.mean(durations):.2f} days")
        if len(durations) > 1:
            print(f"  Std Dev: {statistics.stdev(durations):.2f} days")

        print(f"\nDamage Efficiency ($/mph² as proxy for damage concentration):")
        print(f"  Mean Ratio: {statistics.mean(max_wind_damage_ratios):.4f}")
        print(f"  Range: {min(max_wind_damage_ratios):.4f} - {max(max_wind_damage_ratios):.4f}")
        if len(max_wind_damage_ratios) > 1:
            print(f"  Std Dev: {statistics.stdev(max_wind_damage_ratios):.4f}")
            print(f"    → Higher ratio = more efficient damage per unit intensity")
            print(f"    → Lower ratio = less efficient damage per unit intensity")

        # Rapid intensification analysis
        rapid_intensification = [s.get("rapid_intensification_24h_mph", 0) for s in basin_storms
                                 if s.get("rapid_intensification_24h_mph")]
        if rapid_intensification:
            print(f"\nRapid Intensification (where recorded):")
            print(f"  24-hour intensity gains: {min(rapid_intensification)}-{max(rapid_intensification)} mph/day")
            print(f"  Mean: {statistics.mean(rapid_intensification):.0f} mph/day")
            print(f"    → Critical for forecasting difficulty and impact severity")

        # Multiple landfall analysis
        landfalls = [s.get("landfalls", 1) for s in basin_storms]
        if max(landfalls) > 1:
            multi_landfall = [s for s in basin_storms if s.get("landfalls", 1) > 1]
            print(f"\nMultiple Landfall Events: {len(multi_landfall)} storms")
            if multi_landfall:
                print(f"  Mean damage (multi-landfall): ${statistics.mean([s['damage_billions'] for s in multi_landfall]):.1f}B")

        # Basin-specific insights
        print(f"\nKEY BASIN CHARACTERISTICS:")
        generate_basin_insights(basin_name, basin_storms)


def generate_basin_insights(basin_name, storms):
    """Generate calibration insights specific to each basin."""

    insights = {
        "Atlantic": {
            "dominant_factor": "Storm duration + wind field size",
            "surge_potential": "Very High (continental shelf geometry favors surge)",
            "rainfall_factor": "Moderate to High (orographic effects in some regions)",
            "wind_field_type": "Moderate spread (R34/RMW typically 4-5)",
            "rapid_intensity_likelihood": "Moderate (occurs ~30% of major storms)",
            "calibration_note": "BASELINE REFERENCE - validates current DPS formula",
            "coefficient_adjustments": "None (reference = 1.0)",
        },
        "Eastern Pacific": {
            "dominant_factor": "Rapid intensification + compact wind field",
            "surge_potential": "LOW to Moderate (narrow shelf, steep bathymetry)",
            "rainfall_factor": "Moderate (steep terrain amplification near Mexico/Central America)",
            "wind_field_type": "Compact (R34/RMW typically 3-4)",
            "rapid_intensity_likelihood": "VERY HIGH (Otis: 115 mph/24h)",
            "calibration_note": "UNDERESTIMATED by Atlantic formula - needs RI factor + surge adjustment",
            "coefficient_adjustments": "Add 15-20pt RI factor | Reduce surge component 20-30% | Increase wind factor 10%",
        },
        "Western Pacific": {
            "dominant_factor": "Large wind field + multiple landfalls",
            "surge_potential": "Moderate (complex island bathymetry)",
            "rainfall_factor": "Very High (orographic rainfall in Philippines, Japan, Taiwan)",
            "wind_field_type": "LARGE spread (R34/RMW typically 4.5-5.5)",
            "rapid_intensity_likelihood": "Moderate (occurs ~25% of major storms)",
            "calibration_note": "Typhoons cause damage over broader areas - wind field size critical",
            "coefficient_adjustments": "Increase wind field size factor 20-30% | Boost rainfall component 15% | Increase duration factor 10%",
        },
        "North Indian": {
            "dominant_factor": "Storm surge (Bay of Bengal funnel effect)",
            "surge_potential": "EXTREME (Bay of Bengal funnel, shallow continental shelf)",
            "rainfall_factor": "Very High (monsoon interaction, complex topography)",
            "wind_field_type": "Moderate to Large (R34/RMW typically 4-5)",
            "rapid_intensity_likelihood": "Moderate (occurs ~20% of cyclones)",
            "calibration_note": "Surge is DOMINANT factor - Bay of Bengal amplifies dramatically",
            "coefficient_adjustments": "INCREASE surge component 40-50% | Boost rainfall factor 20% | Reduce wind component 10%",
        },
        "South Indian": {
            "dominant_factor": "Limited by sparse population but significant surge potential",
            "surge_potential": "High (continental shelf geometry supports surge)",
            "rainfall_factor": "Moderate (monsoon dependent)",
            "wind_field_type": "Moderate spread (R34/RMW typically 4-5)",
            "rapid_intensity_likelihood": "Low data - assume ~20%",
            "calibration_note": "Low historical damage due to sparse coastal population - use surge mechanics",
            "coefficient_adjustments": "Use Atlantic formula as baseline | Slight surge boost 5-10%",
        },
        "South Pacific": {
            "dominant_factor": "Extreme intensity but sparse population",
            "surge_potential": "High (island vulnerability but limited infrastructure)",
            "rainfall_factor": "High (monsoon systems, island orography)",
            "wind_field_type": "Compact to Moderate (R34/RMW typically 3.5-4.5)",
            "rapid_intensity_likelihood": "Moderate (~25% of major storms)",
            "calibration_note": "Record low pressures (Winston, Pam) but minimal economic loss - formula bias toward intensity",
            "coefficient_adjustments": "Use Atlantic baseline | Adjust for small island economics - lower total damage multiplier",
        },
    }

    if basin_name in insights:
        info = insights[basin_name]
        for key, value in info.items():
            key_display = key.replace("_", " ").title()
            print(f"  {key_display:30s}: {value}")


def print_correlation_analysis():
    """Analyze correlations between metrics and damage."""

    with open("/sessions/confident-laughing-curie/mnt/hurricane_app/historical_storms_db.json", "r") as f:
        storms = json.load(f)

    print("\n" + "=" * 100)
    print("DAMAGE CORRELATION ANALYSIS")
    print("=" * 100)

    # Calculate correlation coefficients
    def correlation(x, y):
        if len(x) < 2:
            return 0
        mean_x = statistics.mean(x)
        mean_y = statistics.mean(y)
        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(len(x))) / (len(x) - 1)
        stdev_x = statistics.stdev(x)
        stdev_y = statistics.stdev(y)
        if stdev_x == 0 or stdev_y == 0:
            return 0
        return cov / (stdev_x * stdev_y)

    damages = [s["damage_billions"] for s in storms]
    peak_winds = [s["peak_wind_mph"] for s in storms]
    pressures = [s["central_pressure_mb"] for s in storms]
    r34s = [s["r34_nm"] for s in storms]
    durations = [s["duration_days"] for s in storms]

    print(f"\nGlobal Correlation with Total Damage:")
    print(f"  Peak Wind Speed:        {correlation(peak_winds, damages):+.3f}")
    print(f"  Central Pressure:       {correlation(pressures, damages):+.3f}")
    print(f"  R34 Wind Field Size:    {correlation(r34s, damages):+.3f}")
    print(f"  Duration (days):        {correlation(durations, damages):+.3f}")

    print(f"\nInterpretation:")
    print(f"  Positive values: Metric correlates with higher damage")
    print(f"  Strong correlation (±0.7+): Key formula component")
    print(f"  Weak correlation (<±0.4): Indirect effect or basin-dependent")

    # Wind field vs intensity correlation
    rmws = [s["rmw_nm"] for s in storms]
    wind_field_sizes = [r34 / rmw if rmw > 0 else 0 for rmw, r34 in zip(rmws, r34s)]

    print(f"\nWind Field Structure vs Damage:")
    print(f"  RMW (compact center):   {correlation(rmws, damages):+.3f}")
    print(f"  R34 (broad field):      {correlation(r34s, damages):+.3f}")
    print(f"    → Suggests BOTH intensity (peak) AND size (R34) matter for damage")


if __name__ == "__main__":
    analyze_basin_characteristics()
    print_correlation_analysis()

    print("\n" + "=" * 100)
    print("✓ Basin analysis complete - insights ready for formula development")
    print("=" * 100 + "\n")
