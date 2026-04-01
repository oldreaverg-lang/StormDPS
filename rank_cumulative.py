"""
Rank all preloaded storms by cumulative DPI and compare to single-snapshot rankings.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.cumulative_dpi import compute_all_from_bundle

# Actual damage for believability check (NOAA NCEI CPI-adjusted 2024 USD)
ACTUAL_DAMAGE = {
    "Katrina": 198.0,
    "Harvey": 160.0,
    "Maria": 115.0,
    "Ian": 113.0,
    "Sandy": 90.0,
    "Ida": 84.0,   # Not in preload bundle but keep for reference
    "Irma": 80.0,
    "Helene": 79.0,
    "Andrew": 55.0,
    "Ike": 44.0,
    "Milton": 34.0,
    "Michael": 32.0,
    "Florence": 29.0,
    "Beryl": 6.0,
    "Dorian": 5.0,
    "Idalia": 4.0,
}


def main():
    results = compute_all_from_bundle()

    print("=" * 130)
    print("CUMULATIVE DPI RANKINGS — ALL PRESET STORMS")
    print("Multi-snapshot analysis: peak DPI × (1 + duration_bonus + breadth_bonus)")
    print("=" * 130)
    print()

    print(f"{'#':<3} {'Storm':<12} {'Year':>4}  {'CumDPI':>6}  {'Category':<14}  "
          f"{'PeakDPI':>7}  {'Dur+':>6}  {'Brd+':>6}  "
          f"{'CoastHr':>7}  {'TotalHr':>7}  {'PkIKE':>6}  {'Snaps':>5}  "
          f"{'Act$B':>7}")
    print("─" * 130)

    for i, r in enumerate(results, 1):
        actual = ACTUAL_DAMAGE.get(r.storm_name, 0)
        actual_str = f"${actual:.0f}B" if actual else "N/A"
        dur_pct = f"+{r.duration_factor*100:.0f}%"
        brd_pct = f"+{r.breadth_factor*100:.0f}%"
        print(f"{i:<3} {r.storm_name:<12} {r.storm_year:>4}  {r.cum_dpi:>6.1f}  {r.cum_category:<14}  "
              f"{r.peak_dpi:>7.1f}  {dur_pct:>6}  {brd_pct:>6}  "
              f"{r.total_coastal_hours:>7.1f}  {r.total_track_hours:>7.1f}  {r.peak_ike_tj:>6.1f}  "
              f"{r.snapshots_computed:>5}  {actual_str:>7}")

    # Comparison: single-snapshot vs cumulative rankings
    print("\n\n" + "=" * 130)
    print("RANKING COMPARISON: Single-Snapshot DPI vs Cumulative DPI vs Actual Damage")
    print("=" * 130)

    # Sort by actual damage
    damage_sorted = sorted(results, key=lambda r: -ACTUAL_DAMAGE.get(r.storm_name, 0))
    single_sorted = sorted(results, key=lambda r: -r.peak_dpi)
    cum_sorted = sorted(results, key=lambda r: -r.cum_dpi)

    name_to_single_rank = {r.storm_name: i+1 for i, r in enumerate(single_sorted)}
    name_to_cum_rank = {r.storm_name: i+1 for i, r in enumerate(cum_sorted)}
    name_to_dmg_rank = {r.storm_name: i+1 for i, r in enumerate(damage_sorted)}

    print(f"\n{'Storm':<12} {'DmgRank':>7}  {'Act$B':>7}  "
          f"{'SnglRank':>8}  {'PeakDPI':>7}  {'Δ':>4}  "
          f"{'CumRank':>7}  {'CumDPI':>7}  {'Δ':>4}  Improvement")
    print("─" * 110)

    total_single_delta = 0
    total_cum_delta = 0

    for r in damage_sorted:
        actual = ACTUAL_DAMAGE.get(r.storm_name, 0)
        if not actual:
            continue
        dr = name_to_dmg_rank[r.storm_name]
        sr = name_to_single_rank[r.storm_name]
        cr = name_to_cum_rank[r.storm_name]
        sd = sr - dr
        cd = cr - dr

        total_single_delta += abs(sd)
        total_cum_delta += abs(cd)

        improved = ""
        if abs(cd) < abs(sd):
            improved = f"✓ Improved by {abs(sd) - abs(cd)} rank(s)"
        elif abs(cd) > abs(sd):
            improved = f"✗ Worse by {abs(cd) - abs(sd)} rank(s)"
        else:
            improved = "— Same"

        print(f"{r.storm_name:<12} #{dr:>6}  ${actual:>5.0f}B  "
              f"#{sr:>7}  {r.peak_dpi:>7.1f}  {sd:>+3}  "
              f"#{cr:>6}  {r.cum_dpi:>7.1f}  {cd:>+3}  {improved}")

    n = sum(1 for r in results if ACTUAL_DAMAGE.get(r.storm_name, 0) > 0)
    print(f"\n  Mean |Δ| single-snapshot: {total_single_delta/n:.1f}")
    print(f"  Mean |Δ| cumulative:      {total_cum_delta/n:.1f}")
    improvement = total_single_delta - total_cum_delta
    print(f"  Net improvement:          {improvement:+.1f} total rank positions closer")

    # Show which storms got the biggest boosts
    print("\n\n" + "=" * 130)
    print("DURATION & BREADTH BREAKDOWN")
    print("=" * 130)
    print(f"\n{'Storm':<12} {'CoastHr':>7}  {'DPI>30 Hr':>9}  {'PkIKE TJ':>8}  "
          f"{'Dur Bonus':>9}  {'Brd Bonus':>9}  {'Total Boost':>11}  {'Note'}")
    print("─" * 100)

    for r in sorted(results, key=lambda x: -(x.duration_factor + x.breadth_factor)):
        total_boost = r.duration_factor + r.breadth_factor
        note = ""
        if r.duration_factor >= 0.30:
            note = "PROLONGED STALL"
        elif r.duration_factor >= 0.15:
            note = "Extended exposure"
        if r.breadth_factor >= 0.20:
            note += " + MASSIVE WIND FIELD" if note else "MASSIVE WIND FIELD"
        elif r.breadth_factor >= 0.10:
            note += " + Large field" if note else "Large field"

        print(f"{r.storm_name:<12} {r.total_coastal_hours:>7.1f}  "
              f"{r.coastal_snapshots * 6:>9.0f}  {r.peak_ike_tj:>8.1f}  "
              f"+{r.duration_factor*100:>7.1f}%  +{r.breadth_factor*100:>7.1f}%  "
              f"+{total_boost*100:>9.1f}%  {note}")


if __name__ == "__main__":
    main()
