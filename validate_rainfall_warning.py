#!/usr/bin/env python3
"""
Validate the Rainfall Anomaly Warning module against known storms.

Expected outcomes:
  - Harvey (2017): HIGHEST score (90+ range) — 5-day stall over Houston
  - Florence (2018): HIGH score (50-70) — slow-moving, NC river basins
  - Helene (2024): HIGH score (50-70) — Appalachian terrain amplification
  - Katrina (2005): MODERATE (30-50) — fast landfall but massive rain shield
  - Milton (2024): LOW-MODERATE (20-40) — fast-moving, brief landfall
  - Beryl (2024): LOW (< 30) — fast transit, minimal stalling
  - Michael (2018): LOW (< 25) — fast Cat 5, minimal rain threat
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from core.rainfall_warning import compute_all_from_bundle

def main():
    print("=" * 80)
    print("RAINFALL ANOMALY WARNING — VALIDATION")
    print("Red Bar Warning Scores for All Preloaded Storms")
    print("=" * 80)

    results = compute_all_from_bundle()

    # Display table
    print(f"\n{'Rank':<5} {'Storm':<22} {'Score':>6} {'Level':<10} "
          f"{'Stall':>6} {'Moist':>6} {'Terr':>5} {'Basin':>6} "
          f"{'StallH':>7} {'SlowH':>7} {'Est mm':>7} {'Anom?':<5}")
    print("-" * 105)

    for rank, r in enumerate(results, 1):
        w = r["warning"]
        anom = "YES" if w.is_anomalous else "no"
        print(f"{rank:<5} {r['storm_name']:<22} {w.warning_score:>6.1f} {w.warning_level:<10} "
              f"{w.stall_factor:>6.1f} {w.moisture_factor:>6.1f} {w.terrain_factor:>5.1f} "
              f"{w.basin_factor:>6.1f} {w.total_stall_hours:>7.1f} {w.total_slow_hours:>7.1f} "
              f"{w.estimated_total_mm:>7.0f} {anom:<5}")

    # Detail for anomalous storms
    print("\n" + "=" * 80)
    print("DETAILED WARNINGS FOR ANOMALOUS STORMS (score >= 25)")
    print("=" * 80)

    for r in results:
        w = r["warning"]
        if not w.is_anomalous:
            continue

        print(f"\n{'─' * 60}")
        print(f"  {r['storm_name']} — Score: {w.warning_score:.1f} ({w.warning_level})")
        print(f"{'─' * 60}")
        print(f"  Stall:    {w.stall_factor:>5.1f}/40  "
              f"({w.total_stall_hours:.0f}h stall, {w.total_slow_hours:.0f}h slow)")
        print(f"  Moisture: {w.moisture_factor:>5.1f}/30  "
              f"(peak rain rate {w.peak_rain_rate_mmhr:.0f} mm/hr)")
        print(f"  Terrain:  {w.terrain_factor:>5.1f}/15  "
              f"({', '.join(w.affected_terrain) if w.affected_terrain else 'none'})")
        print(f"  Basin:    {w.basin_factor:>5.1f}/15  "
              f"({', '.join(w.affected_basins) if w.affected_basins else 'none'})")
        print(f"  Est rain: {w.estimated_total_mm:.0f} mm ({w.estimated_total_mm/25.4:.1f} in)")
        print(f"\n  Warning text:")
        # Wrap text at 70 chars
        text = w.warning_text
        while len(text) > 70:
            wrap_at = text[:70].rfind(' ')
            if wrap_at < 0:
                wrap_at = 70
            print(f"    {text[:wrap_at]}")
            text = text[wrap_at:].strip()
        if text:
            print(f"    {text}")

    # Believability check
    print("\n" + "=" * 80)
    print("BELIEVABILITY CHECK")
    print("=" * 80)

    # Expected ranking (rough):
    expected_top = ["Harvey", "Florence", "Helene"]
    expected_low = ["Michael", "Beryl", "Milton"]

    scored = {r["storm_name"]: r["warning"].warning_score for r in results}
    top_3 = [r["storm_name"] for r in results[:3]]

    print(f"\n  Top 3 rainfall threats: {', '.join(top_3)}")
    print(f"  Expected top threats:   Harvey, Florence/Helene (interchangeable)")

    harvey_score = next((s for name, s in scored.items() if "Harvey" in name), 0)
    print(f"\n  Harvey score: {harvey_score:.1f} — {'PASS' if harvey_score >= 60 else 'NEEDS TUNING'} (expect 60+)")

    helene_score = next((s for name, s in scored.items() if "Helene" in name), 0)
    print(f"  Helene score: {helene_score:.1f} — {'PASS' if helene_score >= 30 else 'NEEDS TUNING'} (expect 30+)")

    michael_score = next((s for name, s in scored.items() if "Michael" in name), 0)
    print(f"  Michael score: {michael_score:.1f} — {'PASS' if michael_score < 30 else 'NEEDS TUNING'} (expect < 30)")


if __name__ == "__main__":
    main()
