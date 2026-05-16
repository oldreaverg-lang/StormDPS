"""Build the rainfall estimator systematic-bias audit.

Cross-checks the engine's `rainfall_est_mm` (a stall-hour heuristic
in core/rainfall_warning.py) against the published peak observed
rainfall (peak_rainfall_in field in core/ground_truth.py) for every
storm where both values are available.

Surfaces:
  - Per-storm ratio = estimated_mm / observed_mm
  - Systematic bias direction + magnitude
  - Correlation of bias with storm properties (stall_hours, peak_wind,
    IKE, snapshot_count) to identify which storm archetypes drive the
    over-estimation
  - Calibration recommendation

Idempotent — rerun against the live bundle + ground_truth.
"""
import sys
import json
import csv
from statistics import mean, median, stdev
sys.path.insert(0, '.')

from core import ground_truth as gt

with open('frontend/compiled_bundle.json', encoding='utf-8') as f:
    bundle = json.load(f)
storms_b = bundle['storms']

# Collect storms with both observed (ground_truth) and estimated (bundle)
rows = []
for sid, r in gt.all_records().items():
    if r.peak_rainfall_in is None:
        continue
    obs_mm = r.peak_rainfall_in * 25.4
    storm = storms_b.get(sid)
    if not storm:
        # Storm not in bundle (most EP storms aren't yet)
        rows.append({
            'storm_id': sid, 'name': r.name, 'year': r.year,
            'in_bundle': False,
            'obs_in': r.peak_rainfall_in,
            'obs_mm': round(obs_mm, 1),
            'est_mm': None, 'ratio': None,
            'rainfall_level': None,
            'rainfall_warning': None,
            'stall_hours': None, 'peak_wind_kt': None, 'peak_ike_tj': None,
            'snapshot_count': None,
        })
        continue
    est_mm = storm.get('rainfall_est_mm', 0)
    ratio = est_mm / obs_mm if obs_mm > 0 else None
    rows.append({
        'storm_id': sid, 'name': r.name, 'year': r.year,
        'in_bundle': True,
        'obs_in': r.peak_rainfall_in,
        'obs_mm': round(obs_mm, 1),
        'est_mm': est_mm,
        'ratio': round(ratio, 2) if ratio else None,
        'rainfall_level': storm.get('rainfall_level'),
        'rainfall_warning': storm.get('rainfall_warning'),
        'stall_hours': storm.get('stall_hours', 0),
        'peak_wind_kt': storm.get('peak_wind_kt', 0),
        'peak_ike_tj': storm.get('peak_ike_tj', 0),
        'snapshot_count': storm.get('snapshot_count', 0),
    })

# Sort by year for the CSV
rows.sort(key=lambda x: x['year'])

# Stats over storms WITH bundle data
valid = [r for r in rows if r['in_bundle'] and r['ratio'] is not None]
ratios = [r['ratio'] for r in valid]
over_storms = [r for r in valid if r['ratio'] > 1.1]   # > 10% over-estimate
under_storms = [r for r in valid if r['ratio'] < 0.9]  # > 10% under-estimate
on_target = [r for r in valid if 0.9 <= r['ratio'] <= 1.1]

stats = {
    'storms_analyzed': len(valid),
    'storms_skipped_not_in_bundle': len(rows) - len(valid),
    'ratio_mean': round(mean(ratios), 2),
    'ratio_median': round(median(ratios), 2),
    'ratio_stdev': round(stdev(ratios), 2) if len(ratios) > 1 else 0,
    'ratio_min': round(min(ratios), 2),
    'ratio_max': round(max(ratios), 2),
    'storms_over_estimated_10pct': len(over_storms),
    'storms_under_estimated_10pct': len(under_storms),
    'storms_on_target_within_10pct': len(on_target),
}

# Bias-by-archetype: does overshoot correlate with stall_hours / peak_wind / IKE?
# Pearson correlation between ratio and each predictor.
def pearson(xs, ys):
    n = len(xs)
    if n < 2: return 0.0
    mx, my = mean(xs), mean(ys)
    num = sum((xs[i]-mx) * (ys[i]-my) for i in range(n))
    dx = (sum((x-mx)**2 for x in xs)) ** 0.5
    dy = (sum((y-my)**2 for y in ys)) ** 0.5
    if dx * dy == 0: return 0.0
    return num / (dx * dy)

ratios_v = [r['ratio'] for r in valid]
correlations = {
    'ratio_vs_stall_hours': round(pearson([r['stall_hours'] for r in valid], ratios_v), 3),
    'ratio_vs_peak_wind_kt': round(pearson([r['peak_wind_kt'] for r in valid], ratios_v), 3),
    'ratio_vs_peak_ike_tj': round(pearson([r['peak_ike_tj'] for r in valid], ratios_v), 3),
    'ratio_vs_year': round(pearson([r['year'] for r in valid], ratios_v), 3),
    'ratio_vs_observed_mm': round(pearson([r['obs_mm'] for r in valid], ratios_v), 3),
}

# Write CSV
with open('audits/rainfall_estimator/rainfall_comparison.csv', 'w',
          encoding='utf-8', newline='') as f:
    w = csv.DictWriter(f, fieldnames=[
        'storm_id', 'name', 'year', 'in_bundle',
        'obs_in', 'obs_mm', 'est_mm', 'ratio',
        'rainfall_level', 'rainfall_warning',
        'stall_hours', 'peak_wind_kt', 'peak_ike_tj', 'snapshot_count',
    ])
    w.writeheader()
    for row in rows:
        w.writerow(row)

# Write JSON summary
summary = {
    'title': 'StormDPS rainfall estimator systematic-bias audit',
    'date': '2026-05-15',
    'description': (
        "Cross-check of the engine's rainfall_est_mm (stall-hour heuristic "
        "in core/rainfall_warning.py) against published peak observed "
        "rainfall (core/ground_truth.peak_rainfall_in) for every storm "
        "with both values available in the compiled bundle."
    ),
    'observed_data_source': (
        "core/ground_truth.py — NHC TCRs (Bucci, Stewart, Berg, Blake, "
        "Knabb, Beven, Cangialosi, etc.), NWS WFOs, USGS STN, NOAA NCEI"
    ),
    'engine_estimator': (
        "core/rainfall_warning.py compute_rainfall_warning — stall-hour "
        "heuristic with per-hour rainfall rate constants. Not yet "
        "calibrated against ground_truth corpus."
    ),
    'stats': stats,
    'correlations_with_ratio': correlations,
    'per_storm': rows,
    'pattern_summary': {
        'over_estimated_storms': [
            {'name': r['name'], 'year': r['year'], 'ratio': r['ratio'],
             'obs_mm': r['obs_mm'], 'est_mm': r['est_mm'],
             'stall_hours': r['stall_hours']}
            for r in sorted(over_storms, key=lambda x: -x['ratio'])
        ],
        'under_estimated_storms': [
            {'name': r['name'], 'year': r['year'], 'ratio': r['ratio'],
             'obs_mm': r['obs_mm'], 'est_mm': r['est_mm'],
             'stall_hours': r['stall_hours']}
            for r in sorted(under_storms, key=lambda x: x['ratio'])
        ],
        'on_target_storms': [
            {'name': r['name'], 'year': r['year'], 'ratio': r['ratio'],
             'obs_mm': r['obs_mm'], 'est_mm': r['est_mm']}
            for r in on_target
        ],
    },
    'caveats': [
        (
            "Bundle was compiled before the observed-rainfall override "
            "hook in core/dps_engine.py:L112-129 ran for these storms. "
            "The override would replace rain_result.estimated_total_mm "
            "with the ground_truth value at compile time. Once the "
            "bundle is recompiled, the bias measured here disappears "
            "for storms with ground_truth entries — but the underlying "
            "estimator (used for storms without ground_truth) still has "
            "the systematic over-estimation problem documented here."
        ),
        (
            "Five EP storms in ground_truth (Patricia, Lane, Otis, "
            "Hilary, John) aren't in the compiled bundle yet, so they "
            "can't be cross-checked against an engine estimate. "
            "Documented in EP_DPS_AUDIT.md followup #1."
        ),
    ],
}

with open('audits/rainfall_estimator/rainfall_audit_summary.json', 'w',
          encoding='utf-8') as f:
    json.dump(summary, f, indent=2)

# Print summary to stdout
print('Wrote audits/rainfall_estimator/rainfall_comparison.csv')
print('Wrote audits/rainfall_estimator/rainfall_audit_summary.json')
print()
print('=== Storms with both observed + engine estimate ===')
print(f"{'Storm':<10} {'Year':>4} {'obs_mm':>7} {'est_mm':>7} {'ratio':>6}  "
      f"{'stall_h':>7} {'peak_kt':>7}")
print('-' * 75)
for r in valid:
    print(f"{r['name']:<10} {r['year']:>4} {r['obs_mm']:>7.0f} "
          f"{r['est_mm']:>7.0f} {r['ratio']:>6.2f}  "
          f"{r['stall_hours']:>7.0f} {r['peak_wind_kt']:>7.0f}")
print()
print('=== Aggregate stats ===')
for k, v in stats.items():
    print(f'  {k}: {v}')
print()
print('=== Bias correlations (Pearson) ===')
for k, v in correlations.items():
    print(f'  {k}: {v:+.3f}')
print()
print('=== Pattern ===')
print(f'  Over-estimated (ratio > 1.1):   {len(over_storms)} storms')
for r in sorted(over_storms, key=lambda x: -x['ratio']):
    print(f'    {r["name"]:<10} {r["year"]}  '
          f'{r["est_mm"]:>5.0f} mm vs obs {r["obs_mm"]:>5.0f} mm  '
          f'(ratio {r["ratio"]:.2f}, stall {r["stall_hours"]:.0f}h)')
print(f'  On-target (0.9 - 1.1):          {len(on_target)} storms')
for r in on_target:
    print(f'    {r["name"]:<10} {r["year"]}  ratio {r["ratio"]:.2f}')
print(f'  Under-estimated (ratio < 0.9):  {len(under_storms)} storms')
for r in sorted(under_storms, key=lambda x: x["ratio"]):
    print(f'    {r["name"]:<10} {r["year"]}  '
          f'{r["est_mm"]:>5.0f} mm vs obs {r["obs_mm"]:>5.0f} mm  '
          f'(ratio {r["ratio"]:.2f})')
