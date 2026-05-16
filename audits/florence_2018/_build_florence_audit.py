"""Build the three audit artifacts for Hurricane Florence (AL062018).

Mirrors audits/ian_2022/_build_intermediate.py — joins HURDAT2 best-track
with bundle's per-snapshot DPI, then reconstructs the stage-by-stage
formula trace from bundle field values.

v2 (post-ChatGPT-review): adds
  - per-snapshot rainfall logic (near_land + stall/slow flags + cumulative
    effective_rain_hours + cumulative_rainfall_estimate)
  - rainfall estimator internal-formula decomposition
  - 5-storm comparison (Ian, Harvey, Michael, Sandy, Ida vs Florence)
  - persistence-vs-coefficient framing in the markdown

Idempotent: rerun produces identical artifacts from the same inputs.
"""
import csv
import json
import math
from datetime import datetime

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

HURDAT = 'C:/Users/Ryan/APPS/StormDPS/.claude/worktrees/beautiful-moser-492ac9/data/hurdat2.txt'
BUNDLE = 'frontend/compiled_bundle.json'
OUT_CSV = 'audits/florence_2018/florence_snapshots.csv'
OUT_JSON = 'audits/florence_2018/florence_intermediate.json'

# Constants from core/rainfall_warning.py (kept in sync)
KT_TO_MS = 0.514444
NM_TO_M = 1852.0
STALL_SPEED_KT = 5.0
SLOW_SPEED_KT = 8.0
STALL_REF_HOURS = 48.0

# Land bounding boxes (rainfall_warning.LAND_BOXES)
LAND_BOXES = [
    (24.5, 49.0, -98.0, -66.0, "CONUS"),
    (17.0, 19.5, -68.0, -64.0, "PR/USVI"),
    (21.0, 27.5, -80.0, -72.0, "Bahamas"),
]

# Terrain enhancement zones (rainfall_warning.TERRAIN_ZONES)
TERRAIN_ZONES = [
    (34.0, 37.5, -84.0, -79.0, 1.8, "Southern Appalachians"),
    (37.0, 41.0, -82.0, -76.0, 1.5, "Central Appalachians"),
    (33.0, 36.0, -86.0, -83.0, 1.3, "Alabama/GA highlands"),
    (17.5, 18.5, -67.0, -65.5, 1.6, "PR Cordillera Central"),
    (33.0, 36.5, -80.0, -77.0, 1.2, "Carolina Piedmont"),
]

# River basin compound flooding zones (rainfall_warning.RIVER_BASIN_ZONES)
RIVER_BASIN_ZONES = [
    (27.5, 30.5, -97.5, -93.5, 1.8, "Houston / Buffalo Bayou-San Jacinto"),
    (33.5, 35.5, -80.0, -77.0, 1.5, "NC Cape Fear / Neuse / Tar"),
    (29.5, 31.5, -92.0, -89.0, 1.4, "Louisiana Mississippi Basin"),
    (37.0, 40.0, -77.5, -74.0, 1.3, "Mid-Atlantic Potomac / Delaware"),
    (25.5, 27.5, -81.5, -80.0, 1.2, "SW FL Peace / Caloosahatchee"),
    (34.0, 37.0, -85.5, -81.0, 1.6, "TN/NC French Broad / Pigeon"),
    (30.5, 34.5, -85.0, -82.0, 1.3, "GA/AL Chattahoochee / Flint"),
]


def is_near_land(lat, lon):
    for lo, hi, wlo, whi, _ in LAND_BOXES:
        if lo <= lat <= hi and wlo <= lon <= whi:
            return True
    return False


def check_terrain(lat, lon):
    for lo, hi, wlo, whi, enh, label in TERRAIN_ZONES:
        if lo <= lat <= hi and wlo <= lon <= whi:
            return label, enh
    return None, 1.0


def check_basin(lat, lon):
    for lo, hi, wlo, whi, fac, label in RIVER_BASIN_ZONES:
        if lo <= lat <= hi and wlo <= lon <= whi:
            return label, fac
    return None, 1.0


def haversine_nm(lat1, lon1, lat2, lon2):
    R = 3440.065
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Pull HURDAT2 best-track for Florence
# ---------------------------------------------------------------------------

with open(HURDAT, encoding='utf-8') as f:
    lines = f.read().split('\n')

i = 0
hurdat_rows = []
while i < len(lines):
    if lines[i].startswith('AL062018'):
        n = int(lines[i].split(',')[2])
        for j in range(1, n+1):
            parts = [p.strip() for p in lines[i+j].split(',')]
            date_s, time_s, record_id, status, lat_s, lon_s, vmax_s, pres_s = parts[:8]
            r34 = [int(parts[8+k]) for k in range(4)]
            r50 = [int(parts[12+k]) for k in range(4)]
            r64 = [int(parts[16+k]) for k in range(4)]
            try:
                rmw = int(parts[20])
                if rmw == -999:
                    rmw = None
            except (IndexError, ValueError):
                rmw = None
            lat = float(lat_s[:-1]) * (1 if lat_s[-1] == 'N' else -1)
            lon = float(lon_s[:-1]) * (-1 if lon_s[-1] == 'W' else 1)
            timestamp = f'{date_s[:4]}-{date_s[4:6]}-{date_s[6:8]}T{time_s[:2]}:{time_s[2:]}:00'
            hurdat_rows.append({
                'time_utc': timestamp, 'lat': lat, 'lon': lon,
                'record_id': record_id, 'status': status,
                'max_wind_kt': int(vmax_s),
                'min_pressure_mb': int(pres_s),
                'r34_ne_nm': r34[0], 'r34_se_nm': r34[1],
                'r34_sw_nm': r34[2], 'r34_nw_nm': r34[3],
                'r50_ne_nm': r50[0], 'r50_se_nm': r50[1],
                'r50_sw_nm': r50[2], 'r50_nw_nm': r50[3],
                'r64_ne_nm': r64[0], 'r64_se_nm': r64[1],
                'r64_sw_nm': r64[2], 'r64_nw_nm': r64[3],
                'rmw_nm': rmw if rmw else '',
                'max_r34_nm': max(r34),
            })
        break
    i += 1


# ---------------------------------------------------------------------------
# Pull bundle data
# ---------------------------------------------------------------------------

with open(BUNDLE, encoding='utf-8') as f:
    bundle = json.load(f)
flo = bundle['storms']['AL062018']
dpi_by_time = {pt['t']: pt['dpi'] for pt in flo['dpi_timeseries']}


# ---------------------------------------------------------------------------
# Per-snapshot enrichment: forward speed, DPI, rainfall classification,
# cumulative rainfall accumulation under the estimator's logic
# ---------------------------------------------------------------------------

# First pass: forward speed + DPI
for i, row in enumerate(hurdat_rows):
    next_syn = next(
        (r for r in hurdat_rows[i+1:] if r['record_id'] != 'L'),
        None,
    )
    if next_syn and row['record_id'] != 'L':
        dist = haversine_nm(row['lat'], row['lon'], next_syn['lat'], next_syn['lon'])
        t1 = datetime.fromisoformat(row['time_utc'])
        t2 = datetime.fromisoformat(next_syn['time_utc'])
        dt_h = (t2 - t1).total_seconds() / 3600
        row['forward_speed_kt'] = round(dist / dt_h, 1) if dt_h > 0 else None
    else:
        row['forward_speed_kt'] = None
    row['pipeline_dpi'] = dpi_by_time.get(row['time_utc'], '')


# Compute peak_rain_rate from peak wind (Lonfat climatology from rainfall_warning.py)
peak_wind_kt_lifetime = max(r['max_wind_kt'] for r in hurdat_rows)
if peak_wind_kt_lifetime < 50:
    peak_rain_rate = 5.0 + 0.10 * peak_wind_kt_lifetime
elif peak_wind_kt_lifetime < 100:
    peak_rain_rate = 10.0 + 0.15 * (peak_wind_kt_lifetime - 50)
else:
    peak_rain_rate = 17.5 + 0.10 * (peak_wind_kt_lifetime - 100)


# Second pass: per-snapshot rainfall classification with cumulative tallies
cum_stall_hours = 0.0
cum_slow_hours = 0.0
cum_effective_hours = 0.0
cum_estimated_mm = 0.0

for i, row in enumerate(hurdat_rows):
    fwd = row['forward_speed_kt']
    lat, lon = row['lat'], row['lon']
    near_land = is_near_land(lat, lon)
    terrain_label, _ = check_terrain(lat, lon)
    basin_label, _ = check_basin(lat, lon)

    # Compute Δt for this snapshot (use next-snap delta where possible)
    if i < len(hurdat_rows) - 1:
        try:
            t1 = datetime.fromisoformat(row['time_utc'])
            t2 = datetime.fromisoformat(hurdat_rows[i+1]['time_utc'])
            dt_h = (t2 - t1).total_seconds() / 3600
            if dt_h <= 0 or dt_h > 12:  # clamp implausible gaps
                dt_h = 6.0
        except (ValueError, KeyError):
            dt_h = 6.0
    else:
        dt_h = 6.0

    # Stall / slow classification (rainfall_warning.py logic, near-land gated)
    stall_flag = ''
    slow_flag = ''
    effective_dt = 0.0
    if fwd is not None and fwd > 0 and near_land:
        if fwd <= STALL_SPEED_KT:
            stall_flag = 'STALL'
            cum_stall_hours += dt_h
            effective_dt = dt_h
        elif fwd <= SLOW_SPEED_KT:
            slow_flag = 'SLOW'
            cum_slow_hours += dt_h
            effective_dt = dt_h * 0.6  # rainfall_warning weighting

    cum_effective_hours += effective_dt
    cum_estimated_mm += peak_rain_rate * effective_dt

    row['near_land'] = 'Y' if near_land else ''
    row['stall_flag'] = stall_flag
    row['slow_flag'] = slow_flag
    row['terrain_zone'] = terrain_label or ''
    row['basin_zone'] = basin_label or ''
    row['delta_hours'] = round(dt_h, 1)
    row['cum_stall_h'] = round(cum_stall_hours, 1)
    row['cum_slow_h'] = round(cum_slow_hours, 1)
    row['cum_effective_rain_h'] = round(cum_effective_hours, 1)
    row['cum_estimated_rainfall_mm'] = round(cum_estimated_mm, 0)


# ---------------------------------------------------------------------------
# Write enriched CSV
# ---------------------------------------------------------------------------

cols = ['time_utc', 'lat', 'lon', 'record_id', 'status',
        'max_wind_kt', 'min_pressure_mb', 'forward_speed_kt', 'max_r34_nm',
        'near_land', 'stall_flag', 'slow_flag', 'terrain_zone', 'basin_zone',
        'delta_hours', 'cum_stall_h', 'cum_slow_h',
        'cum_effective_rain_h', 'cum_estimated_rainfall_mm',
        'r34_ne_nm', 'r34_se_nm', 'r34_sw_nm', 'r34_nw_nm',
        'r50_ne_nm', 'r50_se_nm', 'r50_sw_nm', 'r50_nw_nm',
        'r64_ne_nm', 'r64_se_nm', 'r64_sw_nm', 'r64_nw_nm',
        'rmw_nm', 'pipeline_dpi']
with open(OUT_CSV, 'w', encoding='utf-8', newline='') as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    for row in hurdat_rows:
        w.writerow({c: row.get(c, '') for c in cols})

print(f'Wrote {len(hurdat_rows)} rows to {OUT_CSV}')
print(f'  Peak rain rate (from peak wind {peak_wind_kt_lifetime} kt): {peak_rain_rate:.1f} mm/hr')
print(f'  Final cumulative stall hours: {cum_stall_hours:.1f}')
print(f'  Final cumulative slow hours:  {cum_slow_hours:.1f}')
print(f'  Final cumulative effective rain hours: {cum_effective_hours:.1f}')
print(f'  Final cumulative rainfall estimate: {cum_estimated_mm:.0f} mm')
print(f'  Bundle stores: {flo["rainfall_est_mm"]} mm  (delta: {cum_estimated_mm - flo["rainfall_est_mm"]:+.0f})')


# ---------------------------------------------------------------------------
# Stage-by-stage trace (unchanged from v1 except added rainfall internals)
# ---------------------------------------------------------------------------

peak_dpi_raw = flo['peak_dps']
dur = flo['duration_factor']
brd = flo['breadth_factor']
cum_dpi = peak_dpi_raw * (1 + dur + brd)

exp_f = flo['exposure_factor']
perp = flo['perp_factor']
stall_b = flo['stall_bonus']
rain_in = flo['rain_inland_factor']
inland_pen = flo['inland_pen_factor']
combined_boost = exp_f + perp + stall_b + rain_in + inland_pen

if combined_boost > 0:
    boosted = peak_dpi_raw * ((cum_dpi / peak_dpi_raw) + combined_boost)
else:
    boosted = cum_dpi

adjusted = boosted * 1.00

T, S = 60.0, 4.0
if adjusted > T:
    compressed = T + S * math.sqrt(adjusted - T)
else:
    compressed = adjusted
final = min(compressed, 99.0)

RAIN_OBSERVED_IN = 35.93
RAIN_OBSERVED_MM = RAIN_OBSERVED_IN * 25.4
RAIN_OBSERVED_LOC = "Elizabethtown, NC"
RAIN_EST_MM = flo['rainfall_est_mm']
OVER_RATIO = RAIN_EST_MM / RAIN_OBSERVED_MM


# ---------------------------------------------------------------------------
# 5-storm comparison: Ian, Harvey, Michael, Sandy, Ida vs Florence
# ---------------------------------------------------------------------------

comparison_ids = {
    'Ian':      'AL092022',
    'Harvey':   'AL092017',
    'Michael':  'AL142018',
    'Sandy':    'AL182012',
    'Ida':      'AL092021',
    'Florence': 'AL062018',
}
comparison = []
for name, sid in comparison_ids.items():
    s = bundle['storms'].get(sid)
    if not s:
        comparison.append({'name': name, 'sid': sid, 'in_bundle': False})
        continue
    # Re-derive pre-compression value
    pdr = s['peak_dps']
    cdpi = pdr * (1 + s['duration_factor'] + s['breadth_factor'])
    cb = (s['exposure_factor'] + s['perp_factor'] + s['stall_bonus']
          + s['rain_inland_factor'] + s['inland_pen_factor'])
    bsted = pdr * ((cdpi / pdr) + cb) if cb > 0 else cdpi
    comparison.append({
        'name': name,
        'sid': sid,
        'in_bundle': True,
        'peak_dps': s['peak_dps'],
        'cum_dpi': s['dps_original'],
        'pre_compression': round(bsted, 1),
        'displayed_dps': round(s['dps'], 2),
        'rainfall_est_mm': s['rainfall_est_mm'],
        'rainfall_level': s['rainfall_level'],
        'stall_hours': s['stall_hours'],
        'coastal_hours': s['coastal_hours'],
        'peak_wind_kt': s['peak_wind_kt'],
        'peak_ike_tj': s['peak_ike_tj'],
        'duration_factor': s['duration_factor'],
        'breadth_factor': s['breadth_factor'],
        'dps_label': s['dps_label'],
    })

# Rainfall estimator internals — decompose the 2281 mm
rainfall_internals = {
    'description': (
        "core/rainfall_warning.py compute_rainfall_warning produces "
        "rainfall_est_mm via: estimated_total_mm = peak_rain_rate * "
        "effective_rain_hours. The structural property of this "
        "formula is that the PEAK rain rate (intensity-derived "
        "constant from Lonfat climatology) is multiplied by the TOTAL "
        "effective hours — implicitly assuming peak rate is sustained "
        "throughout, which over-estimates for any storm where rain "
        "rate decays from peak."
    ),
    'peak_rain_rate_mm_per_hr': peak_rain_rate,
    'peak_rain_rate_formula': (
        f"vmax_kt = {peak_wind_kt_lifetime} > 100, so rate = 17.5 + 0.1 * "
        f"({peak_wind_kt_lifetime}-100) = {peak_rain_rate:.1f} mm/hr"
    ),
    'effective_rain_hours': {
        'recomputed_from_csv': round(cum_effective_hours, 1),
        'derived_from_bundle': round(RAIN_EST_MM / peak_rain_rate, 1),
        'formula': "effective_rain_hours = stall_hours + slow_hours * 0.6",
        'note': (
            "The bundle's rainfall_est_mm divided by the computed "
            "peak_rain_rate gives the implied effective_hours. The "
            "CSV's per-snapshot accumulation gives a different total "
            "because some slow_hours weren't captured in the bundle's "
            "stall_hours field (bundle stores stall+slow conflated as "
            "stall_hours=42)."
        ),
    },
    'warning_score_factors': {
        'description': (
            "rainfall_warning_score = stall_factor (0-40) + "
            "moisture_factor (0-30) + terrain_factor (0-15) + "
            "basin_factor (0-15). Final score: 87.5 -> 'Historic'."
        ),
        'stall_factor_max': 40.0,
        'moisture_factor_max': 30.0,
        'terrain_factor_max': 15.0,
        'basin_factor_max': 15.0,
        'florence_total_warning_score': flo['rainfall_warning'],
        'florence_alert_level': flo['rainfall_level'],
        'thresholds': {
            'Normal': '0-20',
            'Elevated': '20-40',
            'High': '40-60',
            'Extreme': '60-80',
            'Historic': '80-100',
        },
    },
    'structural_critique': (
        "The estimator is rate-times-duration. The rate is "
        "constant per storm; the duration accumulates over near-"
        "land hours. There is no decay term, no per-snapshot rate "
        "variation, no rainfall-rate ceiling. For a 19-day track "
        "with 42 stall + many slow hours, the multiplicative "
        "structure over-estimates because peak rain rate "
        "(intensity-derived) is not sustained throughout a "
        "weakened post-landfall stall. The audit's headline finding "
        "(2.5x overshoot for Florence, 1.56x for Ian, mean 2.20x "
        "across the bundle) is a property of THIS multiplicative "
        "form, not of any specific calibration constant."
    ),
}


trace = {
    "storm_id": "AL062018",
    "name": "Florence",
    "year": 2018,
    "basin": "ATLANTIC",
    "archetype": "Cat 4 lifetime / Cat 2 landfall / inland rainfall catastrophe",
    "stages": {
        "stage_1_per_snapshot_dpi": {
            "description": (
                "Max single-snapshot DPI along the track. Per-snapshot "
                "DPI computed by core/dpi.py:compute_dpi as a 0.30/0.35/"
                "0.35 weighted blend of IKE / SurgeRain / Economic sub-"
                "scores plus interaction bonuses, dampened by land-"
                "proximity."
            ),
            "max_wind_ms_at_peak": flo['peak_wind_ms'],
            "max_wind_kt_at_peak": flo['peak_wind_kt'],
            "min_pressure_hpa": flo['min_pressure_hpa'],
            "peak_ike_tj": flo['peak_ike_tj'],
            "snapshot_count": flo['snapshot_count'],
            "peak_dpi_raw": peak_dpi_raw,
            "notes": (
                "Peak intensity (130 kt, 937 mb) occurred 2018-09-11 18z "
                "over the open Atlantic at 27N/66W — three days BEFORE "
                "landfall. Florence weakened from Cat 4 to Cat 2 between "
                "the peak and landfall due to wind shear + dry-air "
                "entrainment. peak_dps reflects open-ocean peak, "
                "discounted by land_proximity to ~0.30, so the "
                "structurally large pre-dampening DPI shows up as 58.1."
            ),
            "source": "compute_storm_dps output, field 'peak_dps'",
        },
        "stage_2_cumulative_dpi": {
            "description": (
                "cum_dpi = peak_dpi x (1 + duration_factor + "
                "breadth_factor). Both factors cap at 0.10."
            ),
            "formula": (
                "cum_dpi = peak_dpi * (1 + duration_factor + breadth_factor)"
            ),
            "duration_integral_details": {
                "definition": (
                    "Sum over snapshots near coast with DPI > 25: "
                    "sqrt(DPI/peak) * delta_t * zone_weight. "
                    "Capped via excess/(T_REF*3), T_REF=24h."
                ),
                "snapshots_near_coast": (
                    "75.0 coastal_hours / ~6h cadence ~= 12-13 near-"
                    "coast snapshots eligible for duration credit"
                ),
                "outcome": "duration_factor = 0.10 (CAPPED at the maximum)",
            },
            "inputs": {
                "peak_dpi": peak_dpi_raw,
                "duration_factor": dur,
                "duration_factor_capped": dur >= 0.099,
                "breadth_factor": brd,
                "breadth_factor_capped": brd >= 0.099,
                "coastal_hours": flo['coastal_hours'],
                "track_hours": flo['track_hours'],
            },
            "calculation": (
                f"{peak_dpi_raw} * (1 + {dur} + {brd}) = "
                f"{round(cum_dpi, 2)}"
            ),
            "cum_dpi_computed": round(cum_dpi, 2),
            "cum_dpi_in_bundle": flo['dps_original'],
            "source": (
                "core/cumulative_dpi.py compute_cumulative_dpi"
            ),
        },
        "stage_2_5_rainfall_internals": rainfall_internals,
        "stage_3_combined_boost": {
            "description": (
                "Stage-3 economic boost factors. boosted = peak x "
                "(cum/peak + sum(boosts))."
            ),
            "factors": {
                "exposure_factor": {
                    "value": exp_f,
                    "region": flo.get('exposure_region', '(none)'),
                    "source": (
                        "compile_cache.compute_exposure_factor; "
                        "Carolinas COASTAL_EXPOSURE_WEIGHTS=0.55 x cap "
                        "0.10 = 0.055"
                    ),
                },
                "perp_factor": {
                    "value": perp,
                    "note": (
                        "0.0 from stale-bundle bug (us_landfall_count "
                        "issue fixed in commit 659342b). Florence had "
                        "1 US landfall so the corrected code would "
                        "yield ~0.03."
                    ),
                    "source": (
                        "compile_cache.compute_perpendicular_factor (stale)"
                    ),
                },
                "stall_bonus": {
                    "value": stall_b,
                    "stall_hours": flo['stall_hours'],
                    "note": (
                        "0.0275 — the largest stall_bonus among storms "
                        "audited so far. Driven by 42 stall_hours x "
                        "0.01 + slow_hours x 0.005, weighted by "
                        "Carolinas econ 0.55. Cap is 0.05."
                    ),
                    "source": "core/dps_engine.py Stage 3 stall_bonus",
                },
                "rain_inland_factor": {
                    "value": rain_in,
                    "rainfall_warning_score": flo['rainfall_warning'],
                    "rainfall_level": flo['rainfall_level'],
                    "estimated_rainfall_mm": RAIN_EST_MM,
                    "observed_rainfall_mm": round(RAIN_OBSERVED_MM, 1),
                    "observed_rainfall_in": RAIN_OBSERVED_IN,
                    "observed_location": RAIN_OBSERVED_LOC,
                    "estimator_over_ratio": round(OVER_RATIO, 2),
                    "note": (
                        f"Engine estimate {RAIN_EST_MM:.0f} mm vs "
                        f"observed {RAIN_OBSERVED_MM:.0f} mm — "
                        f"{OVER_RATIO:.2f}x overshoot. "
                        "rain_inland_factor saturates the 0.04 cap at "
                        "warning_score >= ~80 regardless of estimate "
                        "magnitude, so the score is unchanged. But the "
                        "user-facing rainfall_text and rainfall_level "
                        "are wrong. See stage_2_5_rainfall_internals "
                        "above for the structural critique."
                    ),
                    "source": (
                        "core/rainfall_warning.py compute_rainfall_warning"
                    ),
                },
                "inland_pen_factor": {
                    "value": inland_pen,
                    "note": (
                        "0.0 — Florence weakened to TS within hours of "
                        "landfall, never sustaining 18+ m/s for 2+ "
                        "inland snapshots."
                    ),
                    "source": "core/dps_engine.py Stage 3 inland_pen_factor",
                },
            },
            "combined_boost": round(combined_boost, 4),
            "boosted_formula": (
                "boosted = peak_dpi x ((cum_dpi / peak_dpi) + "
                "combined_boost)"
            ),
            "boosted_calculation": (
                f"{peak_dpi_raw} x ({round(cum_dpi/peak_dpi_raw, 4)} + "
                f"{combined_boost}) = {round(boosted, 2)}"
            ),
            "boosted_value": round(boosted, 2),
        },
        "stage_4_basin_adjustment": {
            "description": (
                "Atlantic basin: dps_multiplier=1.00, ri_bonus=0, no "
                "WP/EP enhancements. No-op."
            ),
            "basin": "ATLANTIC",
            "dps_multiplier": 1.00,
            "ri_bonus": 0,
            "adjustment_notes": flo.get('adjustment_notes', ''),
            "adjusted": round(adjusted, 2),
            "source": (
                "compile_cache.apply_basin_dps_adjustment "
                "(BASIN_COEFFICIENTS ATLANTIC)"
            ),
        },
        "stage_5_compression": {
            "description": (
                "Per-basin sqrt compression. Atlantic uses (T=60, S=4). "
                "See methodology.html: 'A note on Stage 5 — the "
                "presentation layer'."
            ),
            "params": {"T": T, "S": S, "basin": "ATLANTIC"},
            "formula": (
                "if adjusted > T: adjusted = T + S * sqrt(adjusted - "
                "T); final = min(adjusted, 99)"
            ),
            "calculation": (
                f"60 + 4 * sqrt({round(adjusted, 2)} - 60) = "
                f"60 + 4 * {round(math.sqrt(adjusted - T), 3)} = "
                f"{round(compressed, 2)}"
            ),
            "compressed_value": round(compressed, 2),
            "hard_cap_99": compressed >= 99.0,
            "pre_to_post_delta": round(adjusted - compressed, 2),
            "pivot_note": (
                "Atlantic (T=60, S=4) breakeven: x = 60 + 4*sqrt(x-60) "
                "solves to x = 76. So compression is identity at pre-"
                "comp 76, slightly amplifies below (75.79 -> 75.9), "
                "and dampens above (Ian's 108.78 -> 87.94). Florence "
                "sits exactly at the pivot point — Stage 5 carries no "
                "editorial weight for Florence's score."
            ),
            "source": (
                "compile_cache.apply_basin_dps_adjustment Stage-5 sqrt "
                "compression"
            ),
        },
    },
    "final": {
        "displayed_dps": round(final, 2),
        "displayed_dps_in_bundle": flo['dps'],
        "dps_label": flo['dps_label'],
        "category_lifetime": flo['category_lifetime'],
        "category_landfall": flo['category'],
        "match_check": abs(final - flo['dps']) < 0.5,
    },
    "ground_truth_reference": {
        "damage_usd_2024": 24_230_000_000,
        "deaths_total": 52,
        "peak_surge_ft": 10.1,
        "peak_surge_location": "Emerald Isle, NC",
        "peak_rainfall_in": 35.93,
        "peak_rainfall_location": "Elizabethtown, NC",
        "peak_wind_landfall_mph": 90,
        "landfall_pressure_mb": 956,
        "landfall_saffir": 1,
        "fema_states": ["NC", "SC"],
        "source": (
            "core/ground_truth.py:AL062018 (NHC TCR Stewart & Berg 2019)"
        ),
    },
    "five_storm_comparison": {
        "description": (
            "Florence vs Ian (peak-major Cat 4 landfall), Harvey (5-"
            "day TX stall), Michael (compact Cat 5 panhandle), Sandy "
            "(huge wind field NE corridor), Ida (compact Cat 4 LA + "
            "NE remnant flooding). Same Atlantic basin formula path. "
            "Ordered by displayed DPS."
        ),
        "storms": sorted(
            [s for s in comparison if s.get('in_bundle')],
            key=lambda x: -x['displayed_dps'],
        ),
    },
    "persistence_pathway_finding": {
        "summary": (
            "Florence is anchored by Stage 1 peak_dps = 58.1, which is "
            "24 points BELOW Ian's 82.1 despite similar peak winds. "
            "The gap is land-proximity dampening on Florence's open-"
            "ocean peak. The cumulative bonuses (0.10 + 0.10 = 0.20) "
            "and Stage 3 boost (0.1045) lift Florence by ~30% — "
            "exactly the same percentage as Ian — but the absolute "
            "lift is smaller because of the lower anchor."
        ),
        "structural_observation": (
            "The peak-anchored architecture (cum_DPI = peak x (1 + "
            "dur + brd)) is multiplicative, not additive. Rainfall-"
            "catastrophe behavior is fundamentally additive in nature "
            "(more hours of moderate rain = more cumulative damage), "
            "but the model represents it as a percentage modifier on "
            "an intensity-based anchor. This is why Florence's $24B "
            "of inland-flooding damage cannot push her score above "
            "the Cat 1 landfall's intensity-derived ceiling."
        ),
        "framing_per_review": (
            "The fix is NOT 'increase the rainfall coefficient' — "
            "that would destabilize calibration across the bundle. "
            "The fix may be structural: rainfall catastrophe is a "
            "distinct pathway with its own activation logic "
            "(persistence + terrain + basin), and could feed into "
            "the score as a parallel additive term rather than a "
            "multiplicative modifier on peak. The current "
            "rainfall_warning_score is already a standalone 0-100 "
            "scale; it just doesn't feed back into the DPS except "
            "via the capped rain_inland_factor."
        ),
        "candidate_design": (
            "A 'persistence-pathway DPS' that takes max(intensity_"
            "anchored_DPS, persistence_anchored_DPS) would let "
            "Florence be scored from EITHER her wind-anchored "
            "intensity OR her rainfall-warning-anchored persistence "
            "— whichever produces the higher score. That preserves "
            "the existing calibration for wind-dominant storms while "
            "giving rainfall-dominant catastrophes a separate ranking "
            "pathway. Not implemented; surfaced here as a design "
            "candidate for future audit work."
        ),
    },
    "known_caveats": [
        (
            "perp_factor = 0.0 is the stale-bundle us_landfall_count "
            "artifact (commit 659342b). Florence's 1 US landfall "
            "would yield ~0.03 under fixed code; displayed DPS would "
            "rise from 75.89 to ~76.7 after recompile."
        ),
        (
            "Bundle compiled before observed-rainfall override hook "
            "activated for Florence. Next recompile activates "
            "core/dps_engine.py:L112-129 and rain_result.estimated_"
            "total_mm switches from 2281 mm to 913 mm. Score impact "
            "small (cap binding); rainfall_text + rainfall_level "
            "would correct."
        ),
        (
            "Category landfall = 2 in bundle vs Cat 1 in NHC TCR "
            "(80 kt L-record). Standard 6-hourly-grid artifact."
        ),
        (
            "inland_pen_factor = 0 is correct: Florence dropped below "
            "18 m/s before clearing the coastal box."
        ),
        (
            "CSV's per-snapshot cum_estimated_rainfall_mm uses peak "
            "rain rate sustained throughout — same structural "
            "assumption as the live estimator. The total at end-of-"
            "track may differ from the bundle's stored value by ~10% "
            "due to differences in delta-hours assignment between "
            "this script and the live engine's snapshot iteration "
            "logic. Comparative behavior across the track is what "
            "matters here, not absolute end-state agreement."
        ),
    ],
    "audit_method": (
        "Re-derived from compile_cache.py + core/dps_engine.py + "
        "core/rainfall_warning.py + bundle field values. Cross-"
        "checked against HURDAT2 best-track AL062018 (79 records) + "
        "NHC TCR (Stewart & Berg 2019). One landfall (Wrightsville "
        "Beach NC at 34.2N/77.8W, 80 kt / 956 mb on 2018-09-14 11:15z) "
        "matches the bundle's single-entry landfalls array. v2 of "
        "this audit script (post-ChatGPT review) adds per-snapshot "
        "rainfall classification and 5-storm comparison."
    ),
}

with open(OUT_JSON, 'w', encoding='utf-8') as f:
    json.dump(trace, f, indent=2)

print(f'Wrote {OUT_JSON}')
print()
print('=== Stage-by-stage trace ===')
print(f"Stage 1 peak DPI         = {peak_dpi_raw}")
print(f"Stage 2 cum_dpi          = {round(cum_dpi, 2)} (bundle: {flo['dps_original']})")
print(f"Stage 3 combined_boost   = {round(combined_boost, 4)}")
print(f"Stage 3 boosted          = {round(boosted, 2)}")
print(f"Stage 4 adjusted         = {round(adjusted, 2)}")
print(f"Stage 5 compressed       = {round(compressed, 2)}")
print(f"Final displayed DPS      = {round(final, 2)} (bundle {flo['dps']:.2f}, match {abs(final - flo['dps']) < 0.5})")
print()
print('=== 5-storm comparison ===')
print(f"{'Storm':<10} {'Peak':>5} {'Cum':>5} {'PreComp':>7} {'Display':>7} {'Label':<14}  Stall_h Rain_mm")
for s in sorted([s for s in comparison if s.get('in_bundle')], key=lambda x: -x['displayed_dps']):
    print(f"{s['name']:<10} {s['peak_dps']:>5.1f} {s['cum_dpi']:>5.1f} "
          f"{s['pre_compression']:>7.1f} {s['displayed_dps']:>7.2f} {s['dps_label']:<14}  "
          f"{s['stall_hours']:>5.0f}h  {s['rainfall_est_mm']:>5.0f}mm")
