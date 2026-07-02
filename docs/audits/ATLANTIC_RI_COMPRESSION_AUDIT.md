# Atlantic RI bonus + compression-curve audit (v12)

**Date:** 2026-07-01
**Scope:** (1) Proposal to enable the rapid-intensification bonus for the Atlantic basin with a timestamp-window RI detector; (2) replacement of the per-basin `T + S*sqrt(x-T)` + `min(99)` compression with a C1 saturating exponential.
**Companion script:** `scratch/ri_compression_audit.py`
**Related prior work:** `WP_DPS_AUDIT.md` (v7/v8), `EP_DPS_AUDIT.md` (v11), duration/stall audit.

## Motivation

A TD→hurricane-in-24h storm approaching a major metro (the "Houston scenario") exposed three formula weaknesses: RI contributes nothing for Atlantic storms (basin `ri_bonus=0` AND the per-snapshot RI bonus never fires — `compute_dpi_simple` doesn't thread `previous_snapshot`); the RI estimator elsewhere uses `Δwind_per_step × 4`, which assumes 6-hourly cadence; and the sqrt compression curve *expands* scores in `T < x < T+S²` (Atlantic raw 61 → displayed 64) with a near-vertical slope at T, then hard-ties extreme storms at the 99 clamp.

## Methodology

Full-pipeline rerun (`compute_storm_dps`) over all 244 preload storms with `apply_basin_dps_adjustment` replaced by an instrumented replica. Guards: replica reproduces the live path bit-for-bit (0 mismatches > 0.05) and the baseline reproduces `compiled_bundle.json` (0 mismatches > 1.5). Ground truth: `core/ground_truth.py` damage figures (n=10 Atlantic; the 5 EP truth storms are not in the bundle). Variants: RI_WINDOW (24.5h-window detector, ATL ri_bonus=15), RI_NEARLF (same, gated to RI ending ≤72h before first land contact), COMP_RAT (rational compressor, k fitted per basin), COMP_EXP (parameter-free exponential), BOTH.

## Results

| Storm | Base | RI_WINDOW | RI_NEARLF | COMP_EXP | Damage $B |
|---|---|---|---|---|---|
| Katrina 2005 | 95.9 | 98.0 | 95.9 | **94.0** | 125.0 |
| Harvey 2017 | 84.3 | 86.6 | 84.3 | **83.8** | 125.0 |
| Irma 2017 | 93.4 | 95.5 | 93.4 | **92.5** | 77.2 |
| Ian 2022 | 91.8 | 93.7 | 91.8 | **91.3** | 112.9 |
| Sandy 2012 | 87.0 | 89.2 | 87.0 | **86.9** | 70.2 |
| Michael 2018 | 82.9 | 85.4 | 85.4 | **82.2** | 25.5 |
| Milton 2024 | 82.2 | 88.0 | 88.0 | **81.3** | 34.0 |
| Idalia 2023 | 73.4 | 77.4 | 77.4 | **69.8** | — |

Spearman vs damage (ATL, n=10): BASELINE **+0.426** · RI_WINDOW +0.371 · RI_NEARLF +0.249 · COMP_EXP **+0.426** (monotone — ranks preserved exactly).

## Findings

1. **RI bonus REJECTED for compiled lifetime scores.** Both RI variants *worsen* damage correlation. Ungated RI fires on essentially every major Atlantic storm (uniform inflation, no discrimination); landfall-gated RI boosts Michael ($25B) and Milton ($34B) above their damage rank relative to Irma ($77B)/Ian ($113B). Root cause: for a *completed* storm, RI is already priced in — the peak wind/IKE the storm reached because of RI is what the formula scores. An RI bonus double-counts intensity. RI's real value is **anticipatory** (the peak hasn't materialized yet), so it was implemented as a live-storm forecast signal instead: timestamp-window RI detector (≥30 kt/24h, NHC definition, cadence-proof) in `computeDpsTrend`, feeding a public RI warning on the forecast-DPS band. Compiled scoring untouched.

2. **Exponential compressor ADOPTED** (`f(x) = T + (99−T)(1 − e^−(x−T)/(99−T))` for x > T). C1 at T (slope exactly 1 — kills the expansion zone and threshold instability), strictly monotone (all ranks preserved; Spearman unchanged by construction), asymptotic to 99 (no clamp ties), zero fitted constants. Anchor storms move ≤2.1; expansion-zone storms deflate as intended (Idalia 73.4→69.8, Eta 68.0→65.x). The fitted-k rational alternative moved Katrina −4.6 and was rejected. `compression_S` retired. WP max shift +1.9 (Ragasa 79.5→81.4); no WP storm near ceiling before or after.

3. **Legacy `Δstep×4` RI estimator in WP/EP left in place, flagged.** Fixing it moves WP storms −13..−19 (Gaemi, Yagi, Kong-Rey) because v7/v8 WP coefficients were calibrated against the inflated estimator, and there is **no WP/EP ground truth in the registry** to validate a recalibration (the 5 EP truth storms aren't in the bundle). Recommendation: add WP/EP truth storms (incl. Otis EP182023) to the bundle + registry, then re-run this audit's RI_WINDOW variant per basin before touching the estimator.

## Changes shipped

- `compile_cache.py`: exponential compression (all basins), sqrt + clamp removed.
- `frontend/index.html`: `computeDpsTrend` RI detection now timestamp-window ≥30 kt/24h; forecast-DPS band promoted out of Analyst mode (public for live storms); RI-in-progress warning appended to the band (labeled warning — band numbers NOT inflated).
- Rebake required (`scripts/rebake.py`) — compiled scores shift ≤2.1 for anchors, more for expansion-zone storms.

## Follow-ups

- Forecast-cumulative DPS: accrue duration/stall/exposure along the OFCL forecast track server-side (the band currently projects peak-intensity DPS with size/motion held current; forecast stall risk is surfaced separately via `fc.stall_risk`).
- WP/EP ground-truth expansion, then per-basin RI estimator fix (finding 3).
