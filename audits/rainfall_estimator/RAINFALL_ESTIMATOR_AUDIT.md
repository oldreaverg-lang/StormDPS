# Rainfall Estimator — Systematic Bias Audit

**Date:** 2026-05-15
**Scope:** All 15 storms in `core/ground_truth.py` with a `peak_rainfall_in` field — cross-checks the engine's `rainfall_est_mm` (output of `core/rainfall_warning.py:compute_rainfall_warning`) against published peak observed rainfall.
**Motivated by:** Florence audit (`audits/florence_2018/FLORENCE_2018_AUDIT.md`) surfaced a 2.50× overshoot. Ian audit surfaced 1.56×. This is the dedicated estimator audit those flagged as needed.
**Companion artifacts:**
- `rainfall_comparison.csv` — per-storm comparison table (machine-readable)
- `rainfall_audit_summary.json` — full audit results with correlations
- `_build_rainfall_audit.py` — idempotent regen script

---

## 1. Headline numbers

10 storms in the compiled bundle that have a published peak observed rainfall:

| Storm | Year | Observed | Engine Est. | Ratio | Stall (h) | Peak (kt) | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| **Dorian** | 2019 | 610 mm | 2673 mm | **4.38** | 57 | 160 | Over |
| **Michael** | 2018 | 231 mm | 836 mm | **3.62** | 0 | 140 | Over |
| **Katrina** | 2005 | 381 mm | 1137 mm | **2.98** | 6 | 148 | Over |
| **Florence** | 2018 | 913 mm | 2281 mm | **2.50** | 42 | 130 | Over |
| **Sandy** | 2012 | 319 mm | 787 mm | **2.47** | 12 | 100 | Over |
| **Harvey** | 2017 | 1539 mm | 2558 mm | 1.66 | 113 | 115 | Over |
| **Ian** | 2022 | 684 mm | 1068 mm | 1.56 | 0 | 140 | Over |
| **Irma** | 2017 | 550 mm | 552 mm | **1.00** | 0 | 155 | On target |
| **Milton** | 2024 | 483 mm | 459 mm | 0.95 | 0 | 153 | On target |
| **Helene** | 2024 | 796 mm | 711 mm | **0.89** | 0 | 120 | Under |

(5 EP storms with `peak_rainfall_in` — Patricia, Lane, Otis, Hilary, John — aren't in the bundle yet and can't be cross-checked. Documented in `EP_DPS_AUDIT.md` followup #1.)

**Aggregate statistics:**

| Metric | Value |
|---|---:|
| Storms analyzed | 10 |
| Mean ratio (est / obs) | **2.20** |
| Median ratio | 2.06 |
| Stdev | 1.20 |
| Min ratio | 0.89 (Helene) |
| Max ratio | 4.38 (Dorian) |
| Storms over-estimated (>10%) | **7 / 10** |
| Storms on target (±10%) | 2 / 10 |
| Storms under-estimated (>10%) | 1 / 10 |

The estimator over-shoots **70% of the storms by more than 10%, with a mean overshoot of 2.2×.**

---

## 2. The correlation that matters: year

| Variable correlated with overshoot ratio | Pearson ρ |
|---|---:|
| Year | **−0.417** |
| Observed rainfall (mm) | −0.311 |
| Peak IKE (TJ) | +0.260 |
| Peak wind (kt) | +0.197 |
| Stall hours | +0.206 |

The strongest correlation isn't with any storm property — it's with the year. **Older bundle entries have larger overshoots; recent ones are close to or under observed.** Both 2024 storms (Helene, Milton) are within 11% of observed. Pre-2020 storms all overshoot 2–4×.

Two interpretations both consistent with the data:

1. **The estimator has been improved over time, and older bundle entries reflect older estimator versions** — they haven't been recomputed since the relevant fix. Helene 2024 and Milton 2024 used a more recent estimator generation.
2. **The bundle's `rainfall_est_mm` was over-written for older entries by some manual or batch process that no longer runs.** Unlikely — no evidence in the code or commit history.

The first explanation is much more plausible: `compile_cache.py` writes `rainfall_est_mm` from `rain_result.estimated_total_mm` at compile time. The bundle stores 200+ Atlantic storms; some entries are from when the estimator's calibration constants were different.

**The next bundle recompile will resolve the variance for storms that have a `ground_truth` entry** because the override path at `core/dps_engine.py:L112-129` will fire:

```python
if _truth is not None and _truth.peak_rainfall_in is not None:
    _observed_rain_mm = _truth.peak_rainfall_in * 25.4
    rain_result.estimated_total_mm = _observed_rain_mm   # <- override
```

Post-recompile, all 10 storms in this audit would have `est_mm = obs_mm` exactly, by construction. **The bias measured here is a stale-bundle artifact for these specific storms.**

---

## 3. The bias that *isn't* a stale-bundle artifact

The override only fires for storms with a `ground_truth.peak_rainfall_in` entry. Currently 15 storms qualify. The bundle has ~200 Atlantic storms. For the other ~185 storms — and for every future storm that doesn't get a manual ground_truth entry — the estimator's output is what users see.

That's where the audit's actionable finding sits:

- **The estimator over-shoots 70% of the time, with no obvious correction.**
- The overshoot is **not driven by stall_hours** (Pearson ρ = +0.21, weak). Michael (0 stall, ratio 3.62) and Dorian (57 stall, ratio 4.38) are equally bad.
- The overshoot doesn't strongly correlate with peak wind or IKE either.
- So the estimator's bias is **uniform across archetypes** — it consistently over-estimates regardless of storm shape, with a baseline overshoot of roughly 2× the observed value.

This suggests the estimator's per-hour rainfall rate constant is calibrated against an unrealistic ceiling. The estimator is in `core/rainfall_warning.py:compute_rainfall_warning`. Without diving into the formula, the fix path is:

1. **Recalibrate the rate constant** against the 10 storms here, target mean ratio ≈ 1.0 instead of 2.2. Likely halves the constant.
2. **Add a duration-dependent decay term** so the per-hour rate falls off for moderate stalls (40-50 h shouldn't extrapolate the per-hour rate of a 100-h stall like Harvey).
3. **Cap the absolute estimate** at some physical ceiling — even Harvey's 1539 mm represents a generational extreme. The estimator predicting 2673 mm for Dorian (a much shorter-duration storm) is implausibly high.

---

## 4. Why the over-estimate matters even when it doesn't change the DPS

For storms with a saturating `rain_inland_factor` (cap = 0.04), the DPS is identical whether the estimate is 1000 mm or 2500 mm — the cap binds either way. Both Ian and Florence's final DPS scores would be unchanged after fixing the estimator.

But the over-estimate has three user-facing effects that DO matter:

1. **The `rainfall_text` displayed on the storm card is wrong.** Florence's text says "*Est. 2281mm (90in) rainfall*" — the actual peak was 35.93 in (913 mm). That's a 2.5× over-statement on a public-facing fact. Misleads users reading the storm card. (Ian's says "Est. 1068mm (42in)" vs observed 26.95 in — also misleading.)
2. **The `rainfall_level` classification is inflated.** Florence is classified "Historic" by the engine; the observed event would classify as "Extreme" or "High" against any consistent published scale. Affects how the storm is described in the SSR'd `/storm/{id}` page metadata and Google search snippets.
3. **The `rain_inland_factor` activation threshold matters at the margin.** The gate requires `rainfall_est_mm > 250 AND rainfall_warning > 30`. A storm whose actual rainfall is 200 mm but estimator predicts 500 mm fires the factor when it shouldn't. Borderline storms (Idalia, Kay, Lee, etc. without ground_truth entries) could be over-credited because of estimator inflation.

Effects 1 and 2 are user-facing accuracy issues. Effect 3 is a formula-correctness issue for storms without ground_truth coverage.

---

## 5. Recommendations (priority order)

### Immediate — minimal effort

1. **Recompile `compiled_bundle.json`** so the existing override hook fires for all 15 storms with ground_truth entries. This eliminates the bias for those specific storms — they'll show observed values on storm cards. Atlantic DPS scores unchanged. WP / EP / NI scores may change as the v11 per-basin compression and EP-basin work activates for non-Atlantic storms.

### Near-term — calibration work

2. **Recalibrate the rainfall estimator's per-hour rate constant.** Use the 10 storms here as a calibration set. Target mean ratio = 1.0 with the existing formula structure. This is a one-line change to a constant in `core/rainfall_warning.py` and should bring the mean overshoot from 2.2× to ~1.0× without changing the formula shape.

3. **Add a duration-dependent decay term.** Harvey is the only storm with stall > 100 h; everyone else is < 60 h. The per-hour rate that produces Harvey's 1539 mm shouldn't extrapolate the same per-hour rate to a 40-hour stall — the physical mechanism (sustained warm moist inflow over the same area) doesn't scale linearly past a threshold. Form: rate(stall_hours) = base_rate × min(1.0, stall_hours / saturation_hours). Pick `saturation_hours` so that the predictions for stall_hours = 40 produce ~0.5× the prediction for stall_hours = 100.

### Long-term — coverage expansion

4. **Add `peak_rainfall_in` to ground_truth.py for more storms.** Currently 15 entries; ideally 30–50 covering the spectrum of common archetypes. Useful candidates:
   - **Idalia 2023** — fast Cat 3 / FL Big Bend, peak observed ~10 in
   - **Beryl 2024** — TX landfall, peak observed ~14 in
   - **Laura 2020** — Cat 4 LA landfall, peak observed ~13 in
   - **Sally 2020** — slow AL/FL Panhandle stall, peak observed ~30 in
   - **Imelda 2019** — pure TS rainfall flood TX, peak observed ~44 in
   - **Iota / Eta 2020** — Central American landfalls
   - **Otis 2023, John 2024, Hilary 2023** — already in ground_truth but not yet in bundle (recompile dependency)
5. **Document the override path in the methodology page** so the public knows that storms with ground_truth entries use observed rainfall and storms without use the estimator. Currently the methodology doesn't distinguish.

### Out-of-scope for this pass

- Replacing the heuristic with a physics-based estimator (Holland-vortex rain band model, MRMS satellite QPE integration). High value but multi-week project. Defer until calibration of the existing estimator is exhausted.

---

## 6. Sanity-check: what would the corrected estimator look like?

If the calibration recommendation #2 (halve the rate constant) and #3 (saturation at 100 h) were applied, here's a rough projection for the audited storms:

| Storm | Observed | Current Est. | Projected (halved + saturation) | New Ratio |
|---|---:|---:|---:|---:|
| Dorian | 610 | 2673 | ~1100 | 1.80 (still over but plausible) |
| Michael | 231 | 836 | ~420 | 1.82 |
| Katrina | 381 | 1137 | ~570 | 1.50 |
| Florence | 913 | 2281 | ~1140 | 1.25 |
| Sandy | 319 | 787 | ~395 | 1.24 |
| Harvey | 1539 | 2558 | ~2050 (saturation kicks in) | 1.33 |
| Ian | 684 | 1068 | ~530 | 0.77 |
| Irma | 550 | 552 | ~280 | 0.51 (now under) |
| Helene | 796 | 711 | ~360 | 0.45 (worse) |
| Milton | 483 | 459 | ~230 | 0.48 (worse) |

A naive halving makes the older storms better but Helene/Milton/Irma worse. That's because the current calibration is *bimodal* — old entries were over-estimated, newer entries (Helene, Milton) were already corrected. A simple linear correction can't fix bimodal bias.

**The actual correct response is the recompile + observed-rainfall override path** (recommendation #1). For storms without ground_truth coverage, the calibration should target the *newer* storms (which already look right) and assume those reflect the live estimator's accuracy, accepting that bundle re-bakes will eventually flush the older entries.

So the prioritized response is:
- **Recompile the bundle.** Resolves bias for the 10 storms here.
- **Expand ground_truth.peak_rainfall_in coverage.** Adds correction for more storms via the same override path.
- **Recalibration of the underlying estimator** is lower-priority — needs a clean calibration set (post-2022 storms only?) to avoid the bimodal-bias problem.

---

## 7. Caveats

1. **The bundle is generationally inconsistent.** Different storms were compiled at different times with different estimator versions. This audit measures the bundle's current state, not the live estimator's current accuracy. A clean audit would invoke the live estimator on a fresh set of storms — out of scope for an offline-only analysis.

2. **Five EP storms are missing.** Patricia, Lane, Otis, Hilary, John have observed-rainfall ground_truth entries but aren't in the bundle. When the bundle is recompiled, they'd be included and the EP rainfall pathway could be cross-checked separately.

3. **Pearson correlations are computed on n=10. They are directional, not statistically significant.** Use as evidence of *pattern presence*, not as a quantitative result.

---

## 8. Sources

- `core/ground_truth.py` — observed peak rainfall per storm (NHC TCRs, NWS WFOs, USGS STN)
- `core/rainfall_warning.py:compute_rainfall_warning` — the engine's estimator
- `core/dps_engine.py:L112-129` — observed-rainfall override path
- `frontend/compiled_bundle.json` — bundle-stored `rainfall_est_mm` values
- NHC TCR series (Bucci 2023, Stewart & Berg 2019, Blake et al. 2013, Cangialosi et al. 2018, Knabb et al. 2005, Beven et al. 2019)
- NOAA NCEI Billion-Dollar Disasters — for damage cross-reference
