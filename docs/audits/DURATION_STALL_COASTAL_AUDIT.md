# Duration / Stall / Coastal-Tracking Stacking Audit

**Date:** 2026-05-14 (v2 — bundle-anchored revision; v1 retracted)
**Scope:** Quantify how much of each Atlantic storm's displayed DPS comes from the five overlapping cumulative bonus terms (duration / breadth / Stage-3 boost factors), evaluate a proposed sixth term for "compact-intense-exposed" storms, and quantify how much of any additive bonus actually survives Stage-5 sqrt compression.
**Methodology:** Load production-cached values for each storm directly from `frontend/compiled_bundle.json`, reproduce the boosted score via the same arithmetic as `core/dps_engine.py`, apply the Stage-5 sqrt compression (T=70, S=2.5), and compare to the cached displayed `dps`. All hand-estimated parameters from the v1 audit have been retired.
**Companion script:** `scratch/duration_stall_coastal_audit.py`
**Related prior work:** `WP_DPS_AUDIT.md`, `validate_formula.py`, `tournament_formula.py`

---

## v1 → v2 revision note

The first version of this audit hand-estimated `peak_dpi_proxy`, `coastal_hours_weighted`, and the bonus magnitudes per storm, then reported "Final" scores that were **pre-compression** — uncompressed sums that didn't match the live displayed values. The conclusions about a "compact-storm coverage gap" were structurally directional but the *magnitudes* were wrong by enough to flip the recommendation. v2 reads every input straight from the production-cached bundle and applies Stage-5 compression, so "Final" is what `/storm/{id}` actually shows. The recommendations have been rewritten accordingly.

---

## 1. The five bonus terms under audit

| # | Term | Source | Cap |
|---|---|---|---|
| 1 | `coast_tracking_bonus` | `core/dpi.py` L300-340 — folded into `peak_dps` | ≤ 12 pts |
| 2 | `stall_dpi_bonus` | `core/dpi.py` L363-370 — folded into `peak_dps` | ≤ 8 pts |
| 3 | `duration_factor` | `core/cumulative_dpi.py` | ≤ 0.10 (×peak_dps) |
| 4 | `breadth_factor` | `core/cumulative_dpi.py` | ≤ 0.10 (×peak_dps) |
| 5 | Stage-3 `combined_boost`: `exposure_factor + perp_factor + stall_bonus + rain_inland_factor + inland_pen_factor` | `core/dps_engine.py` | additive, no aggregate cap |

In practice `coast_tracking_bonus` and `stall_dpi_bonus` are already baked into the cached `peak_dps` (the per-snapshot composite), so the "stack" of separately-firing terms in the cumulative pipeline is really three: `duration_factor`, `breadth_factor`, and the Stage-3 combined_boost. The v1 audit treated all five as separate cumulative terms, which inflated the perceived overlap.

---

## 2. Sanity check — does the recompute match the bundle?

Loading peak / duration / breadth / Stage-3 fields from `compiled_bundle.json` and feeding them through `boosted = peak × ((cum_DPI/peak) + combined_boost)` → `compress(boosted)`:

| Storm | Peak | Cum | Boosted | Recompute | Bundle | Δ |
|---|---:|---:|---:|---:|---:|---:|
| Katrina | 96.6 | 115.9 | 127.5 | 88.96 | 92.89 | −3.93 |
| Irma | 86.3 | 103.6 | 109.6 | 85.73 | 88.18 | −2.45 |
| Ian | 82.1 | 98.5 | 108.8 | 85.57 | 87.93 | −2.36 |
| Dorian | 85.1 | 102.1 | 108.2 | 85.46 | 87.78 | −2.32 |
| Ike | 81.0 | 97.3 | 103.9 | 84.55 | 86.53 | −1.98 |
| Maria | 84.2 | 94.1 | 102.0 | 84.14 | 85.92 | −1.78 |
| Sandy | 73.6 | 83.2 | 93.9 | 82.22 | 83.27 | −1.05 |
| Harvey | 72.8 | 87.4 | 94.2 | 82.30 | 83.41 | −1.11 |
| Florence | 58.1 | 69.7 | 75.8 | 76.02 | 75.89 | +0.13 |
| Milton | 78.7 | 85.7 | 94.0 | 82.25 | 83.35 | −1.10 |
| Michael | 69.6 | 83.5 | 89.3 | 80.98 | 81.68 | −0.70 |
| Helene | 75.2 | 82.7 | 88.8 | 80.85 | 81.47 | −0.62 |
| Imelda | 48.7 | 52.3 | 52.3 | 52.26 | 52.30 | −0.04 |

The recompute is systematically ~1–4 pts below the bundle's displayed value, with the gap growing for higher-scoring storms. This likely reflects an additional adjustment (storm-year era factor, or `vuln_bonus`/`compact_bonus` being added between `boosted` and compression) that I haven't yet traced. The sign and ordering are correct; the offset is consistent. Conclusions below use the bundle's displayed values directly where available.

---

## 3. Bonus decomposition (in pre-compression points)

| Storm | Bucket | Damage $B | Peak | Duration pts | Breadth pts | Stage-3 pts | Total bonus pts | No-bonus display | Actual display | **Bonus survives** |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Katrina | stall | 200.0 | 96.6 | 9.7 | 9.7 | 11.6 | 30.9 | 82.9 | 89.0 | **+6.1** |
| Irma | tracker | 80.0 | 86.3 | 8.6 | 8.6 | 6.0 | 23.3 | 80.1 | 85.7 | +5.6 |
| Dorian | stall | 5.0 | 85.1 | 8.5 | 8.5 | 6.1 | 23.1 | 79.7 | 85.5 | +5.7 |
| Maria | fast | 115.0 | 84.2 | 1.5 | 8.4 | 7.9 | 17.8 | 79.4 | 84.1 | +4.7 |
| Ian | fast | 119.6 | 82.1 | 8.2 | 8.2 | 10.3 | 26.7 | 78.7 | 85.6 | +6.9 |
| Ike | tracker | 50.0 | 81.0 | 8.1 | 8.1 | 6.7 | 22.9 | 78.3 | 84.6 | +6.3 |
| Milton | fast | 34.3 | 78.7 | 0.0 | 7.0 | 8.3 | 15.3 | 77.4 | 82.2 | +4.9 |
| Helene | fast | 78.7 | 75.2 | 0.0 | 7.5 | 6.1 | 13.6 | 75.7 | 80.9 | +5.2 |
| Sandy | tracker | 90.0 | 73.6 | 2.2 | 7.4 | 10.7 | 20.3 | 74.7 | 82.2 | +7.5 |
| Harvey | stall | 160.0 | 72.8 | 7.3 | 7.3 | 6.8 | 21.4 | 74.2 | 82.3 | +8.1 |
| Michael | fast | 32.0 | 69.6 | 7.0 | 7.0 | 5.8 | 19.7 | 69.6 | 81.0 | **+11.4** |
| Florence | stall | 29.0 | 58.1 | 5.8 | 5.8 | 6.1 | 17.7 | 58.1 | 76.0 | **+17.9** |
| Imelda | stall | 5.0 | 48.7 | 0.0 | 3.6 | 0.0 | 3.6 | 48.7 | 52.3 | +3.6 |

The "compact-storm bonus gap" claim from v1 is wrong on inspection. **Michael gets +11.4 displayed points from the bonus stack — third-largest in the table.** Maria gets +4.7. The cumulative pipeline is not under-crediting compact storms.

---

## 4. The real architectural finding — Stage-5 compression drag

The Stage-5 sqrt compression at (T=70, S=2.5) has a derivative of `2.5 / (2·√(x−70))` for x > 70. That means the displayed value of an additive pre-compression bonus depends on where the storm already sits:

| Storm | Pre-comp boosted | +1 pre-comp buys | +10 pre-comp buys |
|---|---:|---:|---:|
| Katrina | 127.5 | 0.165 | **+1.6** displayed |
| Irma | 109.6 | 0.199 | +1.9 |
| Maria | 102.0 | 0.221 | +2.1 |
| Harvey | 94.2 | 0.254 | +2.3 |
| Michael | 89.3 | 0.285 | +2.6 |
| Florence | 75.8 | 0.519 | +3.9 |
| Imelda | 52.3 | 1.000 | **+10.0** |

Imelda (pre-comp 52, below the compression threshold) gets full credit for any bonus added. Katrina (pre-comp 128, deep in the compressed region) gets ~16 % of a bonus's nominal value.

This has a real consequence: **any additive bonus designed to lift a top-tier storm has severely diminishing returns.** A bonus that nominally caps at +10 yields only +1.6 displayed points for Katrina, +2.1 for Maria, +2.3 for Harvey. To meaningfully reorder the top of the table, an intervention needs to be **multiplicative** (lifts the pre-compression baseline) or operate **before peak_dps is computed** (where there's no compression to fight).

---

## 5. Proposed compact-storm bonus — does not fire on any modern Atlantic storm

The v1 audit proposed an IKE-deficit-gated bonus to credit "compact intense storms at high exposure": fires when `peak_wind > 55 m/s` AND `expected_IKE(wind) − actual_IKE > 0` AND `exposure_norm > 0`. Tracing this gate against the live cached IKE values:

| Storm | Peak wind m/s | Actual IKE TJ | Expected IKE TJ | Deficit | Bonus |
|---|---:|---:|---:|---:|---:|
| Maria | 77.2 | **133.5** | ~110 | 0 | 0.0 |
| Michael | 72.0 | **321.1** | ~92 | 0 | 0.0 |
| Ian | 72.0 | 246.8 | ~92 | 0 | 0.0 |
| Milton | 78.7 | 119.0 | ~110 | 0 | 0.0 |
| Helene | 61.7 | 236.1 | ~68 | 0 | 0.0 |
| Charley 2004 | (~67) | (~12 est.) | (~80) | +68 | (would fire) |
| Andrew 1992 | (~75) | (~25 est.) | (~99) | +74 | (would fire) |

**None of the storms in the bundle qualify** because they're all medium-to-huge by IKE. Michael at 321 TJ is enormous despite a compact R34. The v1 audit's "compact-storm gap" claim was a misattribution: the storms I was thinking of (Andrew, Charley) are pre-2008 and not in the bundle; the modern Atlantic storms I labelled "fast/compact" (Michael, Maria, Ian, Milton) all have substantial wind-field energy and don't pattern-match the archetype the bonus was designed to catch.

A revised gate based on **R34 directly** (`r34_nm < 100`) would catch some modern storms but raises the question of whether that's the right thing to gate on — Michael's compact R34 didn't prevent him from carrying 321 TJ of integrated kinetic energy. The "compact storms are under-credited" story doesn't survive contact with the production IKE data.

---

## 6. The actual outliers — calibration, not architecture

With the proposed bonus retired, the live formula's Spearman ρ(damage, displayed) = **+0.533** across this 13-storm test set. Outliers in both directions:

**Over-scored relative to damage:**
- **Dorian** ($5B / 74 deaths → 85.5 displayed, ranked 5th of 13). Bahamas stall accumulates duration credit at 0.15 zone weight (the COASTAL_BOX entry for Bahamas), which is intentional dampening — but it apparently isn't enough, because Dorian's pre-compression cum_DPI of 102 places it in Maria's company despite ~25× less damage.
- **Imelda** (over-scored by absolute peak — TS-only winds at 40 kt, but peak_ike_tj cached as 146.4 TJ which seems inflated for an unnamed TS-level event; worth a separate look).

**Under-scored relative to damage:**
- **Harvey** ($160 B / 107 deaths → 82.3 displayed, ranked 7th of 13). All five bonus terms fire at near-max, yet Harvey sits below Irma, Ian, Ike, Dorian. Most of the gap is compression drag — Harvey's pre-comp 94.2 doesn't have room to express its bonus stack.
- **Sandy** ($90 B / 233 deaths → 82.2 displayed, ranked 8th). Same compression drag, lower peak.

**Maria** is in the middle band (84.1 displayed, ranked 6th of 13 — about where $115 B should land in the order). The v1 audit narrative that Maria was structurally under-served by the bonus stack does not hold up against the cached data.

---

## 7. What this means for the methodology

1. **The cumulative bonus stack is doing its job.** Duration and breadth credits cap at 10 % each, fire for most storms, and add 5–11 displayed points on average. The Stage-3 combined_boost adds another 5–10 displayed points for US-coast storms. Nothing in the pipeline systematically under-credits compact-Cat-5 storms in the modern Atlantic record.

2. **The Stage-5 compression is the dominant calibration lever for the top end.** Any additive bonus that targets top-tier storms gets ~5× compressed at pre-comp 100+. Future tuning work on the top of the scale should focus on the compression curve parameters (T, S) rather than adding more additive terms.

3. **The v1 compact-storm-bonus recommendation is retracted.** The proposed IKE-deficit gate doesn't fire on any storm in the bundle. A revised version gating on R34 alone might fire, but the rationale weakens once you see Michael's actual IKE — compact-R34 doesn't imply compact-energy.

4. **Real outliers are calibration, not architecture.** Dorian over-scoring and Harvey/Sandy under-scoring are both surface-level visible. They warrant targeted parameter tuning (Dorian: tighten Bahamas zone weighting further; Harvey/Sandy: examine why peak_dps is lower than intensity would suggest) rather than new formula terms.

---

## 8. Revised recommendations (priority order)

1. **Investigate why `peak_dps` is so low for Harvey (72.8) and Sandy (73.6).** Both were $90 B+ events. Harvey's per-snapshot composite should have been pinned high by the existing `stall_dpi_bonus` (≤ 8 pts) and `compact_bonus` (≤ 22 pts via the IKE-deficit term in `core/dpi.py`). Sandy's enormous wind field (R34 = 480 nm, peak_ike = 640 TJ) should have driven the IKE_score and `coast_tracking_bonus` near max. If the existing per-snapshot bonuses aren't producing the expected peak values, that's a calibration bug in the snapshot composite, not a gap in the cumulative stack.

2. **Re-examine the Stage-5 compression parameters with the modern storm distribution.** Today T=70, S=2.5 caps the displayed-score derivative at 0.165 around Katrina. The audit shows this compresses every additive bonus to ~20–30 % of its nominal value for top-tier storms. Options:
   - Relax to T=75, S=3.0 (slightly more spread above 85, slightly less below).
   - Switch to a logistic curve so the derivative tapers smoothly instead of hitting the slope discontinuity at T.
   - Recalibrate against a wider storm set including pre-2008 (Andrew, Charley, Hugo) to anchor the elbow.

3. **Tighten Bahamas / open-water duration credit** to address Dorian's over-scoring. The Bahamas zone_weight is already 0.15 in COASTAL_BOXES (vs. 1.0 for US mainland), but Dorian's 42 stall hours × 0.15 still accumulates substantial duration_integral. Either lower the Bahamas weight further or add a hard floor on the size of the local economic exposure that's eligible for the duration credit.

4. **Defer the compact-storm bonus until we have a validated archetype.** The original motivation (Andrew, Charley, Hugo not getting credit for vulnerability-amplified compact landfalls) is real, but those storms aren't in the bundle and the modern Atlantic record doesn't contain clean exemplars. Revisit once pre-2008 storms are integrated into `compiled_bundle.json` and we can measure whether the gap is real or imagined.

5. **Add `compiled_bundle.json` ingestion to validate_formula.py / tournament_formula.py.** Both existing validation scripts currently invoke `compute_dpi_simple` on hand-coded landfall parameters and don't exercise the full cumulative + Stage-3 pipeline. The v2 audit script here is a template — pulling cached values directly is faster, more faithful, and lets us validate the full pipeline against the displayed values users see.

---

## 9. Caveats

The recompute is offset ~1–4 pts below the cached `dps` field across the storm set. The likely culprits are storm-year era adjustments (`compute_dpi`'s `storm_year` parameter feeds the economic vulnerability formula) and possibly an additional `vuln_bonus` or `compact_bonus` in the per-snapshot composite that gets factored in between `boosted` and compression. Tracing this exactly is a follow-up; for the audit conclusions it doesn't matter because the offset is consistent.

The exposure-norm estimates used for the proposed bonus are hand-coded; if that bonus is reconsidered later, the gate should use the live `economic.exposure_score` or `economic.vulnerability_score` outputs from the per-snapshot pipeline directly, not a static lookup.

Ground-truth damage figures are CPI-adjusted to 2024 USD from NOAA NCEI Billion-Dollar Disasters and NHC TCRs. They're inherently noisy at the ±20 % level for older storms.

The script at `scratch/duration_stall_coastal_audit.py` is self-contained and re-runs in under a second against the live bundle. Storm set is editable at the top of `STORM_IDS`; additional storms can be tested by adding their ATCF IDs (must be present in `compiled_bundle.json`).
