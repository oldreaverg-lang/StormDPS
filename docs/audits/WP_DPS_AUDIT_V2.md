# West Pacific DPS Formula Audit v2 — Structural Review

**Date:** 2026-07-05
**Scope:** All 8 WP storms in the live compiled bundle (Chan-Hom 2015, Gaemi/Yagi/Krathon/Kong-Rey 2024, Ragasa/Kalmaegi/Fung-Wong 2025), full engine path (not the proxy used in the 2026-04 audit).
**Methodology:** Sandbox variants monkeypatched onto the LIVE engine (`core.dps_engine.compute_storm_dps`), inputs from `frontend/preload_bundle.json` — V0 reproduces `compiled_bundle.json` with max |Δ| = 0.0000. No live code edits.
**Companion script:** `scratch/wp_dps_audit_v2.py` (reruns in seconds)
**Related prior work:** `WP_DPS_AUDIT.md` (2026-04, proxy-based), `WESTERN_PACIFIC_GAPS_ANALYSIS.md`, Bavi live-storm audit (commit `47e08fb` — dampener deferral for active storms).
**Ground truth:** commonly cited damage/death totals (JTWC/JMA/NDRRMC/CMA press figures — same convention as the v1 audit).

---

## 1. Headline result

**The WP formula is two compensating errors deep, and the second layer is at its tuning ceiling.**

1. `peak_dpi`'s surge/rain leg (35% weight) scores **0** and its economic leg (35%) ~14 for every snapshot outside a US region box — and WP storms have no region boxes. WP peak_dpi is structurally 30–45 while Atlantic equivalents run 65–110. ~60% of a WP score is patch-on bonuses.
2. The WP adjustment layer (×1.10 basin, ×0.98–1.15 sub-basin, +7.7–20 RI, +LF/ORO/RAIN, compression T=70) was calibrated **on top of the halved base** to push finals back into the 55–81 band.

Each layer's constants only make sense given the other's defect. The variant experiments prove you cannot fix either alone.

## 2. Current-state validation (V0)

| Storm | DPS | peak_dpi | Damage $B | Deaths | Sub-basin label | Correct? |
|---|---:|---:|---:|---:|---|---|
| Ragasa 2025 | 81.4 | 44.5 | 3.8 | 30 | PHILIPPINES ×1.15 | ✗ (Taiwan / S. China) |
| Yagi 2024 | 78.3 | 44.4 | 14.0 | 844 | PHILIPPINES ×1.15 | ✗ (Vietnam / Hainan) |
| Kong-Rey 2024 | 78.1 | 42.1 | 0.18 | 3 | PHILIPPINES ×1.15 | ✗ (Taiwan) |
| Gaemi 2024 | 73.5 | 38.7 | 3.5 | 150 | PHILIPPINES ×1.15 | ✗ (Taiwan / Fujian) |
| Fung-Wong 2025 | 69.6 | 38.9 | 0.30 | 33 | PHILIPPINES ×1.15 | ✓ |
| Chan-Hom 2015 | 69.5 | 42.2 | 1.6 | 7 | NORTH_CHINA ×0.98 | ✓ |
| Krathon 2024 | 63.1 | 38.8 | 0.31 | 10 | PHILIPPINES ×1.15 | ✗ (Taiwan/Kaohsiung) |
| Kalmaegi 2025 | 55.6 | 30.0 | 0.45 | 266 | PHILIPPINES ×1.15 | ✓ |

**Spearman ρ vs damage +0.452, vs deaths +0.024.**

Standout defects:
- **Kong-Rey ($0.18B, 3 deaths) outscores Gaemi, Chan-Hom, and Kalmaegi.** Kalmaegi (deadliest 2025 PH storm, 266 deaths) is dead last and cannot rise: every downstream knob is multiplicative/additive on its peak_dpi of 30.
- **Kalmaegi records `coastal_hours = 0` despite crossing Cebu** — the duration gate requires DPI > 25 near coast, but its land-adjacent DPI is below even 12 (dead legs + landfall decay). The Atlantic threshold was lowered to 12 in `_BASIN_CUM_TUNING`; WP still uses 25 — but see V1: lowering it does NOT fix this.
- **5 of 8 sub-basin labels are wrong** (density tally over boxes rewards where tracks loiter — the huge PH box sits across the main typhoon corridor). Consequence: **the sub-basin multiplier table has effectively never been validated** — 7 of 8 storms wore ×1.15(PHILIPPINES) regardless of what they hit. The v7 decision to de-risk WP_VIETNAM 1.20→1.10 was made against mislabeled data.
- **Duration factor is 0 for 6 of 8 storms** (zone weights 0.25–0.55 × the 24h "standard crossing" deduction ≈ guaranteed zero). Yagi: 72 coastal hours, dur = 0.

## 3. IKE data glitch (confirmed)

Every anomalous "peak IKE ≈ 196.3 TJ" (Ragasa, Gaemi, Kong-Rey; Chan-Hom 197.5) traces to snapshots where **vmax = 35–40 kt but r34 = 290–300 nm** — physically impossible IBTrACS radii rows (a 35 kt system cannot carry 300 nm gale radii). Identical bogus inputs → identical IKE. These rows:
- saturate `ike_norm` = 2.0 → breadth factor pinned at cap for 3 of 8 storms;
- display as the storm's headline "Peak IKE" (196.3 TJ shown on storm pages now);
- enter via the IBTrACS track path — the b-deck path (live storms) is unaffected.

## 4. Variant experiments — all knob-tunes DEGRADE validation

| Variant | ρ vs damage | ρ vs deaths | Note |
|---|---:|---:|---|
| **V0 current** | **+0.452** | **+0.024** | baseline |
| V1 WP cum tuning (thr 12, caps 0.15) | +0.190 | −0.167 | inflates low-damage lingerers (Kong-Rey +4.0, Fung-Wong +8.6); Kalmaegi +0.9 |
| V2 sub-basin from hurricane-force fixes | +0.286 | −0.143 | only Yagi relabels (→VIETNAM ×1.10) — *lowering* the #1 damage storm; Kong-Rey/Krathon still mislabel |
| V3 IKE de-spike (>1.6× both neighbors) | +0.286 | −0.143 | correct hygiene, but breadth rides single-snapshot maxima either way |
| V4 = V1+V2+V3 | 0.000 | −0.286 | worst of all |
| V5 headroom probe (US-analog region profiles on WP coasts) | +0.167 | +0.167 | peak_dpi 30–45 → **65–88**; finals saturate 90–97 (ceiling returns) |

**Interpretation.** V1–V4: the adjustment layer measures *time-near-boxes* and *bonus arithmetic*, neither of which contains land-impact information — rearranging it is noise. V5: the base signal doubles when the legs live (Kalmaegi 30→65.1, +117%), and deaths-ρ turns positive for the only time in any variant — but the compensating layer then saturates the scale, reproducing the exact v5-era "everything hits 99" failure. **Both layers must move together.**

## 5. Recommendations (priority order)

1. **R1 — Joint recalibration: WP region profiles + adjustment-layer strip-down.** The only structural fix. (a) Add WP coastal region profiles (Philippines archipelago, Taiwan E/W, Pearl River Delta / HK, Vietnam delta, Japan, Korea) to `core/storm_surge` + `core/economic_vulnerability` and extend `_estimate_region_from_coords` — calibrated to WP exposure, NOT US analogs. (b) Simultaneously retire the compensators: basin multiplier 1.10→~1.0, RI rescaled (see R4), sub-basin table re-fit on correctly-labeled landfalls (R3), compression re-anchored. Calibration anchors: Haiyan ≈ 95–97, Yagi ≈ 85–90, Ragasa ≈ high-70s, Kong-Rey ≈ low-60s, Kalmaegi ≈ 65–75, post-dampener fish storms ≈ 40s. Material work; run through `basin-dps-audit` with the expanded set (R6) before shipping.
2. **R2 — IKE radii sanity gate + sustained-IKE breadth.** Reject/clamp radii rows failing a vmax-vs-r34 envelope (e.g. r34 > 150 nm requires vmax ≥ 50 kt) at IBTrACS ingest; make breadth use sustained IKE (rolling 3-snapshot mean or 75th percentile of tropical-phase) instead of the single-snapshot max. Standalone, low-risk, fixes wrong on-page "Peak IKE" today. Ship independently.
3. **R3 — Sub-basin from landfall/land-contact events,** not track density (use the `storm_made_land_contact` machinery; weight by intensity at contact). Ship WITH R1's multiplier re-fit — under the current table, correct labels move scores the wrong way (V2/Yagi).
4. **R4 — Demote RI.** RI spread (+7.7…+20) spans nearly the whole observed DPS spread between ranks 2–7, and intensification *rate* is not land impact. Halve to +2.5…+10 as part of R1's rebalance (not alone — V-experiments show isolated knob moves are noise).
5. **R5 — Mortality belongs in IAS/ERS, not DPS.** ρ(deaths) ≈ 0 is partly by design (hazard→DPS; exposure/vulnerability→ERS/IAS). Ship the exposure integrator (WorldPop/GHSL × wind along track) as a visible IAS component for WP rather than distorting DPS to chase deaths. Kalmaegi's mortality (dense, vulnerable Cebu) is exactly this pathway.
6. **R6 — Expand the WP validation set before any coefficient change.** 8 storms is too few and 2024–25-skewed. Add via IBTrACS: Haiyan 2013, Hagibis 2019, Mangkhut 2018, Doksuri 2023, Goni 2020, Rai 2021, Surigae 2021, Nanmadol 2022, Noru 2022, Saola 2023 (intensity-extreme + damage-heavy + fish-storm anchors).

## 6. What NOT to do

- Do not ship V1/V2/V3-style tweaks alone — every combination measured worse than current on this set.
- Do not re-tune sub-basin multipliers until labels come from landfalls (R3): the current table has never actually been exercised.
- Do not put raw rainfall into DPS to fix Kalmaegi — the 2026-06 Atlantic investigation already showed rainfall terms inflate the wrong storms; Kalmaegi's gap closes via R1 (land-leg activation) + R5 (exposure).
