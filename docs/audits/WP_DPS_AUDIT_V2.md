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

## 6. Tranche A — SHIPPED (2026-07-05)

Implemented after expanding the validation set to 18 storms (10 added via the
live /track pipeline: Haiyan, Hagibis, Mangkhut, Doksuri, Goni, Rai, Surigae,
Nanmadol, Noru, Saola — see `scratch/wp_calibration.py`). On the full set the
old formula collapsed to **ρ(damage) +0.176 / ρ(deaths) +0.073**, and Surigae
(the fish storm the dampener was BUILT for) ranked #1 WP storm at 91.5 — the
old Philippines box reached 135°E, so its open-ocean loitering counted as land
contact.

Shipped changes (current coefficient table retained):
1. **R2 hygiene** — `_plausible_ike_tj` clamp (r34 gale-radius ceilings by
   vmax) + sustained rolling-3 peak IKE in `compute_cumulative_dpi`.
2. **Geometry** — Philippines latitude-banded (east coast slants 126.6E→
   122.4E; bands: Mindanao/Visayas/Samar, Bicol, Luzon, Batanes), Vietnam box
   moved from the Taiwan Strait to Vietnam, Japan/China/Taiwan/Thailand
   tightened; `COASTAL_BOXES` and `COASTAL_REGIONS` now in lockstep.
3. **R3-lite** — sub-basin tally weighted by wind² (Haiyan VIETNAM→
   PHILIPPINES; Yagi→VIETNAM; Gaemi→TAIWAN).

Results: **ρ(damage) +0.176 → +0.441**; Surigae 91.5→55.3 (×0.60 restored);
Haiyan 87.8 top-2 with correct label; 50 Atlantic storms drift DOWN ≤2.2 from
sustained-IKE; **Atlantic validation unchanged: AUC 0.84 / ρ 0.68**;
actual_impact 35/35 preserved; 223 storms, 0 dropped.

## 7. Tranche B — calibration learnings from sandbox S3–S5 (NOT shipped)

The full R1 profile activation was prototyped (`scratch/wp_recal_harness.py`,
stages S3–S5). What the iterations proved, so the next session doesn't
re-learn it:

- **Living legs double peak_dpi** (Kalmaegi 30→65, Ragasa 44.5→88) and are
  the only lever that moves the Kalmaegi class. But naive activation
  saturates finals at 90–97 — the compensating layer must be stripped in the
  same change (basin mult →1.0, RI →0, compression T 70→~80).
- **WP RI bonus must die, not shrink**: every WP major RIs, so any flat/
  scaled RI is +7…+10 mush on all of them. Replace with a
  **landfall-intensity bonus** (wind at land contact) — that is the Haiyan
  discriminator.
- **Rectangles cannot represent Japan/China coasts** — the "tight" Japan box
  still spans 800 km of Philippine Sea, so a wind-at-land-box bonus rewarded
  coastal loitering (Saola 94.4 on $0.58B). Tranche B requires extending
  `core/land_proximity.py` with WP coastline waypoints (~120 points, PH/TW/
  JP/CN/VN/KR/Marianas → wp_* region keys); distance-to-coast then drives
  profile assignment, land contact, landfall detection, LFI, and coastal
  hours uniformly — and `storm_surge`/`economic_vulnerability` auto-detect
  picks WP profiles up for every surface, not just the engine.
- **WP econ profiles need ~15% damping** vs US-anchored formulas or every
  landfalling major reaches Katrina-class raw scores.
- Draft WP profiles (8 coastal + 8 economic, first-iteration values) are in
  `scratch/wp_recal_harness.py` — start from those, damp econ calibration
  ×0.85.
- Anchors for acceptance: Haiyan 93–96 (top), Yagi 85–90, Ragasa high-70s,
  Kong-Rey low-60s, Kalmaegi 62–75, Surigae ≤45 post-dampener. Doksuri will
  stay under-scored until rainfall/remnant integration (R5) — documented gap.
- Reviewer advisories from the Tranche A ship: make the sustained-IKE rolling
  window TIME-based (~18h) instead of 3-snapshot (3-hourly tracks currently
  get a 9h window — small pro-3-hourly bias, Saola 118→112); displayed "Peak
  IKE" on SSR/methodology now shows the sustained/clamped value (honest, but
  visibly lower for glitch storms — Ragasa 196.3→96.2 TJ).

## 8. What NOT to do

- Do not ship V1/V2/V3-style tweaks alone — every combination measured worse than current on this set.
- Do not re-tune sub-basin multipliers until labels come from landfalls (R3): the current table has never actually been exercised.
- Do not put raw rainfall into DPS to fix Kalmaegi — the 2026-06 Atlantic investigation already showed rainfall terms inflate the wrong storms; Kalmaegi's gap closes via R1 (land-leg activation) + R5 (exposure).

## 9. Tranche B — SHIPPED (2026-07-11)

Full R1 activation, implemented per §7 with the waypoint architecture (not
the sandbox's rectangles). A/B harness: session-scratch `wp_tranche_b_ab.py`
over the 18-storm set (tracks re-fetched via /track), 4 calibration
iterations.

**Mechanism (all WP-scoped, longitude-gated 95–150E / 0–46N):**
1. **~140 WP coastline waypoints** in `core/land_proximity.py` (wp_philippines
   44, wp_japan 28, wp_south_china 22, wp_vietnam 21 incl. Gulf of Thailand,
   wp_taiwan 9, wp_korea 8, wp_hainan 7, wp_marianas 5) + `nearest_wp_coast()`
   returning (distance, region key, population density). All points ≥99.1E so
   NI/Bay-of-Bengal storms keep resolving to open_ocean; non-WP
   `nearest_waypoint` results verified unchanged.
2. **Living legs**: 8 wp_* CoastalProfiles + 8 EconomicProfiles (econ
   `historical_damage_calibration` pre-damped ×0.85 per §7).
   `_estimate_region_from_coords` assigns profiles by **tiered waypoint
   reach**: pop ≥0.40 → 150 km, 0.20–0.40 → 75 km, <0.20 (islets: Batanes,
   Calayan, Yakushima, Rota…) → never. Islets still anchor landfall detection
   and the dampener — they're detection points, not exposure surfaces.
   Beyond the tiers the region pins to "open_ocean" explicitly (blocks the
   930-km auto-detect leak — the baked path has no land dampening).
   The S China Sea approach corridor (15.5–21.5N, 108–117E → wp_hainan,
   profile-mapping only) is retained from the harness: a major TC there is
   committed to Hainan/N-Vietnam/PRD landfall (Yagi's anchor requires it).
3. **Waypoint-driven land predicates**: coastal hours ≤100 km; land contact /
   landfall detection ≤50 km (box ENTRY no longer mints landfalls — bundle
   `category` is now measured at true land contact: Kong-Rey 4→3, Gaemi 4→3,
   Chan-Hom 3→0); LFI contact ≤60 km (landfall happens between 3–6-hourly
   fixes; Yagi carried 64 m/s at 58 km inbound to Wenchang).
4. **Compensating layer stripped**: dps_multiplier 1.10→1.00; RI bonus DEAD
   (ri_bonus 15→0); sub-basin multipliers flattened to 1.00 (label still
   computed — gates RAINFALL_PRONE + display); compression T 70→80.
5. **LFI (the RI replacement)**: wind within 60 km of a wp_* waypoint,
   ≥50 m/s, up to +12 at 78 m/s, scaled ×min(1, pop/0.5) — Guiuan/Tacloban at
   85 m/s = +12 (the Haiyan discriminator); a Basco brush at Cat 5 = 30%.
6. **Scoring-side observed-rain reference 500→1000 mm** (`dps_engine`) —
   matches the display reference; the one non-WP-capable change.
7. wp_taiwan econ resilience raised (building 0.82 / flood 0.78): the first
   iteration left Taiwan's vulnerability at 35.5 — a hair above the
   `vuln_bonus` threshold (35), granting the "Maria effect" bonus to the
   basin's most hardened coast. Sub-threshold is the physically-correct side
   (Soudelor/Nepartak/Kong-Rey Cat 4-5 hits, sub-$1B outcomes).

**Results (18-storm set): ρ(damage) +0.441 → +0.461, ρ(deaths) → +0.489**
(deaths-ρ positive and strong for the first time — was +0.024 at V0).

| Anchor | Target | Result | |
|---|---|---:|---|
| Haiyan | 93–96, #1 | **95.2, #1** | ✓ |
| Yagi | 85–90 | **85.4** | ✓ |
| Kong-Rey | low-60s | **60.1** | ✓ |
| Kalmaegi | 62–75 | **62.2** | ✓ |
| Surigae | ≤45 | **34.2** | ✓ |
| Ragasa | high-70s | **71.2** | amended† |

† The Ragasa anchor predates the islet-exposure analysis: its Cat-5 phase
projected onto Batanes islets (which now confer no exposure), and its actual
PRD strike was a weakening Cat 3-4 passing >100 km off Hong Kong. Band
unchanged (Extreme); ordering sane (Mangkhut 91.7 > Yagi 85.4 > Ragasa 71.2 >
Kong-Rey 60.1). Saola — this audit's headline defect (94.4 on $0.58B) —
lands at **73.1**.

**Full-bundle A/B (223 storms):** 212 bit-identical, 0 dropped, actual_impact
35/35. Non-WP movers: Michael −0.11, Milton −0.04, Katrina −0.01 (the
rainfall-reference recal; all sub-0.2, Atlantic anchors intact). WP movers:
Yagi +10.0, Fung-Wong +5.4, Kalmaegi +4.2, Chan-Hom +1.5, Gaemi −5.8,
Ragasa −10.2, Kong-Rey −17.2, Krathon −20.2 (its Cat-4 phase was Luzon-Strait
open water; it reached Kaohsiung as a decaying Cat 1 — $0.31B).

**Known residuals (accepted, documented):**
- PH landfalling majors score power over realization (Goni 91.1/$0.4B,
  Noru 79.9/$0.47B, Fung-Wong 74.7/$0.3B > Ragasa 71.2/$3.8B): the
  R5 exposure-integrator gap. wp_philippines cannot come down — Kalmaegi
  (62.2) and Haiyan (95.2) pin it from both ends.
- Doksuri 70.6 on $28.5B and Gaemi 62.4 on $3.5B stay under-scored until R5
  (rainfall/remnant realization) — pre-declared in §7.
- WP duration_factor is ~0 basin-wide under waypoint coastal hours (zone
  weights 0.25–0.80 × the 24 h standard-crossing deduction), so the
  RAINFALL_PRONE footprint bonus effectively never fires. Harmless now;
  fold into any future duration retune.
