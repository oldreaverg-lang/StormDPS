# Rainfall hazard score — design options (StormDPS ← SurgeDPS)

**Status:** design / decision doc. Nothing here is shipped. Written so that
when the next Atlantic storm forms we can run several options in parallel and
compare which one best predicted the rain hazard.

**Why this matters.** Freshwater flooding from rainfall is the single largest
killer in tropical cyclones — NHC's Rappaport (2014) put it at ~27% of US TC
deaths 1963–2012 and rising (Rappaport 2014 later revisions and Rebora et al.
push the "inland flooding" share toward half; globally it is higher because
the deadliest TCs — Mitch, the SW Indian Ocean storms, WP rainfall events like
Kalmaegi — kill mostly by water, not wind). StormDPS's DPS is a wind/surge
destructive-potential index and weighs rainfall almost not at all:

- `core/dps_engine.py` rain contribution = `rain_inland_factor` (cap **0.04**)
  + `inland_pen_factor` (cap 0.04, **US-bbox only**, 25–48 N / 100–66 W)
  + `stall_bonus` (cap 0.05). Max combined ≈ 13% nudge to `peak_dpi`.
- The rain magnitude behind those factors is `_rain_score_for_dps`, derived
  from a **kinematic estimator** (peak-rain-rate × stall hours, capped 760 mm)
  or an **observed** anchor (GPM IMERG / curated gauge, referenced to 1000 mm).
- There is **no forecast rain** and **no independent rain score** anywhere.

This is the long-documented "R5 gap" (see `docs/audits/WP_DPS_AUDIT_V2.md`,
`memory/dps-formula-gaps.md`): Kalmaegi (266 deaths, mostly Cebu rain-flood),
Doksuri, Hagibis, and Gabrielle all score low because their destruction was
water, not wind.

**Why SurgeDPS is the right producer.** SurgeDPS is already a building-level
compound-flood model (surge + rainfall/riverine + wind) with a forecast-native
rain stack that is ~built but currently dormant (`rainfall_diagnostic_report
.json` shows `rain_depth_populated: 0`). It is CONUS/Atlantic-scoped, which is
exactly the basin we care about here. It can produce a *forecast* rain number;
StormDPS just needs to consume it.

---

## The three decisions

The pipeline is **(A) what data → (B) what score → (C) how it enters DPS**.
Each is an independent choice; the options below can be mixed.

```
 WPC QPF ─┐                    ┌─ magnitude (mm vs ref)      ┌─ replace _rain_score_for_dps
 NWM/HAND ─┼─▶ [rain score] ──┼─ anomaly (Atlas-14 ARI)  ──┼─ new DPS rain component
 IMERG    ─┤   0–100          ├─ inundation (HAND depth)     ├─ separate "Rain Hazard" axis
 Atlas14  ─┘                  └─ exposure (SurgeDPS $/pop)   └─ feed IAS
      (A)                          (B)                             (C)
```

---

## A. Data-source options (feed-in) — the "future forecast"

All are free / no-auth. Column meanings: **lead** = forecast horizon;
**res** = spatial resolution; **cov** = basin coverage; **state** = already in
a repo?

| # | Source | What it gives | Lead | Res | Cov | State |
|---|---|---|---|---|---|---|
| A1 | **WPC QPF** (`SurgeDPS noaa_fetchers.QPFFetcher`) | Forecast storm-total rainfall (mm) on a grid | 0–72 h | 2.5 km | CONUS | **built** |
| A2 | **NWM discharge → HAND** (`SurgeDPS nwm_http_fetcher` + `flood_model/hand_model`) | Forecast **flood depth/extent** from routed rainfall+streamflow | 0–18 h (short), 10 d (med) | reach + DEM | CONUS, gauged reaches | **built** (gauge-based) |
| A3 | **GPM IMERG Early** | Near-real-time global rain (not a true forecast, ~4 h latency) | nowcast | 0.1° | global | observed side already in StormDPS |
| A4 | **Atlas 14** (`SurgeDPS atlas14_fetcher`) | Return period (ARI) for a given depth+duration — the *anomaly* normalizer | climatology | point | CONUS | **built** |
| A5 | **NBM / GFS / ECMWF QPF** | Global forecast precip (coarser than WPC) | 0–240 h | 2.5–13 km | global | not fetched |
| A6 | **MRMS QPE** (`SurgeDPS mrms_fetcher`) | Observed radar precip (verification / nowcast blend) | observed | 1 km | CONUS | **built** |

**Reading:** for an *Atlantic* storm the strong pair is **A1 (WPC QPF)** for the
forecast rainfall field and **A4 (Atlas 14)** to say how anomalous it is; **A2
(NWM→HAND)** upgrades "how much rain" to "what actually floods." A3/A5 exist so
the same score can later extend to WP/SH basins where WPC/NWM don't reach.

---

## B. Independent rain-score options (0–100)

The score is a **hazard** number (how dangerous the rain is), computed the same
way for any storm so scores are comparable. Four formulations, increasing in
fidelity and cost:

### B1 — Magnitude score (simplest)
`score = 100 · min(1, forecast_storm_total_mm / REF)`, REF ≈ 1000 mm
(Harvey peaked ~1539 mm). Pure "how much water falls in the cone."
- **+** trivial from A1; one number; already half-built (StormDPS uses a 1000 mm
  reference on the observed side).
- **−** exposure-blind and climate-blind: 600 mm over a swamp scores the same
  as 600 mm over Houston, and 400 mm scores "low" even where 400 mm is a
  1000-year event.

### B2 — Anomaly score (Atlas-14 return period)
Convert the forecast depth+duration to an **Annual Recurrence Interval** via
Atlas 14, then `score = f(ARI)` (e.g. 10 yr→40, 100 yr→70, 500 yr→90,
1000 yr→100).
- **+** this is the *true* hazard signal — "how far beyond normal." Harvey and
  Florence were ~1000-yr events; the score would light up correctly. Naturally
  handles wet vs dry climates.
- **−** CONUS-only (Atlas 14); needs the depth field co-located with the ARI
  grids. Return period ≠ impact (a 1000-yr rain on an empty coast still floods
  nothing).

### B3 — Inundation score (NWM → HAND)
Forecast rain/streamflow → predicted flood **depth** and **inundated area** via
the HAND terrain model; `score = f(area × depth, or population in the
inundation footprint)`.
- **+** the most physically honest "what will actually flood." This is the
  "solid number" — not rain in the sky but water on the ground.
- **−** heaviest; gauge-limited (ungaged small streams don't flood in v1);
  CONUS-only.

### B4 — Exposure score (SurgeDPS damage aggregate)
Run SurgeDPS's building-level rain-flood model over the cone and aggregate:
rain-driven HAZUS loss ($) and/or population in >X ft rain-flood → normalize.
- **+** exposure-aware realization — distinguishes 500 mm on Miami from 500 mm
  on the Everglades. Reuses the whole SurgeDPS pipeline.
- **−** heaviest compute; conflates hazard with exposure (which per the DPS
  design belongs in IAS/ERS, not DPS — see C).

### B★ — Composite (recommended to *compute*, so we can decompose)
`rain_score = w1·B1(magnitude) + w2·B2(anomaly) + w3·B3(inundation)`, with the
three sub-scores stored separately so the storm post-mortem shows which term
carried the signal. Start `w = (0.35, 0.35, 0.30)`; retune after the first
real storm.

---

## C. Loop-back options (how the number reaches StormDPS)

This is the philosophically loaded choice. StormDPS's stated design rule
(`memory/dps-formula-gaps.md`) is **hazard → DPS/IAS, exposure → ERS**, and the
2026-06 investigation explicitly *rejected* bolting raw rainfall into DPS
because it inflated the wrong storms (Harvey-class over-credited, compact wind
storms under-credited).

| # | Mechanism | Effect on DPS | Pro | Con |
|---|---|---|---|---|
| C1 | **Replace `_rain_score_for_dps`** with the forecast rain score | tiny (keeps the 0.04/0.04/0.05 caps) | 1-line plug; low risk; keeps DPS meaning | doesn't fix under-weighting — rain still ~13% max |
| C2 | **New rain component in the DPS composite** (a 4th term ~15–25%) | large | directly answers ">50% of fatalities" | re-opens the 2026-06 failure mode; changes every baked score; needs full re-audit |
| C3 | **Separate "Rain Hazard" axis** shown next to DPS (own 0–100), optional combined "Total Threat" | none on DPS | cleanest; matches the hazard-axis philosophy; forecast-native, live-only; nothing baked changes | two numbers to explain to users |
| C4 | **Feed IAS** (impact/actual-severity index) | none on DPS | rainfall realization is exactly IAS's job | IAS surface isn't built out yet |

**Recommendation:** **C3** — a distinct, forecast-driven **Rain Hazard** score
sourced from SurgeDPS, displayed alongside DPS on the active-storm card, with an
optional `Total Threat = combine(DPS, RainHazard, SurgeHazard)` roll-up. It
respects the design rule, can't regress any baked score (it's live-only), and
is the honest way to surface a forecast quantity (which is uncertain and
shouldn't be frozen into a historical destructive-power number). C1 is a
zero-risk incremental step we can ship *today* as a placeholder until C3 lands.

---

## Transport: how the number physically moves SurgeDPS → StormDPS

Mirror the existing cross-site contract (StormDPS DPS → SurgeDPS today):

1. SurgeDPS adds `GET /api/storms/{id}/rain_hazard` returning
   `{storm_id, advisory, valid_time, rain_score, subscores:{magnitude,
   anomaly, inundation}, forecast_total_mm, ari_years, source, cone_hash}`.
2. StormDPS's hourly active-storm loop (`api/routes.refresh_active_dps_loop`,
   already polling per-advisory) fetches it, caches it on the `/dps` bundle as
   `rain_hazard`, and the frontend renders it beside DPS.
3. A `rain_parity` probe in `/health/selfcheck` (mirror `surgedps_parity`)
   pages if the two sites disagree about a storm's rain score.

Fail-open throughout: no SurgeDPS response → no Rain Hazard shown, DPS
unaffected.

---

## Validation plan — "compare the results" on the first Atlantic storm

The point of keeping options is to **measure** them. When a storm threatens:

1. At each advisory, compute **B1/B2/B3** from that advisory's **forecast**
   (A1/A2/A4) and log them with the valid time and cone.
2. After the storm, pull the **observed** truth already available to us:
   GPM IMERG / MRMS storm-total (A3/A6), USGS high-water marks
   (`SurgeDPS usgs_hwm`), AHPS crest records, and the eventual NWS/NCEI rainfall
   report + FEMA/NHC damage.
3. Score each option by: (a) did the forecast rain score at T-48/-24 h rank the
   storm's rain threat correctly vs its realized flood damage/deaths; (b)
   calibration — was a "70" actually a ~100-yr-ish outcome; (c) lead-time decay.
4. Keep the option with the best forecast-skill / simplicity trade-off; fold
   the losers' subscores into the post-mortem doc.

Anchor expectations from history (run these retro first, from archived
forecasts where available): **Harvey** and **Florence** should top any decent
rain score (~1000-yr events); **Ida** high (Gulf + NE remnant flooding);
**Michael** low on rain despite Cat 5 wind (fast mover, little rain) — the
storm that proves the rain axis is *independent* of DPS.

---

## Recommended path (lowest-regret)

1. **Now:** ship **C1** (feed the existing `_rain_score_for_dps` from a forecast
   number when a live storm has WPC QPF) — zero risk, immediate.
2. **Wake SurgeDPS's rain stack:** activate the dormant A1+A4 path so
   `/rain_hazard` returns **B★ (magnitude + anomaly)** — no HAND/HEC-RAS needed
   for v1.
3. **Add C3** on StormDPS: a Rain Hazard chip beside DPS, live-only.
4. **Add B3 (NWM→HAND inundation)** as the premium subscore once A2 is verified.
5. **Validate** against the first real Atlantic storm per the plan above, then
   decide whether to graduate any of this into DPS proper (C2) or IAS (C4).

Everything except step 5's decision is additive and reversible; no baked DPS
score changes at any step.
