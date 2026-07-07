# StormDPS data architecture — inputs, seams, and fusion roadmap

**Date:** 2026-07-07 · **Method:** every path below was traced live during the
2026-07-04 → 07-07 audits (Bavi WP092026 end-to-end as the live storm, Katrina
AL122005 / the 223-storm bundle as historical), not reconstructed from memory.
Companion docs: `WP_DPS_AUDIT_V2.md` (formula seams), memory files
`stormdps-track-data-loading.md` / `dps-formula-gaps.md` (session-level history).

---

## 1. Input → surface map

Legend: **key** = cache key, **TTL** = freshness gate. All volume caches live
under `$PERSISTENT_DATA_DIR/cache/` via `storage.cache_read/write`.

### Tracks & intensity (the spine — everything else keys off it)

| Source | Path | Ingest | Cache | Consumers |
|---|---|---|---|---|
| JTWC b-deck (WP/IO/SH) | UCAR RAL mirror `.dat` | `services/atcf_bdeck_client` — 34kt-radii-only rows; MSLP gaps filled from JMA; tz-aware UTC timestamps | IKE cache key `(storm_id, grid, skip)`, live TTL 30 min | `/track` → IKE → `/dps` → hero/SSR/OG; windfield; 3D portrait |
| NHC b-deck (AL/EP) | `ftp.nhc.noaa.gov/atcf/btk` | same client | same | same |
| IBTrACS (historical, global) | NCEI CSV (recent + full) | `services/noaa_client` — **glitch radii rows exist** (r34 300 nm on 35 kt fixes) | same IKE cache, permanent | catalog, baked bundle, ad-hoc `/dps` |
| HURDAT2 | NHC | fallback only | — | — |

**Cadence rule (deliberate):** live/current-season tracks are densified to ~3 h
(`_densify_snapshots_3h`, gap 4.5–13.5 h only, current-season-gated); baked
historical stays at native 6 h. One storm therefore has ONE cadence story per
lifecycle stage, never mixed.

### Forecasts

| Basin | Source | Gaps |
|---|---|---|
| AL/EP | NHC GIS GeoJSON (official track + cone) | — |
| WP/IO/SH | JTWC text bulletin → synthesized cone (`_CONE_ERR_NM`, JTWC 5-yr means) | **no wind radii, no MSLP** in bulletins → `/value` surge null; cone is climatological, not per-storm |

`/forecast` carries `valid_time_utc` + `fetched_at_utc` (forecast-age chip).

### Imagery

NASA GIBS WMTS via `api/satellite_routes` proxy; volume tile cache 48 h TTL,
prefix `sat-variant/ts/z/x/y`. GOES visible = GeoColor (self-night-rendering);
Himawari visible = raw Band 3 (**black at night** → frontend `_autoIRForFrame`
solar-elevation switch to Band 13 IR); **no Himawari GeoColor, no Meteosat on
GIBS**. Consumers: 2D satellite layer, 3D portrait plane/canopy (3×3 z6 stitch,
grayscale + blue tint).

### Environment overlays

Open-Meteo (wind `/wind/field`, pressure, precip) — **the binding constraint:
~10 req/min free tier shared across all three layers and ALL users**, server
WIND semaphore = 1. Volume cache `(bbox_key, ts)` 48 h TTL, `X-Wind-Cache`
header. Client: windowed prefetch (12 frames nearest view, serial, paced on
miss/429). ERDDAP blended SST: 1–2 day publication lag → newest fixes null.

### Ground truth & validation

| Data | Source | Key | Seam |
|---|---|---|---|
| Rainfall anchors | GPM IMERG Late (live loop) / Final (backfill) | **ATCF id (live) / SID (backfill)** | → seam #1 |
| Observed surge/wind | CO-OPS + NDBC | storm_id + track fingerprint | **US-only** |
| Vulnerability | FEMA NRI (`nri_zones.json`) | zone name | US-only; territories hand-tuned |
| Econ exposure | `econ_zones.json` (single source, server+client) | bbox + distance-taper | global but hand-tuned |
| Impact validation | OpenFEMA declarations | storm | US-only; intl damage/deaths hand-collected |

---

## 2. The four seams (verified) and their status

### Seam 1 — storm identity (HIGHEST LEVERAGE)
One physical storm exists as ATCF id, IBTrACS SID, and name+year, and sidecars
key inconsistently. **Verified symptom:** Sinlaku 2026 read rainfall "Historic /
43.1 in" as `WP042026` and "Normal / no anchor" as `2026099N09152`; both appear
in the catalog.
- **Fixed now (2026-07-07):** `/dps` and the warm loop derive `year` from the
  SID's leading 4 digits, so the existing `get_by_name_year` fallback resolves
  anchors for SID lookups. This closes the *user-visible* rainfall flip.
- **Roadmap (the real fix):** a canonical identity layer — internal id + alias
  table (ATCF ↔ SID ↔ name+year ↔ genesis fingerprint, which `compile_cache.
  _genesis_fingerprint` already computes for bundle dedup). Build the alias map
  from IBTrACS's `USA_ATCF_ID` column at catalog build; resolve EVERY sidecar
  read (ground_truth, imerg_rainfall*, DPS cache, catalog dedup) through it.
  Name+year matching is a heuristic; the alias table is ground truth.

### Seam 2 — validation at ingest, not consumption
The radii-sanity gate (`_plausible_ike_tj`) and tz-normalization
(`_parse_timestamp` fromisoformat) live in CONSUMERS. Each new consumer re-hits
the same landmines (this bit us twice with timestamps). **Roadmap:** a single
`validate_snapshot()` pass applied inside all three track clients so every
consumer receives identical pre-cleaned snapshots: radii-vs-vmax envelope,
tz-aware→UTC, unit checks, monotonic timestamps, sentinel (999/-999) stripping.
Scoring-adjacent — ship with an A/B over the 244-storm preload set (expected:
only the known glitch rows move, same set the consumption gate already clamps).

### Seam 3 — one storm, one story across surfaces
The 145 kt (SSR) vs 140 kt (bundle) class of bug: surfaces computed from
different track resolutions/caches can disagree. skip_points unification fixed
the known case. **Roadmap:** extend `/health/selfcheck` with a per-active-storm
cross-surface probe (SSR peak == bundle peak == b-deck max; catalog category ==
bundle category; band label == banded unrounded score) so divergence pages you
via the existing healthcheck cron instead of a user noticing.

### Seam 4 — basin coverage asymmetry (quantified for a WP page vs AL page)
| Layer | Atlantic storm | WP storm today |
|---|---|---|
| Observed surge/wind peaks | CO-OPS+NDBC populated | **empty** |
| Forecast radii/MSLP | NHC full | **absent** (JTWC text) |
| Surge/econ region profiles in peak_dpi | US profiles live | **dead legs** (Tranche B) |
| NRI vulnerability | data-driven | hand-tuned (territories) / none |
| True-color night imagery | GeoColor | auto-IR fallback |

---

## 3. Prioritized integration fixes

| # | Fix | Effort | Impact | Scoring-neutral? |
|---|---|---|---|---|
| 1 | SID year derivation (ground-truth fallback works for SIDs) | XS | Closes the visible dual-ID rainfall flip | ✅ shipped 2026-07-07 |
| 2 | Alias table from IBTrACS `USA_ATCF_ID` + resolve ground_truth/IMERG reads through it | S–M | Retires the identity bug class; catalog dedup gets exact | ✅ neutral |
| 3 | `/health/selfcheck` cross-surface probe (seam 3) | S | Divergence detection instead of user reports | ✅ neutral |
| 4 | Ingest-side `validate_snapshot()` (seam 2) | M | One validation story; new consumers safe by default | ⚠️ A/B required |
| 5 | Catalog: collapse SID+ATCF duplicates via alias table | S (after #2) | One Sinlaku in search results | ✅ neutral |
| 6 | Tranche B: land_proximity WP waypoints + WP profiles (WP_DPS_AUDIT_V2 §7) | L | Fixes the dead-legs asymmetry at the formula level | ❌ full ceremony |

## 4. New-source evaluation

Ranked by seam closed per unit effort. Licensing/limits noted from general
knowledge — **re-verify terms at integration time.**

| Source | Closes | Cost/terms | Effort | Verdict |
|---|---|---|---|---|
| **ATCF a-decks** (same UCAR/NHC dirs as b-decks) | Climatological cone → real per-storm model spread; upgrades forecast ERS + JTWC radii gap (aids carry radii) | Public | S — same parser family we own | **Do first** |
| **IOC sea-level stations** (ioc-sealevelmonitoring.org) | US-only observed layer → global observed surge for WP/IO/SH | Free, attribution; be gentle on their API | M — one client, mirrors CO-OPS shape | **Do second** |
| CIMSS **ADT/SATCON** | Objective intensity between 6 h fixes; live RI detection quality | Public text products | S–M | High value for live storms |
| **CIRA SLIDER / JMA Himawari tiles** | True-color WP imagery (kills auto-IR compromise) | Attribution / JMA redistribution terms need review; non-slippy grids | M–L (reprojection) | Parked option-2 in satellite_routes; worth a spike |
| **ERA5 via Copernicus CDS** or Open-Meteo paid | The 10 req/min overlay ceiling | CDS free w/ queue latency; OM commercial ~€/mo | S (swap fetcher) | Decide when overlay usage grows |
| **JMA/PAGASA best tracks** | WP authority cross-check vs JTWC (1-min vs 10-min winds!) | Public | M + unit reconciliation | With Tranche B validation set |
| **WorldPop/GHSL + gridded GDP** | Exposure integrator (deaths/ERS gap; Kalmaegi class) | Free (CC BY) | L | The Tranche B/R5 backbone |
| **EM-DAT** | Damage validation at scale (replaces hand-collected) | Free for research, registration | S | Do with next basin audit |
| **VIIRS night-lights** (static annual composite) | Own GeoColor-style night blend for non-GIBS basins | Public domain | M (one-time bake) | Pairs with SLIDER spike |
| Sentinel-1/RCM **SAR winds** | Eyewall-resolving observed winds; radii validation | Free but sparse/irregular passes | L | Later — research-grade |

**Two-move opening (recommended):** a-decks + IOC stations. Both are small,
scoring-neutral, reuse existing parser/cache patterns, and each closes a seam
users can see (honest per-storm cones; observed layer for the basin where the
live storm actually is).
