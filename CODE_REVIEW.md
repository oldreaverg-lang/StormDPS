# StormDPS — Architecture Review & Streamlining Plan

_Structural review (architecture + high-leverage debt), not a line-by-line audit of all ~31K lines. Last updated: 2026-05-31._

## Survey snapshot

- **80 Python files, ~31,368 lines** + a **6,071-line `frontend/index.html`** (SPA, ~275 KB inline JS).
- Monoliths: `api/routes.py` (2,974), `services/noaa_client.py` (1,620), `core/ike.py` (1,541), `core/economic_vulnerability.py` (1,209), `core/storm_surge.py` (1,113), `main.py` (739).
- **24 root-level `.py` scripts** mixing operational tools with one-off analysis.

## Architecture, briefly

Request → `main.py` / `api/routes.py` → `core/` (IKE, surge, economic, DPI) → `services/` (NOAA/JTWC/JMA/weather clients). **Presets** are pre-computed server-side into `compiled_bundle.json`; **active storms are scored client-side** in `index.html`'s JS. That front/back split is the source of most debt below.

## The 5 structural problems (ranked)

### 1. No single source of truth (root cause of most pain)
- **Scoring logic implemented twice** — server (`dps_engine`/`dpi.py`/`ike.calculate_ers`) *and* frontend (`calculateDPS/IAS/ERS`). They must agree but are independent; an audit doc notes a "~1–4 pt offset" between cached vs recomputed values — they already drift.
- **Coastal/zone reference data duplicated across ≥5 places**: frontend `ECON_ZONES` + `SHELF_REGIONS`; server `ike._ECON_ZONES`, `storm_surge.REGIONAL_COASTAL_PROFILES`, `economic_vulnerability.ECONOMIC_PROFILES`, `build_nri_zones.ZONES`; plus `nri_zones.json`. The `ECON_ZONES`↔`_ECON_ZONES` pair is hand-synced (we edited *both* by hand multiple times in one session — the hazard, demonstrated).

### 2. Parallel / legacy scoring concepts
- `core/valuation.py` + the `/storms/{id}/value` endpoint = an older "destructive value" model (DP + surge + RI), seemingly superseded by DPS/IAS/ERS but still wired up.
- `dpi.py`'s single **DPI** (0.30/0.35/0.35 blend) coexists with the frontend's separate **DPS / IAS / ERS** — two scoring philosophies in one repo.

### 3. Monoliths
`routes.py` (2,974), `ike.py` (1,541), `economic_vulnerability.py` (1,209), and the 6,071-line `index.html`. Hard to navigate; not unit-testable in pieces.

### 4. Root-script clutter
24 root scripts blur operational tools (`build_nri_zones`, `rebake_ike_cache`, `compile_cache`, `build_preload`) with one-off archaeology (`tournament_formula`, `rank_all_storms`, `basin_analysis`, `historical_storms_database`, `scratch/`, `audits/`). No way to tell what's load-bearing.

### 5. Dead / vestigial code
Old IKE band method behind `apply_size_corrections=False`; the FEMA-NRI overlay in `economic_vulnerability` (already removed, commit `bc17aa9`); likely more.

## Streamlining plan (risk-rated)

### Tier 1 — safe, high-value, do-now (data + hygiene; no scoring-logic change)
1. **Single source of truth for coastal economic zones** — canonical `frontend/econ_zones.json`; server (`core/ike.py`) and frontend both read it. Kills the `ECON_ZONES`/`_ECON_ZONES` hand-sync. Verifiable (output identical).
2. **Externalize `SHELF_REGIONS`** to JSON too (frontend-only today, but de-monoliths the HTML and makes it reusable server-side later).
3. **Quarantine scripts** into `scripts/` (operational) + `archive/` (one-offs); leave root clean.
4. **Delete confirmed dead code** (band method etc.), each with before/after-identical verification.

### Tier 2 — medium, needs care
- Resolve `/value` + `valuation.py` (verify unused by the UI → remove, or document).
- Split `routes.py` into logical routers.

### Tier 3 — big, higher-risk, plan separately
- **Unify the scoring engines** — one implementation. Ideal end state: server computes, frontend renders (removes the JS reimplementation and the drift). Changes the live active-storm compute path → needs its own plan + heavy verification.
- Extract inline JS into modules + a build step (perf + testability; heed the defer/top-level-ref hazards in memory).

## Progress

- [x] **Tier 1.1** — econ-zone single source of truth. Canonical `frontend/econ_zones.json`; server (`core/ike._load_econ_zones`) + frontend (`loadEconZones`) both read it. Verified output-identical (55 zones, server baseline + frontend bbox-logic match; inline script `node --check` clean). _Minor remaining dup: `build_nri_zones.ZONES` still has its own copy of the US bboxes — low-risk (NRI build tool, US-only); fold into the JSON later._
- [ ] Tier 1.2 — externalize SHELF_REGIONS
- [ ] Tier 1.3 — quarantine scripts
- [ ] Tier 1.4 — delete dead code
- [x] (pre-work) Removed orphaned NRI overlay in `economic_vulnerability` — `bc17aa9`

## Guiding rule

This deploys to production on every push. **Every streamlining step must be output-verifiable** (before/after identical for refactors) and committed incrementally — never a big-bang rewrite.
