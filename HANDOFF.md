# StormDPS — session handoff (updated 2026-07-09)

For a fresh session (or another engineer) picking up cleanly. Authoritative
context lives in `CLAUDE.md` (deploy rules, NTFS-mount hazards, project
skills) and the operator's memory files — read those first. This doc is a
point-in-time snapshot, not a spec; verify against current code before acting.

**Live site:** https://stormdps.com · **GitHub:** oldreaverg-lang/StormDPS ·
push to `main` → Railway auto-deploy (~30 s–4 min). Repo:
`C:\Users\Ryan\APPS\StormDPS-recovered`.

**Deploy state:** local HEAD == origin/main == `c5e9fb8` (PSI mobile perf
rounds 2-3: server-injected __ACTIVE_HINT__ so the live storm's /track
starts at ~340 ms instead of serializing behind /storms/active; map
preconnect + VERIFIED-reused Leaflet preload are injected ONLY when a
storm will auto-load — a storm-free shell ships zero map hints. PSI moved
47→55 with TBT halved; tried-and-removed a /track as=fetch preload — API
cache headers block reuse, it double-downloaded. NB: BAVI left the active
list 2026-07-11, so PSI runs now measure the storm-FREE homepage until the
next system.) Also shipped since the mobile review:
Lighthouse fixes for /surgedps (meta description/canonical/OG + WCAG AA
contrast on the dark sidebar — dpsColor ramp lightened, slate-600→400;
SurgeDPS `5e6568b`), catalog cache headers on dynamic year ranges
(`ff41100`, pairs with the operator's new Cloudflare Cache Rule — verified
MISS→HIT on all variants). KNOWN CEILING: PSI mobile TBT (~2 s) is
dominated by the ~350 KB inline-script eval — the structural fix is
splitting the SPA / deferring chart render (backlog).
BAVI (WP092026) has been the live test storm all week.

**Shipped 2026-07-10 (`78f1337` + SurgeDPS `4ace988`) — mobile pass:**
phones no longer auto-load the 3.5 MB bundle (first visit 3.9 MB → 344 KB
raw; storm-browser open warms it, desktop unchanged; fetchStorm's mobile
8-s bundle wait removed — API/volume path serves immediately). Fixed:
storm-title name clipping ("B…"), /compare one-column unattributed grid,
FAQ's third band vocabulary (visible + JSON-LD), static-header collisions
(all 7 pages), clustered map forecast labels (tap-to-show ≤900px).
Backlog #4 (stall-bar fold) + #5 (banner keyboard a11y) DONE. SurgeDPS:
deep link closes the mobile overlay sidebar; "Step 4 of 1" progress clamp.
Follow-up DONE (SurgeDPS `51c39a4`): data/dps_scores.json + catalog.py
resynced to the canonical bundle (430/446 keys had drifted; Ike 87→88.9,
Beryl 55→79.6). **After any future bake, rerun SurgeDPS
`scripts/build_dps_scores.py --bundle <this repo>/frontend/compiled_bundle.json`
and patch the printed catalog.py diffs.** Forgetting it now PAGES:
`/health/selfcheck` carries a `surgedps_parity` probe (`4b85ebf`,
verified live: compared=19 drifted=0) comparing SurgeDPS
/api/storms/historic against the bundle; URL overridable via
SURGEDPS_API_URL, fetch errors advisory.

**Shipped 2026-07-10 (`67d1c2d`):** "one storm, one story" — sidebar
catalog now serves the hero's engine scores (bundle first, live DPS cache
for unbaked storms) via `core/storm_identity.harmonize_catalog`; five
pre-canon label-scheme copies eliminated; SID+ATCF catalog twins collapsed;
`/health/selfcheck` gains a `score_consistency` probe that pages on drift.
Backlog quick-win #3 (catalog label vocabulary) and DATA_ARCHITECTURE
roadmap #3 + #5 are DONE. Full audit + verification:
docs/audits/CROSS_SURFACE_SCORE_AUDIT_2026-07-10.md.

**Shipped 2026-07-10 (`6a1b99d`):** the ALIAS TABLE (backlog #11's core +
DATA_ARCHITECTURE roadmap #2) — `data/storm_aliases.json` (3,804 storms,
rebuild via `scripts/build_alias_table.py`, IBTrACS since1980 CSV) +
`/dps` identity overlay: any id form now returns the canonical name and the
bundle's `actual_impact`. Plus `data/recorded_damage.csv` (~79 curated
figures; the 24-row calibration CSV WINS conflicts — so Helene displays its
calibration $32B, not NCEI's $78.7B; flip precedence deliberately if ever
desired) → bundle enriched 35→181 storms with damage; compare page now
shows "Michael (2018)" + Recorded Damage for every lookup. NB: embedded
python now has pytest (`python -m pytest tests/ -q` works, 133 offline).

---

## 1. Where the product now stands (shipped 2026-07-07 → 09)

Everything below is LIVE, was fresh-eyes reviewed pre-push (`code-review`
skill — it caught real blockers almost every round; keep the habit), and
carries its detail in git log + the docs listed in §4.

- **3D storm portrait finished and re-homed** — the cone iteration completed
  the portrait (dome canopy w/ real imagery, rings riding the cone, eyewall
  plugging the crater rim); it then MOVED off the map to the bottom of
  **/methodology** as a self-contained, fail-open showcase that auto-loads
  the live storm's peak fix (hidden when no active storm / <900 px / any
  error). Last on-map version: git `c841e96`.
- **Map = fixed always-on layer set** (operator-directed): Satellite (active
  storms with track <12 d, desktop only) + Windfield + Wind flow, one Legend
  button, nothing else. Advanced panel, IR/Pressure/Precip/Wind-particle
  toggles and the frame scrubber are gone; the Storm Timeline is the only
  satellite-frame driver; Himawari auto night-IR retained. Mobile (≤900 px)
  never fetches GIBS (initSatelliteLayer early-return). ~2,100 lines of
  machinery excised and archived VERBATIM in `docs/RETIRED_MAP_OVERLAYS.md`
  (restoration = copy-paste + wiring checklist).
- **/compare** — storm-vs-storm page (`frontend/compare.html`, route in
  main.py): verdict line, winner-bolded metric grid, dual-track map,
  overlaid DPS lifetime curves, deep links `?a=&b=`. "Compare" button beside
  Share in the storm title bar; homepage otherwise untouched.
- **Analog finder** — `GET /storms/{id}/analogs` (weighted-L1 over bundle
  features: dps .30 / wind .20 / IKE .20 / rainfall .10 / hours .05 / +.15
  cross-basin; pool via `seo._read_compiled_bundle` = volume-first) plus the
  "Storms like this one" chip strip on the storm page, each chip deep-linking
  into /compare. Live storms route through the (cached) /dps bundle.
- **Perf**: live-track **stale-while-revalidate** (the measured 13-s
  first-viewer stall after each 30-min TTL expiry is gone; contextvar bypass
  keeps the hourly DPS warm loop computing from fresh tracks); operator
  deployed the **Cloudflare Cache Rule for `/frontend/*`** (3.5 MB bundle now
  edge-cached ~1 y; the URI-Full-vs-URI-Path gotcha is in the audit doc).
  Full audit + roadmap: `docs/PERF_AUDIT_2026-07.md`.
- **Flood banner recalibrated (display only)** — 100 ≡ 1000 mm observed rain
  (was 500 mm; most typhoons pinned "Historic… comparable to Harvey").
  DPS-side factors intentionally still use the 500 mm score
  (`_rain_score_for_dps`) — bit-identical across all 244 storms, verified.
  Mobile banner collapses to one line, tap to expand.
- **Task #14 done** — hero label single-sourced from server `dps_label`; all
  client bands (color/severity/context) use the UNROUNDED score.
- **Map key compacted** — 2 chip rows (~40 px vs 242 px); the "Track Energy"
  row was removed outright (`ikeColor()` had zero call sites — it described
  a scale nothing draws).

## 2. Priority backlog (operator-reviewed suggestions, best first)

**Quick wins**
1. ~~**Landfall panel**~~ **SHIPPED 2026-07-09** (`5b105cb`): #landfallBar on
   the storm view. Server: `core/landfall_forecast.py` (+11 offline tests,
   run via embedded python — no pytest there) feeds a fail-open `landfall`
   object on `/storms/{id}/forecast`. Coastline waypoint DB is
   ATLANTIC-ONLY and feeds DPS scoring — do NOT extend it casually;
   out-of-coverage basins (incl. the EP Pacific-Mexico gap) get
   `coverage:false` and the frontend falls back to zone-approach copy via
   computeForecastERS. AL storms deep-link to
   `/surgedps?storm=active_<atcf>` — SurgeDPS `e9de16a` restored ?storm=
   parsing (operator-approved reversal of the 2026-05 removal, explicit
   links only). Verified live with BAVI (zone mode, Shanghai/Yangtze +44h).
2. **Cloudflare Cache Rule #2** (operator dashboard): URI Path
   `/api/v1/storms/catalog*` → eligible for cache (s-maxage=900 already
   sent). Gotcha: field must be URI *Path*, not URI Full.
3. **Catalog label vocabulary bug**: catalog entries can carry off-canon
   `dps_label` values (e.g. Katrina SID shows "Catastrophic" — not in the
   canonical 7-band set). The sidebar/catalog label path (noaa_client
   `calculate_dps`/bake) needs aligning with `core/dpi.categorize_dpi`.
4. **Stall-Risk banner**: apply the flood banner's mobile one-line +
   tap-to-expand pattern.
5. **Flood banner a11y** (review nit): tabindex="0" + keydown toggle.

**Medium**
6. ~~**Bundle diet**~~ **SHIPPED 2026-07-10** (`80c263b`): the SPA eager-
   loads `frontend/bundle_index.json` (~175 KB raw / ~35 KB wire; light
   fields + detail_ids) and fetches `frontend/bundle_storm/<id>.json`
   (median ~13 KB) on storm open via `ensureStormDetail()`. Measured:
   desktop storm page 501 KB total (was ~4 MB); mobile chips scored
   instantly. Artifacts are DERIVED from the monolith by
   `scripts/build_bundle_split.py` — rebake.py + auto-rebake.yml chain it,
   and `tests/test_bundle_split.py` fails CI on drift. Slim entries are
   `_light`-tagged (missing heavy fields: dpi_timeseries/actual_impact/
   rainfall_text/landfalls/ground_truth); `ensureCompiledBundle` self-heals
   them via /dps. Monolith still serves the server side + old cached SPAs.
7. **Catalog cold-start** (CLAUDE.md open item): move IBTrACS warm in
   main.py lifespan after `yield`.
8. **Season dashboard** `/season/2026` and **"on this day"** homepage module
   — both cheap aggregations over the catalog (feature brainstorm 2026-07-09).
9. **/compare & analogs polish**: OG share card for comparisons; analogs on
   the /compare page itself; consider a `?an=&bn=` name-hint for ATCF deep
   links (names fall back to raw IDs there — see §3 seams).
10. **3D showcase polish** (in payoff order): cirrus outflow deck (parallax,
    anticyclonic), eyewall updraft/eye subsidence, rainband arc traces,
    bloom last.

**Large / ceremony-bound**
11. **Storm-identity ALIAS TABLE** (docs/DATA_ARCHITECTURE.md) — the seam
    bit this session repeatedly (see §3). Retires the whole bug class.
    Then a-decks (real cone spread) and IOC sea-level stations (global
    observed surge).
12. ~~**WP recalibration Tranche B**~~ — **SHIPPED 2026-07-12** (commits
    3392c42 + cf38b6e; docs/audits/WP_DPS_AUDIT_V2.md §9). ~140 WP coastline
    waypoints (core/land_proximity, wp_* keys, longitude-gated 95–150E) drive
    living-legs surge/econ profiles + a landfall-intensity bonus; the
    compensating layer (1.10 mult, RI, sub-basin table) is stripped, T→80,
    econ ×0.85, scoring rainfall ref 500→1000mm. Anchors met (Haiyan 95.2 #1,
    Yagi 85.4, Kong-Rey 60.1, Kalmaegi 62.2, Surigae 34.2); 220/223 non-WP dps
    bit-identical (3 Atlantic movers <0.12); ρ(damage) +0.461, ρ(deaths)
    +0.489. R5 exposure-integrator gap remains (PH majors score power over
    realization; Doksuri/Gaemi under-scored). **Next-bake gotcha found the
    hard way:** a scoring change must ALSO bump api/routes.py
    _DPS_CACHE_VERSION or previously-warmed live storms serve old-code scores
    from the persistent /dps volume cache (rebake.py now prints the reminder).
13. Also open, not urgent: `/value` surge null for JTWC storms; dormant-code
    candidates are DONE (excised); Apple submission still paused.
14. ~~**Current-season sidebar scores**~~ — **SHIPPED 2026-07-12** (commit
    b6edbdd). Current-season storms fell in the gap between baked (annual
    bundle) and currently-active (the only set the DPS loop warmed): the
    hamburger showed a bare Saffir-Simpson category for ingested rows and a
    crude wind estimate for IBTrACS rows (Sinlaku 36 vs canonical 73).
    `warm_current_season_dps()` now full-engine-warms every current-year
    catalog storm at startup + hourly and regenerates the harmonized
    default-view; `harmonize_catalog` collapses SID/ATCF twins by
    (name,year,basin) when the alias table lags; `_without_stale_custom`
    purges demo rows once custom_storms.csv no longer declares them. Live: 47
    → 36 rows, all canonical, no twins, no fakes; selfcheck 208/0.
17. **North Indian DPS** — SHIPPED 2026-07-12 (commit 2fe6a29;
    docs/audits/NI_DPS_AUDIT.md). The last dead-legs basin, in the deadliest
    surge ocean on Earth. ~52 ni_* waypoints + 10 coastal/economic profiles
    (Bay of Bengal deltas carry the highest surge_amplification 1.75 +
    concavity 0.42 in the system) + LFI, gated lat 0-31/lon 42-97, checked
    BEFORE WP so the 95-97E Andaman/Myanmar overlap resolves to NI. Live:
    Mocha 83, Fani 86, Nargis 76, Tauktae 65, Amphan 60; ρ(damage) 0.63→0.77.
    **0 movers** (no baked NI storm; no WP storm in the overlap) — bundle
    bit-identical, NO rebake needed; _DPS_CACHE_VERSION → v16. Landfall panel
    now covers NI. **All five basins now have living legs** (Atlantic native,
    EP + WP + SH + NI activated). Known: hazard-vs-realized (NI deaths low from
    evacuation → surge potential scored, evacuation success is ERS/IAS); Ockhi
    stays low (at-sea marine disaster, not a landfall).
16. **Experimental Features page + forecast rain-hazard (C1)** — SHIPPED
    2026-07-12 (commits f908117 + 6b42ad3; docs/RAINFALL_SCORE_OPTIONS.md).
    New /experimental page (linked from About, noindex) lays out 3 options for
    a forecast rainfall-hazard score (rain = leading TC killer, DPS's R5 gap).
    core/rain_forecast.py computes a live 0-100 score from the forecast track
    (point-max residence model, forward-speed-dominated: Michael 28 vs Harvey
    100 — rain axis independent of wind). /forecast now returns `rain_forecast`
    (fail-open) + logs each advisory to a per-storm JSONL for post-storm
    grading. **Does NOT touch DPS/bundle** — experimental, shown alongside.
    Kinematic v1 placeholder; next: wake SurgeDPS's dormant WPC-QPF + Atlas-14
    + NWM/HAND path (Options B2/B3) to serve a /rain_hazard endpoint, then the
    C3 "Rain Hazard" chip. SurgeDPS already has the fetchers (mrms/nwm/atlas14/
    ahps) + a building-level compound-flood model, currently dormant.
15. ~~**Southern Hemisphere DPS**~~ — **SHIPPED 2026-07-12** (commit 807e8c2;
    docs/audits/SH_DPS_AUDIT.md). SI/SP storms scored open-ocean (Winston 48,
    Idai 31, Cat-5-empty-coast Ilsa 42 above everything). Added ~90 sh_*
    waypoints + 12 coastal/economic profiles + landfall-intensity bonus,
    southern-latitude gated (lat<0). Live: Winston 90, Yasi 74, Idai 60,
    Ilsa 42, Kenneth 38; **0 non-SH movers** (218 baked bit-identical), 5
    baked SH storms rebaked; _DPS_CACHE_VERSION → v15-sh-legs. Known gap:
    Gabrielle (ET, track ends 286 km from NZ) — same rainfall/ET realization
    limitation as WP's Doksuri.

## 3. Known seams the new features deliberately work around

(All symptoms of the missing alias table — fix #11 retires these.)
- `/dps` returns the raw ID as `name` for ATCF-id lookups of catalog storms
  (Katrina AL122005 → name "AL122005"); the SID-keyed entry carries the name.
- The catalog keys most storms by IBTrACS SID; ATCF ids resolve via /dps
  compute instead. /compare + analogs compensate (pickedNames fallback
  chain, same-name+year twin exclusion so a storm is never its own analog).
- Raw `/track` has NO per-point dps/ias/ers — the SPA computes those
  client-side; /compare deliberately omits those rows.
- ATCF-vs-SID lookups of the same storm can differ ~1 DPS point (different
  cached computes) — cosmetic, known.
- **Bundle vs live-/dps divergence widened slightly under Tranche B.** The
  baked bundle scores WP storms from the curated preload_bundle snapshot;
  `/dps` recomputes from the live IBTrACS/b-deck track, which isn't
  bit-identical. The new engine is more track-sensitive (LFI, waypoint
  profile assignment), so a baked WP storm's /dps live-recompute can sit a
  few points off its bundle value (Gaemi 62.4 baked / 71.1 live). Every
  user-facing surface for a PRESET storm reads the bundle (bundle_index +
  bundle_storm), so this is invisible on historical pages; it only shows on
  direct /dps API calls. Real fix = feed /dps the same snapshot source as the
  bake (or bake from the live track).

## 4. Document map (read before re-deriving anything)

- `docs/PERF_AUDIT_2026-07.md` — measured perf state, Railway-volume vs
  Cloudflare analysis, ranked roadmap (bundle diet spec lives here).
- `docs/RETIRED_MAP_OVERLAYS.md` — verbatim archive + restoration guide for
  every retired overlay (wind-particle/pressure/precip, panel UI, toggles).
- `docs/DATA_ARCHITECTURE.md` — canonical input→cache→consumer→surface map;
  alias-table design.
- `docs/audits/WP_DPS_AUDIT_V2.md` — WP formula audit; Tranche B plan.
- Memory files: stormdps-track-data-loading (loading/caching + layer
  restructure gotchas), dps-formula-gaps (scoring), stormdps-no-directives
  (copy policy), stormdps-analyst-mode, surgedps-recovery.

## 5. Hard-won engineering gotchas from this session

- **Route re-entry sentinel trap**: calling a FastAPI handler directly leaves
  truthy `Query(...)` objects as params — pass EVERY Query-default explicitly
  (see `_swr_refresh_track`, `_warm_one_dps`, analogs endpoint).
- **The hourly DPS warm loop is the live-DPS freshness driver** — anything
  that lets it read a stale track (like SWR did before the bypass) silently
  lags every live score by a cycle.
- **Leaflet must initialize in a VISIBLE container** (display:none → 0×0 →
  zoom-0 world map; Chart.js self-heals, Leaflet doesn't).
- **Toggle-style auto-enables need `!enabled` guards** — a double renderMap
  otherwise toggles the layer off, and there's no button to recover.
- **Auto-enabled layers must not be torn down by satellite re-inits** (the
  old teardown killed the windfield 50 ms after enable).
- **`_isActiveStorm` is racy on slow direct-URL historic loads** — gate
  satellite on track freshness (<12 d), never the flag alone.
- **A mesh spun via `rotation.z` under a −π/2 x-rotation must spin `.y` once
  it lives in world space** (the 3D canopy capsized until reviewed).
- **Cloudflare only edge-caches by extension unless a Cache Rule exists**,
  and rule expressions: URI Full includes the scheme — use URI Path.
- **Keep using the `code-review` skill before every push** — it found ~8
  genuine blockers across this session's ten pushes.

## 6. Workflow must-knows (unchanged, verified again this session)

- **NTFS mount truncates large files silently.** Native PowerShell git for
  commits/pushes; never read a >50 KB repo file through the bash mount and
  push it. `index.html` and `api/routes.py` are the usual victims.
- **No system python on PATH for repo work** — embedded interpreter at
  `D:\Install ComyUI Here\ComfyUI_windows_portable_nvidia\ComfyUI_windows_portable\python_embeded\python.exe`
  (3.13, has httpx; ignores PYTHONPATH — inject sys.path). NB the machine
  also has user-level Python 3.13 via the `py` launcher (used for the
  SurgeDPS venv), but the bare `python` command is the WindowsApps stub.
- **Skills:** `code-review` (before pushing scoring/routes/index.html),
  `compile-cache-bake` (+ don't bake while a <7-day WP/EP no-landfall storm
  is live), `deploy-verify`, `github-safe-push`, `basin-dps-audit`.
- **Golden-master lock:** `tests/test_scoring_baseline.py`; approved scoring
  changes require `python tests/gen_scoring_baseline.py` after the bake.
- **Verify claims live** (API responses / browser / console), never assume.
  Local pre-deploy verification pattern that worked all session: a scratchpad
  stdlib proxy on 127.0.0.1:8000 serving the working-tree frontend with
  /api/v1/* forwarded to production (index.html hardcodes that port for
  localhost API_BASE); test in the preview browser at desktop + 375 px.
- **Homepage HTML is Cf-Cache-Status DYNAMIC** — index.html changes reach
  users without a purge. `/frontend/*` is now edge-cached via Cache Rule
  (versioned bundle URL → still purge-free).

## 7. Related repos

- **SurgeDPS** recovered 2026-07-09 to `C:\Users\Ryan\APPS\SurgeDPS-recovered`
  after a Windows reset wiped the local folder (clone of
  github.com/oldreaverg-lang/SurgeDPS; last commit 2026-05-23; venv rebuilt;
  `.env` must be refilled from Railway; Node not installed; xlsx/docx were
  gitignored and are unrecoverable from git). Read ITS `HANDOFF.md` before
  working there. Lesson standing for all repos: push early, even WIP.
