# StormDPS — session handoff (updated 2026-07-09)

For a fresh session (or another engineer) picking up cleanly. Authoritative
context lives in `CLAUDE.md` (deploy rules, NTFS-mount hazards, project
skills) and the operator's memory files — read those first. This doc is a
point-in-time snapshot, not a spec; verify against current code before acting.

**Live site:** https://stormdps.com · **GitHub:** oldreaverg-lang/StormDPS ·
push to `main` → Railway auto-deploy (~30 s–4 min). Repo:
`C:\Users\Ryan\APPS\StormDPS-recovered`.

**Deploy state:** local HEAD == origin/main == `b148245`, deployed and
verified live. Working tree clean except this file and
`scratch/resolution_3h_test.py` (untracked leftover, safe to delete).
BAVI (WP092026) has been the live test storm all week.

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
1. **Landfall panel** (public "when and how hard" card): landfall window +
   forecast DPS at landfall + ERS zone, from the existing forecast/cone/zone
   machinery. Small; ship while a storm is threatening land. Copy must stay
   descriptive (see stormdps-no-directives memory).
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
6. **Bundle diet** — compiled_bundle.json is 3.56 MB raw, parsed on every
   first visit. Split: slim eager index (~40 KB: id/name/year/dps/label/
   category) + per-storm detail on demand. Requires bake + sw + BUNDLE_VERSION
   ceremony. Biggest mobile win left (see PERF_AUDIT §4.5).
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
12. **WP recalibration Tranche B** (docs/audits/WP_DPS_AUDIT_V2.md §7 +
    scratch/wp_recal_harness.py): activates WP surge/econ profiles, strips
    the compensating adjustment layer; ALSO owed: the scoring-side rainfall
    reference recalibration (500 mm→~1000 mm, display side already done).
    Full bake + golden-master refreeze ceremony; don't bake while a <7-day
    WP/EP no-landfall storm is live.
13. Also open, not urgent: `/value` surge null for JTWC storms; dormant-code
    candidates are DONE (excised); Apple submission still paused.

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
