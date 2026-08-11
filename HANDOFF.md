# StormDPS — session handoff (updated 2026-08-10)

**Deploy state:** local HEAD == origin/main == `ef4f580`. Site healthy,
`/health/selfcheck` GREEN (first time in 8 days), CI green, 186 tests pass.

---

## 2026-08-10 — memory root cause, CI repair, and a two-site security pass

Read this section before touching memory, CI, or SurgeDPS.

### The memory incident — root cause FOUND and fixed, but NOT fully closed

**What it was.** Every 6 h (`_GLOBAL_IBTRACS_CATALOG_TTL`) the global catalog
refreshes from the 315.6 MB IBTrACS archive. The code did
`cached.read_text()` then `csv.DictReader(io.StringIO(csv_text))`. StringIO
widens ASCII to UCS-4 at **exactly 4.00 bytes/char** (measured across three
sizes), so ONE parse held **~1,578 MB** — 316 MB str + 1,262 MB StringIO. The
observed 8,586 MB peak is 5.4 of those overlapping. This is what exhausted the
Railway credits on 07-31 and took the site down.

**Fix (`3ab930a`):** `get_ibtracs_catalog` now streams. `_ibtracs_path()`
returns the cached file WITHOUT reading it; `_parse_ibtracs_catalog_file()`
feeds `csv.DictReader` straight off the handle. The parse body moved unchanged
into `_parse_ibtracs_catalog_rows()`, shared by both paths; the text wrapper
stays for the other 11 `_fetch_ibtracs` callers. Download deliberately
untouched — the cache file has no TTL, so it is fetched once; the recurring
cost was the re-read.

**Verified by EQUIVALENCE, which is the check that matters** (a silent
disagreement would corrupt the storm catalog): full range 1851-2099 (1,129
storms), recent, and a historic slice all produce IDENTICAL output, peak
29.6 -> 0.9 MB (31.6x) on a synthetic IBTrACS-shaped fixture including the
real archive's quirks (units row under the header, unnamed storms, quoted
commas, blank numerics). In production the catalog is byte-identical:
**998 entries, id-set sha1 `803beb98975d2834`, before and after.**

**Mitigations (`ef4f580`)**, none dependent on the remaining unknown:
- `MALLOC_MMAP_THRESHOLD_=131072` + `MALLOC_TRIM_THRESHOLD_=131072`.
  **`MALLOC_ARENA_MAX=2` (added 08-01) was the WRONG KNOB** — it caps arena
  count, not whether memory returns to the OS. glibc's mmap threshold is
  DYNAMIC: it ratchets up to each freed mmap'd block's size (32 MB cap),
  permanently, after which sub-32 MB allocations come from the brk heap and
  only return if they sit at the top. That is the measured signature: >32 MB
  blocks released cleanly (a 3,458 MB drop in one 10 s window) while the floor
  climbed for 8 days. Setting the threshold explicitly DISABLES the dynamic
  adjustment. Both values are glibc's own defaults — pinned, not tuned.
- `--max-requests 5000 --max-requests-jitter 500`. The worker previously ran
  for the life of the deploy (~8 days, 8.6 GB peak). Safe because `--preload`
  is on: a recycle is a fork, not a cold boot. The hazard that a recycle drops
  the in-memory catalog timestamp and re-triggers the refresh is neutralised
  by the streaming parse landing first.
- `_IKE_EXECUTOR` pinned to `max_workers=6` (was `min(32, cpu_count*2)`).
  Thread stacks are charged to RSS once touched, and `cpu_count()` reports the
  HOST's cores inside a container. 6 covers both submitters; the IKE batch is
  already capped at 4 by an `asyncio.Semaphore` at the call site.

**⚠️ STILL UNEXPLAINED: ~2.5 GB.** A process that booted at 95 MB reached a
2,629 MB peak. RULED OUT WITH EVIDENCE, do not re-chase:
- **NumPy** — production never executes a grid-based IKE computation. All
  track points route through `core/ike_coaps.py`, which imports no NumPy
  (census: 1,676 points across 23 storms, ZERO grid-path). 28,200 real
  computes moved RSS **0.05 MB**. The grid code is unreachable (one branch
  needs an empty dict `{}` no producer can emit).
- **Unbounded Python caches** — 171,280 objects total, largest container
  101 KB. The whole object graph is ~50-100 MB.
- **Thread leak**, **`--preload`**.
- **`_search_ibtracs_by_atcf_id` full-archive load** (routes.py:2453-2456) —
  plausible on paper, REFUTED by measurement: an uncached track request swings
  only 120 MB and releases it. NB my first test of this was INVALID (a
  high-water mark cannot detect an allocation below it); the valid test samples
  RSS *during* the request.

**How to judge whether this is actually fixed:** a low reading right after a
deploy PROVES NOTHING — that mistake was made on 08-01 (reported 106 MB,
declared an 85% cost cut, wrong). Watch the FLOOR over days. Settles in the
hundreds of MB = holding. Climbs toward GB = the unexplained allocator is
still live.

### The alarm was right and was overruled

`/health/selfcheck` returned 503 continuously for 8 days and the healthcheck
cron failed 31 consecutive runs — on a REAL condition (`resident memory
2050 MB over 1500 MB`), with the other six probes green. **The operator saw
those emails and asked about them, and was told it was nothing to worry about
and could be kicked down the road. That answer was wrong.** Do not dismiss
this alarm; it is the only thing standing between a memory regression and
another credits-exhaustion outage. The alarm still needs a
transient-vs-sustained split (spikes are normal; a risen floor is not) before
it is fully actionable — that work is NOT done.

### CI: red for 8 days, twice, both self-inflicted

`requirements-dev.txt` installs ONLY what the suite reaches. It went red
07-10 (numpy, fixed 08-02 by `a1cd11c`), held green for exactly TWO commits,
then `ecf399b` (SHIPS RI outlook) added `tests/test_ships_ri.py` ->
`services/ships_client.py` -> `import httpx`, which was not in the dev deps.
Red again for 8 days. Fixed by `b5b3e55`.

**RULE, now written into requirements-dev.txt:** before adding a test that
imports from `services/` or `api/`, run it in a CLEAN VENV:
```
python -m venv /tmp/ci && /tmp/ci/bin/pip install -r requirements-dev.txt
/tmp/ci/bin/python -m pytest -q
```
A pass in your normal environment proves nothing — that is exactly how both
regressions shipped. Also note `ci.yml` is compile-and-import only and never
runs pytest, so a green "CI" check is NOT a test signal.

### Open on StormDPS (found in the 08-10 audit, none fixed)

- **Two storms both named "Dolphin."** `/storms/active` returns WP142026 as
  `DOLPHIN`; its own SSR page, the catalog, and its `/dps` all say `Chan-Hom`.
  WP122026 is separately titled Dolphin. The wrong name ships in the homepage
  SSR payload. The active feed is the wrong side.
- **WP152026 (and any brand-new storm) has no real storm page and no OG card**
  — both serve the not-found fallback shell while the storm is live in the
  feed. Its track is also forecast-only (`track_source: jtwc`, 7 of 9 points
  future-dated) because it has no b-deck yet; correctly TAGGED but nothing in
  the UI surfaces the tag.
- **Unknown storm ids take 11-35 s to 404**, with no negative cache.
- Sitemap contains zero 2026-season storms despite them having real SSR pages.

### SurgeDPS — 4 commits, 2 of them security (separate repo)

See `C:\Users\Ryan\APPS\SurgeDPS-recovered\HANDOFF.md` for detail. Summary:
`/api/cell` unauthenticated DoS bounded (`7f20cd1`), `/api/gauges` path
traversal closed (`3e23ee6`), `?refresh` gated behind VALIDATION_TOKEN at all
six endpoints + flood-zone cache byte-capped + orphaned vulnerable
`api_server_fastapi.py` deleted (`236aa18`), flood-zone miss wall + a real LRU
(`64566b4`). Also: **`/api/flood_zones` is NOT broken, it is UNSEEDED** —
FEMA's WAF blocks Railway's egress at the TLS handshake, which is why
`scripts/seed_flood_zones_local.py` exists; run it from a machine that can
reach FEMA.

### Process note

The account **spend limit was reached** mid-session, which killed 10 of 13
investigation agents and BOTH memory commits shipped without adversarial
review. Every other change today was reviewed and review caught a real defect
in most of them — including two of mine in this session's own work (an
antimeridian break that would have 400'd legitimate CPHC cells, and a
"byte-capped LRU" that was actually FIFO and gave the hot set no protection).
Treat `3ab930a` and `ef4f580` as provisional until reviewed or proven by
several days of live data.

---

## 2026-07-31 — cost trim (MERGED to main 2026-08-01 as `c9ea77a`, verified live)

Railway ran
out of credits 07-31 (site DOWN, "Application not found" at the Railway edge;
operator let it wait for the 08-01 cycle reset — no storms threatening).
Cost audit found memory = ~93% of the $23.69 bill (~2.2 GB avg resident,
9 GB spikes; single gunicorn worker already). All of the following is now ON
MAIN and live — the "staged / verify once the service is back" wording that
used to head this section was stale from 08-01 to 08-10:
- **SATELLITE MAP LAYER RETIRED** (operator call: "a little buggy and isn't
  worth the space"): 438 lines excised from index.html — state/init/teardown/
  setSatelliteFrame (incl. the 07-22 ghost guard + IR archive fallback),
  syncOverlaysToTimestampMs, renderMap auto-enable, anim-loop sync, syncedSatTs
  span. Archived VERBATIM in docs/RETIRED_MAP_OVERLAYS.md ("Satellite imagery
  layer (retired 2026-07-22)" section). Wind layers were ALREADY driven
  directly by the storm timeline (setWindfieldFrameByIdx/setWindDirFrameByIdx
  in syncVisualization) so nothing rewires. **Backend api/satellite_routes.py
  stays LIVE — the /methodology 3D showcase consumes it.** Both inline scripts
  pass node --check; NOT yet browser-verified (site was down).
- **/health/memory** (main.py): /proc RSS+HWM, known module-cache sizes
  (routes/seo/og_card attr names verified), ?deep=true heap census. Use it to
  find the ~2 GB resident hog, THEN decide the next diet step (stream-parse
  IBTrACS etc. deferred until the probe names the holder).
- **MALLOC_ARENA_MAX=2** (Dockerfile) — glibc arena retention was likely
  converting the 9 GB IBTrACS/bake spikes into paid baseline.
  **⚠️ This hypothesis was WRONG and the knob did not hold the floor down.**
  See the 2026-08-10 section: arena count is not the retention mechanism; the
  dynamic mmap threshold is. `MALLOC_MMAP_THRESHOLD_` was added 08-10.
  `MALLOC_ARENA_MAX=2` was kept (harmless, mildly useful) but it is not the
  fix and should not be cited as one.
- SurgeDPS project ($6.14): volume prune + backup retention are OPERATOR
  dashboard actions; sleep mode explicitly declined for now.
Merge protocol: merge cost-trim → main AFTER the service resumes, then
deploy-verify (storm loads, animation plays, no /satellite fetches from
index.html, /methodology 3D still works, /health/memory returns data).
**DONE 2026-08-01** (`c9ea77a`), all five checks passed. Caveat recorded for
posterity: the 08-01 verification read `/health/memory` immediately after the
deploy, saw 106 MB, and concluded an ~85% cost cut. That was a fresh fork
reading its own boot footprint — it proved nothing, and the floor was back
over 2 GB within days. Judge memory by the FLOOR over days, never by a
post-deploy sample.

**FEATURE — RI Outlook via SHIPS-RII: BACKEND SHIPPED `ecf399b`, NO UI YET.**
`services/ships_client.py` + 9 offline tests in `tests/test_ships_ri.py`;
`ri_outlook` is attached in `get_storm_forecast` and is live in the API
response right now. Nothing in `frontend/index.html` renders it — the chip and
the GRIP-blended band edge described below are still unbuilt. Original spec,
operator-approved 2026-07-31:
inspired by Rozoff et al. 2026 (WAF-D-25-0076, ensemble RI prediction; AMS
full text blocked from this environment — method from press + the team's
EnsGRIP precursor deck; GRIP blend: V=(1-P_RI)·V_fcst + P_RI·V_upper).
Source VERIFIED LIVE: https://ftp.nhc.noaa.gov/atcf/stext/
{YYMMDDHH}{BASIN##YY}_ships.txt — 6-hourly ~9 KB text per storm, free.
Contains: (a) SEVEN RI probability lines, rigid format
`SHIPS Prob RI for 30kt/ 24hr RI threshold=   11% is  1.3 times
climatological mean ( 8.6%)` (thresholds 20kt/12h…65kt/72h);
(b) `POT = MPI-VMAX (KT) :  63.6` → MPI = VMAX+POT, the GRIP upper bound
for free (no SST-MPI computation needed). Plan: tiny ingest per active
AL/EP storm per advisory → "RI Outlook" chip on the forecast panel +
GRIP-blended upper edge for the Forecast DPS Band. Live test storm at
verification time: Genevieve EP072026. JTWC basins have no SHIPS text —
AL/EP only, fail-open.

**What shipped 2026-07-22 (Bertha landfall day — sat-layer fix + bug-hunt):**
- **Satellite ghost guard + IR archive fallback** (`e11debb`): operator saw
  "imagery ~100 mi north of track" — NOT a georef bug (IR centroid vs live
  NHC position measured ~20 mi for both storms). The double-buffered frame
  swap held the previous frame on screen across big time jumps (first paint
  snaps newest→track start on EVERY load), and GIBS GeoColor's archive is
  only ~2 days deep (older visible tiles = transparent sentinels; Band13 IR
  runs weeks) so pre-archive frames held stale imagery forever. Now: >3 h
  displayed-vs-target gap hides the stale frame during the swap (NB Leaflet
  setOpacity mutates options.opacity — back-buffer clones force base), and
  an all-sentinel visible frame older than 24 h learns the archive edge
  (`_irArchiveCutoffMs`) and re-renders as IR, labeled "IR (archive)".
- **Bug-hunt fixes** (`1540950` + `464a07d`): (1) `frontend/logo-512.png`
  NEVER EXISTED yet was the og:image/twitter:image/JSON-LD target on every
  page + the OG-card fallback — all shares 404'd their image; asset created.
  (2) `/og/storm/{id}.png` now renders LIVE storms (catalog-fallback dict
  enriched with `dps` from the hourly-warmed live DPS cache; 1 h cache
  headers; NB skips `_dps_cache_needs_rearm`, so a dead fish-storm's card
  can briefly over-score — self-heals on next /dps view). (3) fetchHistory
  debounce hardened (`_fetchInFlight` could wedge search until reload if
  setup threw pre-try). (4) `_isActiveStorm` now DERIVED from
  `window._activeStorms` at fetchHistory entry + 12-day freshness backstop
  post-load (was: set by loadActiveStorm, cleared only by presets — search
  left it stale-true). (5) **Live-name short-circuit**: typing an active
  storm's NAME loaded the newest CATALOG namesake (Fausto 2020 while
  Hurricane Fausto 2026 was live!) because backend bare-name resolution
  prefers the catalog; the client now maps active names/ids to the live
  ATCF id up front — explicit year opts out.
- QA traps for this environment (browser pane): `document.hidden=true` →
  rAF never fires → Leaflet vector paths all "M0 0" AND the hero DPS
  count-up shows "-"; read_console_messages duplicates entries ×2-4 —
  resource timing is ground truth; a hard-driven tab wedges on the
  fetchHistory debounce for up to 90 s (fetchWithTimeout) — retest in a
  fresh tab before calling anything broken.

For a fresh session (or another engineer) picking up cleanly. Authoritative
context lives in `CLAUDE.md` (deploy rules, NTFS-mount hazards, project
skills) and the operator's memory files — read those first. This doc is a
point-in-time snapshot, not a spec; verify against current code before acting.

**Live site:** https://stormdps.com · **GitHub:** oldreaverg-lang/StormDPS ·
push to `main` → Railway auto-deploy (~30 s–4 min). Repo:
`C:\Users\Ryan\APPS\StormDPS-recovered`.

**Deploy state:** local HEAD == origin/main == `254c3d3`. Selfcheck green
(all 6 probes; "2 active" — Bertha AL022026 + Fausto EP062026).

**What shipped this session (2026-07-21):**
- **NHC forecast pipeline resurrected** (`2dabe6b`). NHC RETIRED the
  `gis/forecast/archive/{id}_5day_latest.json` / `_5day_pgn_latest.json`
  GeoJSON convention — hard 404 for EVERY storm — so `/storms/{id}/forecast`
  had been silently empty for ALL AL/EP storms (the route swallows upstream
  failures; JTWC basins were fine on their own bulletin-synth path, which is
  why BAVI had a cone all July while Bertha had none). Cone, forecast line,
  landfall panel, stall risk, forecast DPS band and forecast ERS were all
  dead for NHC storms. `get_forecast_track` now reads what CurrentStorms.json
  actually advertises per storm: the **TCM forecast advisory text**
  (`forecastAdvisory.url`, regex parse via `_parse_tcm_forecast` — live
  format says "GUSTS TO"; DD/HHMMZ month rollover handled closest-month in
  `_tcm_datetime`) and the **official cone KMZ** (`trackCone.kmzFile`,
  stdlib zip+ElementTree in `_parse_cone_kmz`, no new deps). The route falls
  back to `_synthesize_cone` if the KMZ leg fails. `get_storm_snapshot` also
  stopped burning a guaranteed-404 round-trip on the retired
  `*_fcst_latest.json` (it always fell through to CurrentStorms anyway; one
  call site has an 8-s timeout budget). Semantics: forecast `hour` is now
  advisory-relative (0/9/21…) not synoptic TAU — all consumers interpolate,
  reviewed fine; `time` is a display-only label ("Tue Jul 22 00:00Z").
  Verified live: Bertha 7 taus + 1430-vertex official cone, Fausto 9 + 2154;
  landfall panel "Mississippi River Delta, LA ~+18 h", stall risk moderate
  (4.3 kt min), sidebar "SLOW" chip — all lit by this fix. 22
  forecast/baseline tests green; fresh-eyes review: no blockers.
- **Bertha (TD2→TS) full pipeline audit — every leg verified on the 3-h
  cadence**: active feed, b-deck track (35 pts, every gap exactly 3.0 h —
  densification working), SWR freshness (stale-serve → background revalidate
  observed live), DPS 22.73 @ v16 with IMERG 10.14 in, SST 20/35 valid
  (newest 3 null = normal ERDDAP 1–2 d lag), rainfall 6-h accumulations,
  observed layer 3 stations, catalog parity (sidebar one hourly warm-cycle
  behind — expected).
- **Edge-cache pass** (`254c3d3` + operator dashboard rule).
  `/storms/catalog/custom` sent NO Cache-Control → Cloudflare BYPASSed it
  for every visitor; now mirrors catalog (300/900) and serves HITs.
  `/storms/active` sends 60/60. Operator deployed a Cache Rule: URI Path
  `/` OR `/index.html` OR `/api/v1/storms/active` → eligible, "use
  cache-control header if present, bypass if not". Measured: homepage TTFB
  270 ms → ~75 ms (edge HIT); active HITs. `/index.html` itself stays
  BYPASS (that path's handler sends no Cache-Control — cosmetic, nobody
  visits it). **PROCESS CHANGE: every index.html deploy MUST now "Purge
  Custom URLs" for `/` + `/index.html`** — the old "homepage is DYNAMIC,
  purge-free" fact is DEAD (§6 updated).
- **Two scare-findings DEBUNKED — don't re-chase**: (1) "the page inits
  ×4" is FALSE — the preview-pane console tool duplicates log entries;
  `performance.getEntriesByType('resource')` proves every boot fetch fires
  exactly once. (2) frontend `BUNDLE_VERSION=15` vs `_DPS_CACHE_VERSION`
  `v16-ni-legs` is NOT drift — unrelated counters; live bundle_index
  verified byte-identical to local.
- **Toolchain notes**: `.env` GITHUB_TOKEN is STALE (gh_push.py → 401 Bad
  credentials); native PowerShell git worked (secrets hook ran, push exit
  code checked explicitly). Embedded python has NO fastapi — anything
  importing api/routes must use `py` (3.13.14, fastapi 0.137.2); embedded
  remains fine for offline/core tests.
- **SST/observed layer-cache SWR** (`5354ef1`, follow-up DONE same day):
  a TTL-expired (90-min) same-fingerprint cache entry is now stale-served
  instantly with a singleflight background revalidate
  (`_kick_layer_revalidate`, modeled on the live-track SWR; 24-h stale
  ceiling → inline fetch). Fetch legs extracted to `_fetch_sst_track` /
  `_fetch_observed_track` (shared by route + revalidate task); write
  gates unchanged; historical storms untouched. NB a NEW advisory changes
  the fingerprint → that first view is still a true cold fetch (SWR can't
  bridge different fingerprints — arrays are index-aligned to the track).
- Remaining follow-ups from the audit: carto z5 basemap 503 burst on cold
  load (third-party rate limit); SPA split remains the storm-present TBT
  ceiling (§2).

**What shipped 2026-07-12 — see §2 items 12–17 for detail:**
- **All five basins now have "living legs."** Atlantic was always native; this
  session activated **WP** (Tranche B), **EP** (was already done), **SH**, and
  **NI** — each with real `*_` coastal + economic profiles in
  `core/storm_surge` + `core/economic_vulnerability`, driven by curated
  coastline waypoints in `core/land_proximity` (wp_/sh_/ni_ keys), a
  landfall-intensity (LFI) bonus, and a no-landfall dampener. Every basin is
  **geo-gated** (WP lon 95–150E; SH lat<0; NI lat 0–31/lon 42–97, checked
  BEFORE WP) so activating one **cannot** change another's baked scores —
  proven each time by a rebake A/B (0 non-target movers). Audits:
  `docs/audits/{WP_DPS_AUDIT_V2 §9, SH_DPS_AUDIT, NI_DPS_AUDIT}.md`.
- **Current-season sidebar scoring fixed** — `warm_current_season_dps()` warms
  every current-year catalog storm (not just active) so the hamburger shows
  canonical scores, twins dedup by (name,year,basin), demo rows purged.
- **Experimental Features page** (`/experimental`, off About) with a live
  forecast **rain-hazard** prototype (`core/rain_forecast.py`) — separate from
  DPS. `docs/RAINFALL_SCORE_OPTIONS.md`.
- **`_DPS_CACHE_VERSION` is now `v16-ni-legs`.** CRITICAL RULE (learned the
  hard way): any scoring-formula change MUST bump it or previously-warmed live
  storms serve stale scores from the persistent /dps cache. rebake.py prints a
  reminder.

**Older context (still true, historical):** BAVI (WP092026) was the live test
storm the week of 2026-07-04→11 (landfalled Zhejiang ~07-11). PSI mobile perf
rounds shipped 47→55→96 (storm-free); KNOWN CEILING: storm-present TBT (~2–3 s
inline-script eval) needs the SPA split (backlog). SurgeDPS Lighthouse/parity
fixes; the alias table + recorded-damage + cross-surface harmonize all shipped
2026-07-10.

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

## 1. Where the product now stands (architecture overview — see the top block for the latest 2026-07-21 state)

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
2. ~~**Cloudflare Cache Rule #2**~~ **DONE 2026-07-21** — catalog was
   already edge-HIT; `catalog/custom` fixed via origin headers (`254c3d3`);
   `/storms/active` + homepage covered by the operator's new Cache Rule
   (see top block; purge caveat in §6).
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
- **Preview-pane QA traps (2026-07-21)**: the in-app browser loads pages
  with `document.hidden=true` → rAF never fires → Leaflet leaves EVERY svg
  path as "M0 0", so vector layers LOOK dead but aren't — verify layer
  creation + API payloads, not pixels. And `read_console_messages`
  DUPLICATES log entries (×2–4) — `performance.getEntriesByType('resource')`
  is the ground truth for "how many times did X fetch".

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
- **Homepage HTML is now EDGE-CACHED** (Cache Rule 2026-07-21: `/`,
  `/index.html`, `/api/v1/storms/active`; TTL from the origin's
  s-maxage=900/60). **index.html deploys MUST purge `/` + `/index.html`**
  (Caching → Purge Custom URLs) or users see the old page for up to 15 min.
  `/frontend/*` stays edge-cached via its own rule (versioned bundle URL →
  still purge-free). The homepage embeds an active-storms JSON snapshot
  server-side; a worst-case 15-min-stale first paint self-corrects because
  the client refetches /storms/active on boot.

## 7. Related repos

- **SurgeDPS** recovered 2026-07-09 to `C:\Users\Ryan\APPS\SurgeDPS-recovered`
  after a Windows reset wiped the local folder (clone of
  github.com/oldreaverg-lang/SurgeDPS; last commit 2026-05-23; venv rebuilt;
  `.env` must be refilled from Railway; Node not installed; xlsx/docx were
  gitignored and are unrecoverable from git). Read ITS `HANDOFF.md` before
  working there. Lesson standing for all repos: push early, even WIP.
