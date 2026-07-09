# Performance audit — 2026-07-08

Live measurements against production (httpx, brotli/gzip accepted, single
region; sizes are decompressed except where noted). Companion change shipped
with this doc: **stale-while-revalidate for live-storm tracks** in
`api/routes.py` (see §3).

## 1. Measured state

| Surface | Size | Encoding | cf-cache-status | Cache-Control | Latency |
|---|---|---|---|---|---|
| homepage HTML | ~103 KB (br wire) | br | **DYNAMIC** | max-age=300, s-maxage=900 | 170–240 ms |
| compiled_bundle.json?v=11 | 3.56 MB (raw) | gzip | **DYNAMIC** | max-age=1y, immutable | 330 ms |
| storms/catalog | 149 KB | gzip | **DYNAMIC** | max-age=300, s-maxage=900 | 150 ms |
| sw.js | 5.8 KB | gzip | MISS | no-cache ✓ (override fixed) | 164 ms |
| font woff2 | 26 KB | — | **HIT** | max-age=30d, immutable | **20 ms** |
| satellite tile | — | — | MISS → **HIT** | max-age=3600 | 137 → **18 ms** |
| /storms/active | 0.2 KB | gzip | DYNAMIC | **none** | 130 ms |
| /dps (live) | 4.3 KB | gzip | DYNAMIC | **none** | 133 ms |
| /track (live) | 35 KB | gzip | DYNAMIC | **none** | **13,254 ms** cold / 140 ms warm |
| satellite frames/auto | 1 KB | gzip | DYNAMIC | **none** | 138 ms |

Two structural facts fall out:

1. **Cloudflare only edge-caches by file extension unless a Cache Rule says
   otherwise.** `.woff2` and `.png` are on the default list (hence the 18–20 ms
   HITs); `.json` and HTML are not — so the two largest payloads on the site
   (bundle, catalog) and every API response ignore their own `s-maxage` and hit
   Railway every time, worldwide.
2. **The origin-side caching (Railway volume) is in excellent shape** — years
   of work documented in `docs/DATA_ARCHITECTURE.md` and the cache modules:
   fingerprinted per-storm SST/rainfall/observed caches, TTL'd live-track and
   DPS caches, satellite tile proxy cache, wind/pressure caches (now
   unconsumed), catalog pre-warm. The one hole was the live-track TTL expiry
   (13 s, fixed below).

## 2. Railway persistent volume vs Cloudflare caching

They are **complementary layers, not alternatives** — they cache different
things against different failure modes:

| | Railway volume | Cloudflare edge |
|---|---|---|
| What it saves | **Recompute + upstream fetches** (ERDDAP, Open-Meteo, GIBS, IBTrACS, IKE math) | **Origin round-trips + egress** for byte-identical responses |
| Latency win | Turns 5–25 s computes into ~150 ms | Turns ~150 ms origin hits into ~20 ms, globally |
| Survives | Redeploys, works for POST bodies, per-key TTLs/fingerprints under our control | Origin outages (stale-serve), traffic spikes |
| Weaknesses | Still burns Railway CPU/egress per request; single region | Opaque eviction (esp. free tier), needs purge discipline, GET-only, per-PoP (a HIT in Frankfurt is a MISS in Sydney) |
| Current use | Extensive and correct | **Under-used: only extension-default assets** |

**Verdict: keep the volume as the "don't recompute" layer (no changes
needed); the win available is on the Cloudflare side** — one Cache Rule turns
the site's biggest static payloads from per-request origin hits into edge
HITs. Do not try to move volume-cached compute to the edge; do not add volume
caching where CF suffices (versioned static JSON).

## 3. Shipped with this audit

**Live-track stale-while-revalidate** (`api/routes.py`): a TTL-expired
live-storm track cache is now served immediately (`X-Track-Cache:
stale-refreshing` — normally one advisory old, capped at 6 h after downtime)
while a singleflight background task re-enters the route (contextvar bypass,
strong task ref) to recompute and re-save. The hourly DPS warm loop sets the
same bypass so live DPS bundles keep computing from fresh tracks. The
13-second first-viewer stall after every 30-min TTL expiry is gone; genuinely
uncached storms still compute inline. Track responses also gain
`Cache-Control: public, max-age=120` (browser-level; a prerequisite for any
future edge rule).

## 4. Roadmap (ranked by payoff ÷ effort)

### Operator (Cloudflare dashboard — no code)
1. ~~**Cache Rule: `stormdps.com/frontend/*`**~~ — **DONE 2026-07-08**
   (operator-deployed; expression `http.request.uri.path wildcard
   "/frontend/*"`, Eligible for cache, Edge TTL = use cache-control header if
   present else bypass, Browser TTL = respect origin). Verified live: bundle
   DYNAMIC→MISS→HIT (Railway round-trip eliminated), nri_zones HIT at 16 ms,
   sw.js still revalidates (`no-cache` honored), HTML untouched. Gotcha
   learned: the "URI Full" field includes the scheme — `/frontend/*` under
   URI Full matches nothing; the field must be **URI Path**.
2. **Cache Rule: `stormdps.com/api/v1/storms/catalog*` → cache, respect
   origin** (`s-maxage=900` already set). Same for `/api/v1/storms/*/track`
   once comfortable — the new `max-age=120` bounds staleness.
3. **Leave HTML DYNAMIC for now.** Origin TTFB is ~150 ms and deploys are
   frequent; edge-caching HTML would reintroduce mandatory purges into every
   deploy for ~130 ms. Revisit when iteration slows.
4. (Confirmed fixed: sw.js now serves `no-cache` — the Browser-Cache-TTL
   override issue from June is no longer reproducible.)

### Code (next sessions)
5. **Bundle diet** — `compiled_bundle.json` is 3.56 MB raw / ~19 KB per storm,
   parsed on every first visit. Split into a slim index (id, name, year, dps,
   dps_label, category — ~40 KB) loaded eagerly + per-storm detail
   (dpi_timeseries, factors, rainfall text) fetched on storm open (or as a
   second idle-deferred file). Cuts first-visit parse/memory ~90% on mobile;
   requires bake + sw + BUNDLE_VERSION ceremony.
6. **Catalog cold-start** (CLAUDE.md open item): move the IBTrACS warm in
   `main.py` lifespan after `yield` so redeploys serve traffic immediately.
7. **Apply the SWR pattern** to the live `/dps` bundle cache if its TTL
   expiry shows the same (smaller) stall.
8. **PageSpeed re-run** after rule #1 lands (last blocker to a clean score
   was bundle transfer).

### UI strategy (future, from this session's reviews)
9. **Mobile flood-banner & simplification pattern worked** — apply the same
   "one line, tap to expand" treatment to the Stall-Risk banner next.
10. **Legend**: re-add an Energy row only if a layer actually paints by IKE;
    tabindex/keyboard toggle for the flood banner (a11y nit from review).
11. **3D showcase**: candidate upgrades live in HANDOFF §4.5 (cirrus deck,
    updraft/subsidence, rainband arcs, bloom) — pure polish, do after a quiet
    news day.
12. **Observed-surge button** is the last non-Legend map control (analyst
    mode); fold it into the analyst panel on /methodology if map minimalism
    should go further.

## 5. What was checked and found healthy (no action)

- Homepage critical path: ~520 ms; fonts preloaded + edge-cached; Leaflet/
  Chart.js lazy; bundle deferred to idle.
- Satellite pipeline: tiles edge-cache (1 h) + volume-cache at origin;
  auto night-IR; frames endpoint cheap (140 ms).
- Always-on Windfield/Wind flow: pure client-side compute, zero network.
- Per-storm data layers (SST/rainfall/observed): fingerprinted volume caches,
  pre-warmed for all 223 catalog storms; live TTLs bounded.
- Open-Meteo exposure: eliminated (wind/pressure/precip overlays retired —
  `docs/RETIRED_MAP_OVERLAYS.md`).
