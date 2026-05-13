# StormDPS — Session Handoff

For a new agent picking up this project. Read top to bottom; everything you need is here.

---

## What this is

**StormDPS** — open-data tropical cyclone analysis service. Computes a 0–100 "Destructive Power Score" combining peak wind, IKE, surge potential, duration, geographic reach. A modern alternative to the Saffir-Simpson Hurricane Wind Scale.

- **Live site:** https://stormdps.com (FastAPI on Railway, Cloudflare in front)
- **GitHub:** https://github.com/oldreaverg-lang/StormDPS
- **Dataset DOI:** [10.5281/zenodo.20149123](https://doi.org/10.5281/zenodo.20149123) (CC BY 4.0)
- **Mobile app:** React Native / Expo (separate Apple/Play submission, not live yet)

Author: Ryan Reaves. Solo project, self-funded, no business model yet — see "User context" below.

---

## Operating rules (read FIRST)

Read these memory files at session start, no exceptions:

```
~/.claude/projects/C--Users-Ryan-APPS-StormDPS/memory/MEMORY.md
```

That index points to:

- **`user_sleep_pattern.md`** — *"User sleeps in 1-hour increments — don't suggest stopping points / 'get some rest' / wrap-up summaries based on assumed fatigue."* The previous agent violated this repeatedly. Don't do it. No "let's call it a night," "is this a good stopping point," "we can revisit in the morning." Match the user's pace; they will say when they're done.
- **`feedback_direct_to_main.md`** — Commit straight to main on this repo. Skip worktree isolation / PRs. `git push` deploys to Railway automatically.
- **`feedback_dont_overexplain_after_rejection.md`** — When the user says no, brief acceptance + move on. No long mea-culpa explanations.
- **`feedback_defer_check_top_level_refs.md`** — Before adding `defer`/`async` to any external `<script>`, grep the inline JS for top-level references to that library's globals. Top-level `Chart.register(...)` etc. will ReferenceError during parse and silently halt the entire inline script. Cascade errors hide the real cause.
- **`feedback_event_listener_arg_leak.md`** — `addEventListener('foo', namedFn)` invokes `namedFn(event)`. If `namedFn` has meaningful positional args, the event object clobbers them. Always wrap with `() => fn()` in listener registration helpers.
- **`project_seo_outreach_timing.md`** — SEO infrastructure is done; backlink outreach is intentionally deferred until first 2026 Atlantic storm (June+) gives a news hook. Don't proactively suggest outreach steps.

---

## User context

- **Goal**: still TBD. Possibilities: portfolio piece for hire, civic-good project, B2B insurance pivot. Not making money currently and no rush to.
- **Strategic decision**: wait for first 2026 Atlantic storm before doing any backlink/PR push. Hurricane season starts June 1, 2026. First named storm typically mid-late June.
- **Apple submission**: in progress, paused mid-flow. User has Apple Developer account approved. Next user action: run `cd mobile && npx eas-cli login && npx eas-cli init` — must be done by the user (interactive Expo login). Until that's done, no EAS build.
- **Tone**: technical, no-nonsense, fast feedback loops. Confirms when something works. Gets visibly annoyed when you over-explain or hedge.

---

## Infrastructure & secrets

- **Hosting**: Railway, single project (`oldreaverg-lang/StormDPS`), auto-deploys on push to `main`.
- **CDN**: Cloudflare in front of stormdps.com. DNS-managed there.
- **Cloudflare settings already applied**:
  - Email Address Obfuscation: **OFF**
  - Browser Cache TTL: **Respect Existing Headers**
  - Auto Minify HTML/CSS/JS: **ON** (verified safe with current code)
- **Railway env vars set** (do NOT touch these without explicit user confirmation):
  - `ADMIN_TOKEN` — gates DELETE /cache/*, POST /preload/generate, /validation/outcome, /audit/radii, /admin/warm-ibtracs. Passed as `X-Admin-Token` header.
  - `ALLOWED_ORIGINS` — comma-separated CORS whitelist (currently `https://stormdps.com,https://www.stormdps.com`)
  - `PERSISTENT_DATA_DIR=/app/persistent` — Railway volume mount
- **Search Console**: Google Search Console **verified**. Bing Webmaster Tools **verified** (auth file at `/BingSiteAuth.xml`). Sitemap submitted to both.

---

## Codebase layout

```
StormDPS/
├── main.py                    # FastAPI app entry, all top-level routes
├── seo.py                     # SSR helpers for /storm/{id} pages
├── storage.py                 # Persistent-volume paths, atomic writes, IKE eviction
├── api/
│   ├── routes.py              # Core API: /storms/active, /storms/{id}/dps, /cache/*, /admin/*
│   ├── weather_routes.py      # Weather data overlays
│   ├── satellite_routes.py    # Satellite tile server
│   ├── wind_routes.py         # Wind-field tiles
│   ├── pressure_routes.py     # Pressure isobar overlays
│   ├── precip_routes.py       # Precipitation tiles
│   └── schemas.py             # Pydantic models
├── core/
│   ├── dps_engine.py          # The Destructive Power Score formula
│   ├── ike.py                 # Integrated Kinetic Energy computation (Holland B vortex)
│   └── cumulative_dpi.py      # Coastal zone weights + lifetime DPI
├── services/
│   ├── noaa_client.py         # NHC / IBTrACS / ATCF / HURDAT2 fetching
│   ├── weather_data_service.py
│   ├── validation_log.py
│   ├── source_health.py
│   ├── wind_radii_audit.py
│   └── open_meteo_limiter.py
├── models/hurricane.py        # HurricaneSnapshot dataclass
├── frontend/
│   ├── index.html             # 6,000-line SPA (the live tracker)
│   ├── methodology.html       # Long-form SEO page
│   ├── historic-storms.html   # Ranked storms hub page
│   ├── data.html              # Dataset landing page (Dataset JSON-LD)
│   ├── faq.html               # FAQ with FAQPage JSON-LD
│   ├── about.html             # Author bio + contact (E-E-A-T)
│   ├── privacy.html           # Privacy policy (required for Apple)
│   ├── surgedps.html          # Legacy SurgeDPS landing
│   ├── surgedps/              # SurgeDPS React build (separate sub-app)
│   ├── sitemap.xml            # Submitted to Google + Bing
│   ├── robots.txt
│   ├── BingSiteAuth.xml       # Bing Webmaster verification token
│   ├── compiled_bundle.json   # ~200 storms with pre-computed DPS scores (baked)
│   ├── sw.js                  # Service Worker v4
│   └── logo*.png, favicon.ico
├── mobile/
│   ├── app.json               # Expo config — name "StormDPS", bundle com.stormdps.app
│   ├── package.json           # Renamed to "stormdps"
│   ├── eas.json
│   └── app/, src/             # Expo Router screens + services
├── historical_storms_db.csv   # The Zenodo-deposited dataset (CSV)
├── historical_storms_db.json  # Same data, JSON
├── dataset_README.md          # Data dictionary (goes in Zenodo deposit)
├── CITATION.cff               # GitHub "Cite this repository" config with DOI
├── README.md                  # Project README with DOI badge
├── Dockerfile                 # Railway build config
├── railway.toml
├── requirements.txt
├── .env.example               # Documents required env vars
├── .dockerignore
└── HANDOFF.md                 # This file
```

---

## Where the code stands right now

### Performance (mobile PageSpeed)

Last verified: **88** (up from 71 baseline). Trend over the session: 71 → 83 → 88 → (88 again — async CSS landed but couldn't be measured due to a Cloudflare cold-cache timeout on re-test).

| Metric | Value | Status |
|---|---|---|
| Performance | 88 | green-tier border |
| Accessibility | 90 | good |
| Best Practices | 96 | good |
| SEO | 100 | perfect |
| FCP | 3.0 s | needs improvement |
| LCP | 3.0 s | needs improvement |
| TBT | 0 ms | perfect |
| CLS | 0 | perfect |
| Speed Index | 3.0 s | needs improvement |

The most recent commit (`724571e`) async-loaded `leaflet.css` + `leaflet-velocity.css` via the `media="print"` swap trick — expected to push to 91–93, but the verifying PageSpeed run hit `ERR_TIMED_OUT` because of Railway redeploy + Cloudflare cold-cache + the catalog cold-start. **Re-test PageSpeed after a normal-browser warm hit**.

### Open performance wins, ranked

1. **Catalog cold-start (10.9 s)** — `/api/v1/storms/catalog/global` blocks during Railway warm-up because the IBTrACS warm task hogs CPU. Fix: in `main.py` lifespan, defer the IBTrACS warm to run *after* the server starts accepting requests (use `asyncio.create_task` only after the lifespan `yield`). Affects every first user after a redeploy.
2. **WebP for logo-180.png** — 23 KiB savings. Need to generate a `.webp` (Python + Pillow or external tool) and switch markup to `<picture>` with WebP source.
3. **Chart.js tree-shaking** — 62 KiB unused JS. Requires building Chart.js locally with only the modules we use; means adding a real frontend build pipeline. Significant work, modest gain.
4. **Inline-JS minification** — 27 KiB savings on the inline SPA script. Requires same build pipeline as #3.

### Recent commit history (most recent first)

- `724571e` — Async-load Leaflet CSS to remove last render-blocking requests (verify with fresh PageSpeed)
- `3dd0e02` — Fix runaway active-storms polling: `_onDomReady` was passing the event arg
- `9d8c9b6` — Fix Chart-undefined breakage from defer: wrap top-level `Chart.register` calls
- `378a9de` — Add edge-cacheable Cache-Control to `/` and bump compiled_bundle TTL
- `5f989d4` — Guard SPA autoload against defer-script race
- `8254cd0` — Mobile performance: defer external scripts, preconnect CDNs, cache static assets, fix font-swap CLS
- `5e7a7f7` — App Store prep: rebrand mobile to StormDPS, strip unused capabilities, ship privacy policy
- `09c96ea` — Second-pass audit fixes: SSR/welcome conflict, SW v4, polish
- `bed5206` — Audit-pass fixes: SSR consistency, iOS scroll, mobile UX, SEO polish
- `5442e5b` — SEO part 3: /about page, BreadcrumbList JSON-LD, internal-link audit
- `0e30442` — Add Bing Webmaster Tools site-ownership verification
- `5918e4e` — Wire Zenodo DOI everywhere: 10.5281/zenodo.20149123
- `df6b538` — Add CITATION.cff and dataset README for Zenodo deposit
- `61d4727` — SEO part 2: SSR body content, historic-storms hub, FAQ page
- `f2b93da` — SEO overhaul: SSR'd per-storm pages, methodology + data landing pages
- `4cbdac1` — Harden API for public deploy: admin auth, rate limit, CORS, non-root

Read these with `git log <hash> -1 --stat` for context.

---

## Apple App Store submission — paused mid-flow

**Current state:**

- App.json renamed to "StormDPS", bundle ID `com.stormdps.app`, slug `stormdps`
- Stripped unused capabilities (no location, no background modes, no notifications plugin — these were declared but unused, which would have gotten rejected under Apple Guideline 2.5.4)
- Icon and splash set to the 1024×1024 brand logo (Apple-spec RGB, no alpha)
- Privacy policy live at https://stormdps.com/privacy (required for submission)
- Mobile package.json deps cleaned up (expo-location, expo-notifications removed)

**Next user action (BLOCKING — cannot proceed without):**

```
cd mobile
npx eas-cli login        # interactive — user's Expo account
npx eas-cli init         # writes projectId into app.json
```

This is a TTY-interactive flow and cannot be done from inside an agent session.

**Then user runs:**

```
npx eas-cli build --platform ios --profile preview
```

This builds in EAS Cloud (10–15 min), auto-uploads to App Store Connect.

**Then user adds testers** via App Store Connect → TestFlight → Internal Testing:
- Self (iPhone 13 — testing only, not screenshot device)
- Friend with iPhone Pro Max (6.9" screenshots — Apple requires this size)
- Dad with iPad (iPad screenshots — required since `supportsTablet: true`)

**Once testers screenshot:**

Agent should write the App Store Connect listing copy: description (max 4000 chars), subtitle (max 30 chars), promotional text (max 170 chars), keywords (max 100 chars comma-separated), what's-new, support URL (set to https://stormdps.com/about), marketing URL (https://stormdps.com), age rating questionnaire (4+ probably), privacy nutrition labels (Data Not Collected — verified by grep, no analytics SDKs in mobile code).

**Weather-app review risk:**

Apple scrutinizes weather/safety apps. The web has the "Experimental & Educational — defer to NHC" disclaimer in the footer. The **mobile app needs the same disclaimer somewhere obvious** — probably first-launch screen + Settings tab. Audit mobile UI for this before submitting.

**Notifications:**

User wants push notifications when a storm's DPS score changes. **DEFERRED to v1.1.** v1 ships without. Building real push requires: APNs auth key from Apple Developer portal, server-side push token storage + monitoring job + sender service, mobile permission UI + subscribe UI. ~1–2 weeks. v1 → ship → v1.1 → add push as feature update.

---

## SEO state (complete)

Everything code-side is done:

- Path-based per-storm URLs: `/storm/{ATCF_ID}` server-rendered with unique title, meta description, canonical, OG/Twitter cards, Article + BreadcrumbList JSON-LD, and a visible H1 + DPS score card with prose.
- Long-form landing pages: `/methodology`, `/historic-storms`, `/faq`, `/data`, `/about`, `/privacy`. All have proper JSON-LD (Article, FAQPage, Dataset, AboutPage, BreadcrumbList).
- Homepage Dataset JSON-LD with DOI identifier (for Google Dataset Search).
- Zenodo deposit live at https://doi.org/10.5281/zenodo.20149123.
- CITATION.cff so GitHub shows "Cite this repository" button.
- Sitemap.xml lists every URL with `<lastmod>` entries.
- Both Google Search Console + Bing Webmaster verified.
- IBTrACS-pattern URLs get `noindex,follow` to avoid duplicate-content with ATCF URLs.

What's left (deferred to first storm of season):

- Hacker News Show HN post
- Cold emails to weather bloggers (Eye on the Tropics, Tropical Tidbits, Yale Climate Connections, Capital Weather Gang)
- Twitter/X presence during active storms

---

## How to operate

### Git workflow

- Worktree: `C:\Users\Ryan\APPS\StormDPS\.claude\worktrees\stoic-spence-f672c2`
- Main checkout: `C:\Users\Ryan\APPS\StormDPS`
- Branch in worktree: `claude/stoic-spence-f672c2`
- **Commit straight to main** on this repo. From the worktree, push with:
  ```
  git push origin claude/stoic-spence-f672c2:main
  ```
  This pushes the local branch to the remote `main`. Railway auto-deploys.
- User can sync their main checkout via `git pull --ff-only`.

### Cache invalidation after a deploy

If a deploy ships HTML changes and Cloudflare is caching the page, the user must purge:

- Cloudflare → Caching → Configuration → Purge Everything (nuclear, fine for this site)
- Or surgical: Custom Purge with specific URLs

Then hit the site once in a browser to warm the new cache.

### Testing changes

- Local: no, the SPA doesn't run locally without the FastAPI backend running too. Just push to main and watch Railway.
- Live: check https://stormdps.com after Railway shows green.
- Mobile PageSpeed: https://pagespeed.web.dev/analysis?url=https%3A%2F%2Fstormdps.com — **warm Cloudflare with a real-browser hit first**, then run.

### Common debug paths

- **Storms not loading**: check browser DevTools console first. Real errors are usually at the top of the console output; subsequent errors are often cascades from the first one halting script execution.
- **Cloudflare serving stale content**: purge cache, hard refresh, or load with `?v=N` query string to bypass.
- **Service Worker serving old code**: DevTools → Application → Service Workers → "Update on reload" checkbox forces a fresh SW fetch every refresh.

### Verifying SSR'd storm pages

```
curl -s https://stormdps.com/storm/AL122005 | grep -E '<h1|<title|"datePublished"' | head
```

Should show: `<title>Hurricane Katrina (2005) — DPS …`, `<h1>Hurricane Katrina (2005) — DPS …`, and Article JSON-LD.

---

## What NOT to do

- Don't suggest stopping points or "good places to pause." Match user's pace.
- Don't modify Cloudflare settings on the user's behalf — guide them through the dashboard.
- Don't run interactive CLI commands that require auth (`eas init`, `gh auth login`, etc.) — give the user the command to run themselves.
- Don't add backwards-compatibility shims — the user prefers clean breaking changes when justified.
- Don't commit secrets or `.env` files. ADMIN_TOKEN and ALLOWED_ORIGINS live in Railway env vars only.
- Don't add `defer`/`async` to external scripts without grepping inline JS for top-level references to that lib's globals first (see `feedback_defer_check_top_level_refs.md`).
- Don't pass named functions directly to `addEventListener` if they have meaningful positional parameters (see `feedback_event_listener_arg_leak.md`).
- Don't recommend Cloudflare Rocket Loader. It's been known to break complex inline JS.

---

## Loose ends to potentially tackle

In rough priority order, none are urgent:

1. **PageSpeed verify** — re-test after `724571e` to confirm we hit ~91–93.
2. **Catalog cold-start** — defer IBTrACS warm in lifespan (see "Open performance wins" #1 above).
3. **Apple submission flow** — blocked on user running `eas init`.
4. **Mobile in-app disclaimer audit** — verify the "not an official forecast" disclaimer is prominent in mobile UI before Apple submission.
5. **WebP logo** — small perf win.
6. **Push notifications (v1.1)** — full feature; requires APNs + server pipeline.

---

End of handoff. Read the memory files first, then attack whatever the user brings up.
