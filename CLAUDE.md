# StormDPS — Claude context

**Last updated:** July 9, 2026  
**Live site:** https://stormdps.com · **GitHub:** https://github.com/oldreaverg-lang/StormDPS · **Railway** auto-deploys on push to `main`.

---

## Operating rules

- Commit straight to `main`. No PRs, no worktrees.
- No "good stopping point" / "let's call it a night" suggestions. Match user's pace.
- When the user says no, brief acceptance and move on.
- Don't add `defer`/`async` to external scripts without grepping inline JS for top-level references to that lib's globals first (e.g. `Chart.register(...)` at top level will ReferenceError and silently halt the entire script).
- Don't pass named functions directly to `addEventListener` if they have meaningful positional params — the event object clobbers them. Wrap: `() => fn()`.

## ⚠️ CRITICAL: NTFS mount truncation

**Never read large files from the Linux sandbox mount path (`/sessions/.../mnt/APPS/`) and push to GitHub.** The mount silently truncates large files — no error, clean cut. Has already destroyed: `routes.py`, `noaa_client.py`, `atcf_bdeck_client.py`, `main.py`, `frontend/index.html`.

**It's the *tool*, not git.** The truncation (and the `.git`-write failure) only happen through the **Bash/Linux** tool's mount. Native git run via **PowerShell** reads/writes the real `C:\` filesystem safely — so the **default** commit/push path is plain `git add && git commit && git push` **via PowerShell** (keeps local HEAD in sync; no API-commit desync, no manual `git reset` afterward). `credential.helper=manager` is configured, so push auth works non-interactively. `gh_push.py` (API commit) is the **fallback** — use it only when working through the Bash tool or if native push auth fails.

**Safe read paths:**
1. **Read tool** (`C:\Users\Ryan\APPS\...`) — always complete
2. **GitHub API** — fetch a known-good commit, patch in memory

**⚠️ 2026-07-01 addendum — the mount also serves STALE/CHIMERA reads.** After
editing a file with the Edit/Write tools, the Bash mount can serve a hybrid:
new content but the OLD byte length — silently cutting off the file's tail
(observed on files as small as 15 KB, and on `compile_cache.py` where the
truncation ate the `if __name__` block, making `python compile_cache.py` a
silent no-op). Consequences:
- **Never run repo scripts via Bash that read-modify-write repo files**
  (`scripts/rebake.py`'s sw.js/index.html version bump read a truncated
  index.html through the mount and WROTE THE TRUNCATED FILE BACK to the real
  filesystem — destroyed the tail of frontend/index.html; recovered via the
  GitHub-API pattern). Run `rebake.py` from native PowerShell only.
- **Never gh_push a file that was edited in the same session** — gh_push reads
  through the mount and can push the chimera. Push via native PowerShell git,
  or verify the pushed blob SHA against the local bytes afterward.
- Bash IS safe for: read-only analysis of files not edited this session,
  network calls, and code copied to /tmp first.

**Recovery pattern:**
```python
info = gh('GET', f'/contents/{path}?ref={good_sha}')
original = base64.b64decode(info['content']).decode()
patched = original.replace(OLD, NEW)          # in memory, never touch mount
py_compile.compile(tmp, doraise=True)         # verify Python files
gh('PUT', f'/contents/{path}', {'content': b64encode(patched), 'sha': cur_sha, ...})
```

---

## User context

- Solo project, self-funded, no business model yet. Possibilities: portfolio, civic-good, B2B insurance pivot.
- Wait for first 2026 Atlantic storm before any backlink/PR push.
- Apple submission paused — see `docs/APPLE_SUBMISSION.md`.
- Technical, no-nonsense, fast feedback loops.

---

## Infrastructure

- **Railway** single project, auto-deploys on `main` push.
- **Cloudflare** in front of stormdps.com. After HTML deploys: Caching → Purge Everything.
- **Env vars on Railway** (do not touch without confirmation): `ADMIN_TOKEN`, `ALLOWED_ORIGINS`, `PERSISTENT_DATA_DIR=/app/persistent`.
- **Search Console**: Google + Bing both verified. Sitemap submitted.

---

## Key files

```
main.py                    FastAPI entry, top-level routes, lifespan
api/routes.py              Core API — /storms/active, /storms/{id}/dps, /cache/*, /admin/*
api/schemas.py             Pydantic models
core/dps_engine.py         Destructive Power Score formula
core/ike.py                IKE computation + coastal zone weights (ERS)
core/cumulative_dpi.py     Lifetime DPI
services/noaa_client.py    NHC / IBTrACS / ATCF / HURDAT2 fetching
services/atcf_bdeck_client.py  ATCF b-deck — NHC FTP (EP/AL) + UCAR RAL (WP/IO/SH)
frontend/index.html        6,400-line SPA (live tracker)
frontend/compiled_bundle.json  ~200 storms, pre-computed DPS (baked at build time)
build_nri_zones.py         Rebuild frontend/nri_zones.json from FEMA NRI
audit_nri_zones.py         Sanity-check after any nri_zones.json rebuild — always run this
compile_cache.py           Bake DPS scores into compiled_bundle.json
```

Service clients in `services/` — 24+ files. Active ones: `noaa_client`, `atcf_bdeck_client`, `jtwc_client`, `mrms_client`, `imerg_rainfall`, `weather_data_service`. The rest are integrations of varying maturity.

---

## How to operate

**Push a file safely** — use the recovery pattern above; never read from the mount for files >50 KB.

**After deploy** — purge Cloudflare cache if HTML changed, then warm with a browser hit before running PageSpeed.

**Verify SSR storm pages:**
```
curl -s https://stormdps.com/storm/AL122005 | grep -E '<h1|<title|"datePublished"'
```

**Debug storms not loading** — DevTools console first. Real error is at the top; cascades follow.

**Service Worker stale code** — DevTools → Application → Service Workers → "Update on reload".

**API prefix:** all routes are `/api/v1/...` (not `/api/...`).

---

## What NOT to do

- Don't modify Cloudflare settings without guiding the user through the dashboard.
- Don't run interactive CLI auth commands (`eas init`, `gh auth login`) — give the user the command.
- Don't add backwards-compatibility shims.
- Don't commit `.env` files. Secrets live in Railway env vars only.
- Don't recommend Cloudflare Rocket Loader — breaks complex inline JS.
- Don't read large files from the NTFS mount and push them (see CRITICAL section above).

---

## Open items

1. **PageSpeed re-test** — run after `724571e` (async Leaflet CSS) once Cloudflare is warm.
2. **Catalog cold-start** — defer IBTrACS warm in `main.py` lifespan to after `yield`.
3. **Apple submission** — blocked on user running `eas init`. See `docs/APPLE_SUBMISSION.md`.
4. **Push notifications (v1.1)** — deferred; requires APNs + server pipeline.

## Current state

See `HANDOFF.md` — rewritten every session; this file is not. (The stale
June 16 snapshot that lived here ended mid-sentence — an old truncation
artifact — and was removed 2026-07-09.)

---

## Project skills (.claude/skills/)

Repeatable workflows are captured as skills - use them instead of re-deriving:

- `github-safe-push` - commit/push to `main` without the NTFS-truncation bug (ships `scripts/gh_push.py`, which refuses NUL-corrupted or non-compiling files). Prefer this over the inline recovery pattern above.
- `compile-cache-bake` - rebake `frontend/compiled_bundle.json` after a scoring change.
- `nri-zone-rebuild` - regenerate `frontend/nri_zones.json` from FEMA NRI + audit.
- `basin-dps-audit` - validate a basin's DPS formula vs ground truth before changing coefficients.
- `deploy-verify` - purge/warm Cloudflare + verify SSR/PageSpeed after a deploy.
- `code-review` - fresh-eyes sub-agent review before pushing.

## Testing & secret scanning

- `pytest -q` runs the offline suite in `tests/` (compile-guard over every module, `compiled_bundle.json` invariants; validator scripts opt-in via `--run-integration`). Dev deps: `requirements-dev.txt`.
- Secret scanning: `.pre-commit-config.yaml` (gitleaks) - activate once with `pip install pre-commit && pre-commit install`. CI workflows `.github/workflows/secret-scan.yml` and `tests.yml` must be landed from native git (the agent PAT cannot push workflow files).
