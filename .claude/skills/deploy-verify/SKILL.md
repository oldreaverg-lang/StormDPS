---
name: deploy-verify
description: Verify a stormdps.com deploy after pushing to main - purge stale Cloudflare cache, warm it, check SSR storm pages, and re-run PageSpeed. Use after any push that changes HTML/JS/CSS or after a Railway redeploy.
---

# Verify a deploy

`git push` to `main` auto-deploys to Railway, behind Cloudflare. Run this after a deploy that touches the frontend, or after a redeploy.

## Steps

1. Wait for Railway to show the new deployment green.
2. If HTML/JS/CSS changed, purge the stale edge copy (targeted, not "purge everything"):
   - Cloudflare dashboard -> Caching -> Configuration -> **Purge Custom URLs**:
     `https://stormdps.com/` and `https://stormdps.com/index.html`
   - See `docs/CACHING.md` for prefix/tag purges and bypass tricks.
3. Warm the cache with one real-browser hit to `https://stormdps.com/` BEFORE measuring anything (cold Cloudflare + Railway cold-start skews results).
4. Verify server-rendered storm pages:
   ```
   curl -s https://stormdps.com/storm/AL122005 | grep -E '<h1|<title|"datePublished"'
   ```
   Expect the Katrina title + H1 + Article JSON-LD.
5. Confirm the persistent volume is mounted (should read `/app/persistent`):
   ```
   curl -s https://stormdps.com/health/storage | jq .root
   ```
6. Re-run mobile PageSpeed AFTER warming:
   https://pagespeed.web.dev/analysis?url=https%3A%2F%2Fstormdps.com

## Reference

- `docs/CACHING.md` - purge recipes, cf-cache-status debugging, recovery flows.
