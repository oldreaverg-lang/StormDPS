# Caching & cache-clear recipes

StormDPS sits behind two cache layers:

1. **Cloudflare** — edge cache in front of `stormdps.com`. Caches static
   assets (`index.html`, JS, CSS) and, by default, 200 responses from
   `/api/v1/satellite/...`, `/api/v1/wind/...`, etc. when origin sets
   cacheable headers.
2. **Railway persistent volume** — mounted at `/app/persistent` on the
   container (see `Dockerfile`). `storage.py` places every overlay cache
   under `$PERSISTENT_DATA_DIR/cache/` so a redeploy doesn't wipe tiles or
   precomputed fields.

Most "inconsistent loading" reports trace back to one of:

- A stale `index.html` pinned at the Cloudflare edge (fixed JS never ran
  on the user's browser).
- A 502 that got cached at the edge (we now strip cache headers on 5xx,
  but older cached 502s can linger until TTL expiry).
- Missing `PERSISTENT_DATA_DIR` env var on Railway — the container then
  falls back to `/app/data`, which *is* wiped on redeploy.

## Verifying the volume is actually in use

```
curl -s https://stormdps.com/health/storage | jq
```

The `root` field should read `/app/persistent`. If it reads `/app/data`,
set `PERSISTENT_DATA_DIR=/app/persistent` in the Railway service's env
vars and redeploy.

## Cloudflare: targeted purge

Prefer targeted purges over "purge everything" — a full purge
temporarily eliminates edge offload and hammers the origin.

### Purge a single URL (most common)

Cloudflare dashboard → Caching → Configuration → **Purge Custom URLs**.
Paste the exact URLs you want to invalidate:

```
https://stormdps.com/
https://stormdps.com/index.html
https://stormdps.com/api/v1/satellite/frames/goes-east
```

### Purge by tag or prefix

Requires Enterprise. If available:

```
curl -X POST \
  https://api.cloudflare.com/client/v4/zones/$ZONE_ID/purge_cache \
  -H "Authorization: Bearer $CF_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"prefixes":["stormdps.com/api/v1/wind"]}'
```

### Emergency: purge everything

Dashboard → Caching → Configuration → **Purge Everything**. Use sparingly
— expect a temporary spike in origin load.

### Bypass cache while debugging

Append a random query string to force a miss without purging:

```
https://stormdps.com/index.html?cb=$(date +%s)
```

Or set a Page Rule for `stormdps.com/*` → *Cache Level: Bypass* while
iterating, and remove it once the fix ships.

## Railway: rebuild & volume ops

### Force a rebuild without a code change

Railway dashboard → Deployments → **Redeploy** on the latest deployment.
This rebuilds the container but leaves `/app/persistent` intact.

### Inspect the persistent volume

Open a shell on the running service (Railway → Deployments → **...** →
Shell):

```
du -sh /app/persistent/cache/*
ls /app/persistent/cache/satellite/ | head
cat /app/persistent/cache/ibtracs_index_v1.json | jq '. | length'
```

Or from your laptop:

```
curl -s https://stormdps.com/health/storage | jq
```

### Drop a single overlay cache (leaves everything else alone)

```
# In the Railway shell:
rm -rf /app/persistent/cache/wind/*
rm -rf /app/persistent/cache/pressure/*
rm -rf /app/persistent/cache/precip/*
rm -rf /app/persistent/cache/satellite/*
```

Directories are auto-recreated on the next request (see
`storage.py`). Directory creation is idempotent — safe to `rm -rf` the
subtree if a layer gets into a weird state.

### Drop the compiled DPS bundle

```
rm /app/persistent/cache/compiled_bundle.json
rm /app/persistent/cache/preload_bundle.json
```

`compile_cache` will rebuild both on the next invocation.

### Nuclear option

```
rm -rf /app/persistent/cache
```

Only do this if nothing is working — it forces every layer to refetch
from upstream (NOAA, NASA GIBS, Open-Meteo) on the next request.

## When "inconsistent satellite loading" is actually the origin

If tile requests look inconsistent in the browser network panel:

1. Check `/health/storage` — if `satellite_cache.size_mb` is climbing,
   the cache is working; the inconsistency is upstream (NASA GIBS rate
   limiting or missing frames).
2. Check the browser devtools Network tab for `cf-cache-status`:
   - `HIT` — Cloudflare served it.
   - `MISS` / `EXPIRED` — request went to origin, so this is an origin
     concern, not a Cloudflare one.
   - `BYPASS` — a Page Rule or response header disabled caching for
     that URL.
3. Check Railway logs for `ike_routes` or `satellite_routes` errors —
   NASA GIBS occasionally returns 404 for frames that haven't been
   published yet, which we map to a transparent client-side skip.

## Common recovery flows

### "The frontend looks old after I deployed"

Cloudflare is serving a stale `index.html`. Purge:
`https://stormdps.com/` and `https://stormdps.com/index.html`.

### "Wind layer returns 502"

Almost always Open-Meteo URL length. `_MAX_GRID_POINTS = 350` in
`api/wind_routes.py` keeps us under their ceiling. If this recurs,
check recent commits to that file.

### "Pressure field looks misaligned with the storm"

Usually a storm-timeline sync bug, not a data bug. The storm-track
marker and the satellite/pressure frames must share a timestamp;
`setSatelliteFrame` in `frontend/index.html` snaps the track marker to
the frame's nearest storm index.

### "Storage used percent keeps climbing"

Each layer has its own eviction (see `evict_old_wind_frames`,
`evict_old_pressure_frames`, `evict_old_precip_frames`,
`evict_ike_cache`). If one isn't running on schedule, trigger it
manually by touching any cached file that layer owns — the next write
runs eviction.
