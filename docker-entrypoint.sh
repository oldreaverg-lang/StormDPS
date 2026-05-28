#!/bin/sh
#
# StormDPS container entrypoint.
#
# Runs as root just long enough to fix permissions on the Railway
# persistent volume, then drops to the unprivileged `app` user (uid
# 1001) via gosu and execs the gunicorn command from the Dockerfile.
#
# Why this script exists
# ----------------------
# Railway mounts persistent volumes at runtime AFTER the image is built.
# The Dockerfile's `chown -R app:app /app` at build time changes the
# IMAGE's view of /app/persistent, but the volume mount overlays its
# own (often root-owned) inode on top. Result: at runtime, `app` user
# can read but not write inside /app/persistent/cache/.
#
# This blew up as:
#   [DPS CACHE] Failed to save WP062026: [Errno 13] Permission denied
#   [IBTRACS] Could not write default-view cache
#   ... and one warning per hour per uncached active storm.
#
# Fixing it inside the running process is impossible — the process
# runs as `app` and can't chown root-owned files. The fix has to
# happen at boot, as root, before we drop privileges.
#
# Idempotent and cheap:
#   * On a freshly-permissioned volume: chown is a no-op walk
#   * On a misconfigured volume: chown fixes every node in ~5–10 s
#     for a typical 5 GB tree of small files
#
# Safety:
#   * Failures are logged but non-fatal — if /app/persistent doesn't
#     exist (local dev or a misconfigured Railway service), we still
#     launch gunicorn
#   * The exec at the end replaces this shell with gunicorn so signals
#     (SIGTERM from Railway's stop) reach the worker cleanly
#
set -e

# ── Fix volume ownership (only when volume exists) ─────────────────
if [ -d /app/persistent ]; then
    echo "[entrypoint] chown -R app:app /app/persistent ..."
    # `|| true` because chown can hit transient errors on a few nodes
    # (e.g. a broken symlink the volume happens to contain) and we'd
    # rather log + continue than refuse to start.
    chown -R app:app /app/persistent 2>&1 | tail -n 20 || true
    echo "[entrypoint] volume root: $(ls -ld /app/persistent | awk '{print $1, $3, $4}')"
else
    echo "[entrypoint] /app/persistent not mounted — skipping chown (local dev?)"
fi

# ── Drop privileges and exec gunicorn directly ─────────────────────
#
# We invoke gunicorn from this script rather than relying on the
# Dockerfile's CMD because shell-form CMD lines get wrapped by Docker
# as `["/bin/sh", "-c", "..."]`, leaving a shell process between PID 1
# and gunicorn. SIGTERM from Railway during graceful shutdown then
# hits the shell, which doesn't reliably forward signals to gunicorn —
# in-flight requests get killed instead of finishing within
# --graceful-timeout. BuildKit calls this out as JSONArgsRecommended.
#
# With `exec gosu ... gunicorn ...`:
#   • gosu drops privileges in-place (no fork)
#   • exec replaces this shell with gunicorn (no extra PID)
#   • gunicorn becomes the direct child of the container init
#   • SIGTERM hits gunicorn's master, which gracefully stops workers
#
# PORT comes from Railway (or the Dockerfile's ENV PORT=8080 fallback).
# Any args passed by `docker run ... <args>` (via CMD or override)
# are appended after our base list, allowing local-dev overrides like
#   docker run img --reload
# without rewriting this script.
PORT="${PORT:-8080}"
exec gosu app:app gunicorn main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind "0.0.0.0:${PORT}" \
    --workers 1 \
    --preload \
    --timeout 120 \
    --graceful-timeout 30 \
    --access-logfile - \
    --error-logfile - \
    "$@"
