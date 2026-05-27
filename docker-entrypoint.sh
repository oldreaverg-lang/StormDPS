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

# ── Drop privileges and exec the CMD ────────────────────────────────
# gosu is preferred over `su` because it doesn't fork an intermediate
# shell, so signals propagate correctly and there's no extra PID.
# The "$@" forwards the Dockerfile's CMD (shell-form gunicorn invocation
# wrapped by Docker as `/bin/sh -c '...'`) verbatim.
exec gosu app:app "$@"
