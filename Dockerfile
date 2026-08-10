FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies + tippecanoe build deps + gosu (for the
# privilege-drop pattern in docker-entrypoint.sh — see that file's
# header for why we need it).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    libgomp1 \
    libexpat1 \
    libsqlite3-0 \
    libsqlite3-dev \
    libcurl4 \
    zlib1g-dev \
    git \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Build tippecanoe from source (vector tile generator for PMTiles)
RUN git clone --depth 1 https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
    && cd /tmp/tippecanoe \
    && make -j$(nproc) \
    && cp tippecanoe tippecanoe-decode tippecanoe-enumerate tippecanoe-json-tool tile-join /usr/local/bin/ \
    && rm -rf /tmp/tippecanoe

# Copy requirements first for better Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Persistent data — mount a Railway volume at /app/persistent
# Contains: cache/ike/ (IKE results), cache/ibtracs_catalog.json,
# cache/preload_bundle.json, validation/ (SQLite DB + JSONL logs)
# Set PERSISTENT_DATA_DIR=/app/persistent in Railway env vars
# Falls back to /app/data when env var is not set
RUN mkdir -p /app/persistent/cache/ike /app/persistent/validation

# Create the non-root app user. Note: we do NOT `USER app` here —
# the entrypoint script needs to run as root briefly so it can chown
# the Railway-mounted volume to app:app before exec'ing gunicorn via
# gosu. Without that step the app user can't write to /app/persistent.
RUN groupadd --system --gid 1001 app \
    && useradd  --system --uid 1001 --gid app --home /app --shell /usr/sbin/nologin app \
    && chown -R app:app /app

# Entrypoint script: chowns the volume, then drops to `app` via gosu.
# See docker-entrypoint.sh for the why and how.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Railway sets PORT automatically (8080); default to 8080 for consistency
ENV PORT=8080

# glibc keeps one malloc arena per thread by default (8 × cores) and rarely
# returns freed spike memory (IBTrACS refresh / bakes) to the OS — the RSS
# high-water mark becomes the PAID baseline on Railway's per-GB-minute
# billing. Two arenas trades a little allocator contention for a resident
# footprint that tracks the real working set.
ENV MALLOC_ARENA_MAX=2

# MALLOC_ARENA_MAX alone did not hold the floor down, because it is the wrong
# knob for this symptom: it caps how many arenas exist, not whether freed
# memory goes back to the OS. glibc's mmap threshold is DYNAMIC by default —
# it starts at 128 KB, and every time an mmap'd block is freed it ratchets up
# to that block's size (capped at 32 MB), permanently. After the first few
# large frees, everything under 32 MB is served from the brk heap instead and
# is only returned when it happens to sit at the very top. That is exactly the
# observed signature: blocks over 32 MB still released cleanly (a 3,458 MB
# drop was measured in one 10 s window) while the floor climbed over days.
#
# Setting MALLOC_MMAP_THRESHOLD_ explicitly DISABLES that dynamic adjustment
# (glibc: "if this parameter is set, the dynamic adjustment is disabled"), so
# the process keeps behaving like a freshly-started one. Both values are
# glibc's own defaults — this pins them rather than raising or lowering them.
#
# Cost: allocations between 128 KB and 32 MB now take an mmap/munmap syscall
# pair instead of coming from the heap. For this workload — a handful of large
# buffers, not a high-rate allocator — that is a fair trade for a floor that
# does not ratchet.
ENV MALLOC_MMAP_THRESHOLD_=131072
ENV MALLOC_TRIM_THRESHOLD_=131072

# Expose the port
EXPOSE ${PORT}

# Container starts as root, runs entrypoint, entrypoint chowns the
# Railway volume + drops to `app` via gosu + execs gunicorn directly
# (no intermediate shell, so SIGTERM during graceful shutdown reaches
# gunicorn's master process and workers can drain in-flight requests
# within --graceful-timeout 30). All gunicorn flags live in the script.
# --preload loads the app in the master before forking so IBTrACS
# (~100 MB) is shared via copy-on-write instead of duplicated per
# worker; --workers 1 prevents OOM on 512 MB Railway containers.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# The gunicorn invocation lives in docker-entrypoint.sh (see that file
# for the rationale — TL;DR: shell-form CMD wraps in /bin/sh -c which
# breaks SIGTERM propagation during Railway's graceful shutdown).
# CMD is JSON exec form so any docker-run override args get appended
# verbatim to the gunicorn command line inside the entrypoint:
#   docker run img --reload --log-level=debug
CMD []
