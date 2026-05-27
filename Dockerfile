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

# Expose the port
EXPOSE ${PORT}

# Container starts as root, runs entrypoint, entrypoint drops to app
# via gosu and execs the CMD below. --preload loads the app in the
# master process before forking, so IBTrACS data (~100MB) is shared
# via copy-on-write instead of duplicated per worker. --workers 1
# prevents OOM on 512MB Railway containers.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# Shell-form CMD so ${PORT} expands at runtime — combined with the
# ENTRYPOINT above, Docker hands the entrypoint argv equivalent to
# ["/bin/sh", "-c", "gunicorn ..."], which gosu execs as the app user.
CMD gunicorn main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:${PORT} \
    --workers 1 \
    --preload \
    --timeout 120 \
    --graceful-timeout 30 \
    --access-logfile - \
    --error-logfile -
