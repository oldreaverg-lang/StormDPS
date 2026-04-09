FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies + tippecanoe build deps
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

# Railway sets PORT automatically (8080); default to 8080 for consistency
ENV PORT=8080

# Expose the port
EXPOSE ${PORT}

# Run with gunicorn + uvicorn workers for production
CMD gunicorn main:app \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:${PORT} \
    --workers 2 \
    --timeout 120 \
    --graceful-timeout 30 \
    --access-logfile - \
    --error-logfile -
