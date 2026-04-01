FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Railway sets PORT automatically; default to 8000 for local testing
ENV PORT=8000

# Expose the port
EXPOSE ${PORT}

# Run uvicorn directly (lower memory than gunicorn + worker)
CMD uvicorn main:app \
    --host 0.0.0.0 \
    --port ${PORT} \
    --timeout-keep-alive 120 \
    --log-level info \
    --access-log
