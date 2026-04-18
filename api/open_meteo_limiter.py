"""
Shared Open-Meteo request governor.

Open-Meteo's free tier allows ~10 requests/minute.  Wind, pressure and
precip overlay routes all hit the same upstream; without coordination a
single animation-scrub triggers 3 concurrent requests per frame and
quickly blows the rate budget.

This module provides:
  - A process-wide asyncio Semaphore (default: 3 concurrent requests)
  - A 429-aware wrapper that retries once after the Retry-After period
  - Proper error mapping: upstream 429 → client 429 (not 502)
"""

import asyncio
import logging
import os
import random

import httpx
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Per-overlay concurrency limits.
# Wind gets 1 slot — it fetches many historical frames and must not starve
# the others. Pressure and precip each get 1 slot so they always progress
# even when wind is busy burning through the track.
# All limits are env-overridable without a redeploy.
_MAX_CONCURRENT_WIND     = int(os.getenv("OPEN_METEO_MAX_WIND",     "1"))
_MAX_CONCURRENT_PRESSURE = int(os.getenv("OPEN_METEO_MAX_PRESSURE", "1"))
_MAX_CONCURRENT_PRECIP   = int(os.getenv("OPEN_METEO_MAX_PRECIP",   "1"))
_MAX_CONCURRENT_DEFAULT  = int(os.getenv("OPEN_METEO_MAX_CONCURRENT","1"))

_semaphores: dict[str, asyncio.Semaphore] = {}


def _get_semaphore(label: str) -> asyncio.Semaphore:
    """Lazy-init per-label semaphore on the running event loop."""
    key = label.upper()
    if key not in _semaphores:
        limits = {
            "WIND":     _MAX_CONCURRENT_WIND,
            "PRESSURE": _MAX_CONCURRENT_PRESSURE,
            "PRECIP":   _MAX_CONCURRENT_PRECIP,
        }
        _semaphores[key] = asyncio.Semaphore(limits.get(key, _MAX_CONCURRENT_DEFAULT))
    return _semaphores[key]


async def open_meteo_get(
    client: httpx.AsyncClient,
    url: str,
    params: dict,
    *,
    label: str = "overlay",
) -> httpx.Response:
    """GET from Open-Meteo with concurrency limiting + 429 retry.

    Returns the httpx.Response on success (status 200).
    Raises HTTPException(429) on rate-limit exhaustion or
    HTTPException(502) on other upstream failures.
    """
    sem = _get_semaphore(label)

    max_attempts = 3
    for attempt in range(max_attempts):
        async with sem:
            r = await client.get(url, params=params)

        if r.status_code == 200:
            return r

        if r.status_code == 429:
            if attempt < max_attempts - 1:
                # Respect Retry-After if provided, else progressive backoff.
                base_wait = float(r.headers.get("Retry-After", str(2 + attempt * 2)))
                # Add a small random jitter (0–0.5s) so a burst of concurrent
                # 429s — wind + pressure + precip all rate-limited together
                # — don't all wake up at the same instant and thunder the
                # upstream a second time. Per-call delta is bounded so we
                # never stretch beyond the 10s cap.
                wait = min(base_wait, 10.0) + random.uniform(0.0, 0.5)
                logger.info(f"[{label}] Open-Meteo 429 — retry {attempt+1}/{max_attempts-1} in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue
            # Exhausted retries — propagate as 429 to client
            raise HTTPException(
                429,
                detail="Open-Meteo rate limit exceeded; retry later",
                headers={"Retry-After": "5"},
            )

        # Any other non-200 — 502 to client
        raise HTTPException(
            502,
            f"Open-Meteo {r.status_code}: {r.text[:200]}",
        )

    # Should be unreachable, but satisfy the type-checker
    raise HTTPException(502, "Open-Meteo request failed after retries")
