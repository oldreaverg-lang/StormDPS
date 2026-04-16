"""
Shared Open-Meteo request governor.

Open-Meteo's free tier allows ~10 requests/minute.  Wind, pressure and
precip overlay routes all hit the same upstream; without coordination a
single animation-scrub triggers 3 concurrent requests per frame and
quickly blows the rate budget.

This module provides:
  - A process-wide asyncio Semaphore (default: 2 concurrent requests)
  - A 429-aware wrapper that retries once after the Retry-After period
  - Proper error mapping: upstream 429 → client 429 (not 502)
"""

import asyncio
import logging
import os

import httpx
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Max concurrent Open-Meteo requests across ALL overlay routes.
# Env-overridable so we can bump it on a paid tier without redeploying.
_MAX_CONCURRENT = int(os.getenv("OPEN_METEO_MAX_CONCURRENT", "2"))
_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    """Lazy-init so the semaphore lives on the running event loop."""
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    return _semaphore


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
    sem = _get_semaphore()

    for attempt in range(2):
        async with sem:
            r = await client.get(url, params=params)

        if r.status_code == 200:
            return r

        if r.status_code == 429:
            if attempt == 0:
                # Respect Retry-After if provided, else wait 2s
                wait = float(r.headers.get("Retry-After", "2"))
                wait = min(wait, 10.0)  # cap sanity
                logger.info(f"[{label}] Open-Meteo 429 — retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue
            # Second 429 — propagate as 429 to client
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
