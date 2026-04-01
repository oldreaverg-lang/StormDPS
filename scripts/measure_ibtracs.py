import asyncio
import time
from pathlib import Path

from services.noaa_client import NOAAClient

async def main():
    cache_dir = Path(__file__).resolve().parents[1] / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    start = time.time()
    async with NOAAClient(timeout=120.0, cache_dir=str(cache_dir)) as client:
        cat = await client.get_ibtracs_catalog(2015, 2026)
    elapsed = time.time() - start
    print(f"Loaded {len(cat)} storms in {elapsed:.1f}s")

if __name__ == "__main__":
    asyncio.run(main())
