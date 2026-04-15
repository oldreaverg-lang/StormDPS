#!/usr/bin/env python3
"""
Batch-fetch IKE data for historical storms via the local API.

Hits GET /api/v1/storms/{storm_id}/track for each storm,
which triggers the server to fetch from IBTrACS/HURDAT2, compute IKE,
and cache the results to data/cache/ike/.

Covers both:
  * Atlantic basin (AL…) — original scope
  * Western Pacific basin (WP…) — added to support JTWC storms
    such as Typhoon Sinlaku (WP262025).

After running this, run:
    python build_preload.py --all
    python compile_cache.py

Usage:
    python batch_fetch_storms.py              # all basins
    python batch_fetch_storms.py --basin WP   # Western Pacific only
    python batch_fetch_storms.py --basin AL   # Atlantic only
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

API_BASE = "http://localhost:8000/api/v1"
CACHE_DIR = Path(__file__).parent / "data" / "cache" / "ike"

# All Atlantic storm IDs from 2015-2025 catalog
ATLANTIC_STORM_IDS = [
    # 2015
    "AL012015", "AL022015", "AL032015", "AL042015", "AL052015",
    "AL062015", "AL072015", "AL082015", "AL102015", "AL112015", "AL122015",
    # 2016
    "AL012016", "AL022016", "AL032016", "AL042016", "AL052016",
    "AL062016", "AL072016", "AL092016", "AL102016", "AL112016",
    "AL122016", "AL132016", "AL142016", "AL152016", "AL162016",
    # 2017
    "AL012017", "AL022017", "AL032017", "AL052017", "AL062017",
    "AL072017", "AL082017", "AL092017", "AL112017", "AL122017",
    "AL132017", "AL142017", "AL152017", "AL162017", "AL172017",
    "AL182017", "AL192017",
    # 2018
    "AL012018", "AL022018", "AL032018", "AL042018", "AL052018",
    "AL062018", "AL072018", "AL082018", "AL092018", "AL102018",
    "AL122018", "AL132018", "AL142018", "AL152018", "AL162018",
    # 2019
    "AL012019", "AL022019", "AL042019", "AL052019", "AL062019",
    "AL072019", "AL082019", "AL092019", "AL102019", "AL112019",
    "AL122019", "AL132019", "AL142019", "AL162019", "AL172019",
    "AL182019", "AL192019", "AL202019",
    # 2020
    "AL012020", "AL022020", "AL032020", "AL042020", "AL052020",
    "AL062020", "AL072020", "AL082020", "AL092020", "AL112020",
    "AL122020", "AL132020", "AL142020", "AL152020", "AL162020",
    "AL172020", "AL182020", "AL192020", "AL202020", "AL212020",
    "AL222020", "AL232020", "AL242020", "AL252020", "AL262020",
    "AL272020", "AL282020", "AL292020", "AL302020", "AL312020",
    # 2021
    "AL012021", "AL022021", "AL032021", "AL052021", "AL062021",
    "AL072021", "AL082021", "AL092021", "AL102021", "AL122021",
    "AL132021", "AL142021", "AL152021", "AL162021", "AL172021",
    "AL182021", "AL192021", "AL202021", "AL212021", "AL222021",
    "AL232021",
    # 2022
    "AL012022", "AL022022", "AL032022", "AL042022", "AL052022",
    "AL062022", "AL072022", "AL082022", "AL092022", "AL132022",
    "AL142022", "AL152022", "AL162022", "AL172022",
    # 2023
    "AL012023", "AL022023", "AL032023", "AL042023", "AL052023",
    "AL062023", "AL072023", "AL082023", "AL092023", "AL102023",
    "AL112023", "AL122023", "AL132023", "AL142023", "AL152023",
    "AL162023", "AL172023", "AL182023", "AL202023",
    # 2024
    "AL012024", "AL022024", "AL032024", "AL042024", "AL052024",
    "AL062024", "AL072024", "AL082024", "AL092024", "AL102024",
    "AL112024", "AL122024", "AL132024", "AL142024", "AL152024",
    "AL162024", "AL172024", "AL182024",
    # 2025
    "AL012025", "AL022025", "AL032025", "AL042025", "AL052025",
    "AL062025", "AL072025", "AL082025", "AL092025", "AL102025",
    "AL112025", "AL122025", "AL132025", "AL142025", "AL152025",
    "AL162025", "AL172025", "AL182025", "AL192025", "AL202025",
    "AL212025", "AL222025", "AL232025", "AL242025", "AL252025",
    "AL262025", "AL272025", "AL282025", "AL292025", "AL302025",
    "AL312025", "AL322025", "AL332025",
]


# Western Pacific storm IDs (JTWC-tracked) worth pre-caching.
# Covers notable recent typhoons + current-season WP storms. Storm numbers
# follow the JTWC convention (NN = sequential basin index, YYYY = year).
# Typhoon Sinlaku (WP262025) is explicitly included so the live pipeline
# has historical context to attach when it appears on the active feed.
WESTERN_PACIFIC_STORM_IDS = [
    # 2013
    "WP302013",  # Haiyan (Yolanda)
    # 2018
    "WP262018",  # Yutu
    "WP312018",  # Mangkhut
    # 2020
    "WP222020",  # Goni (record-strength landfall)
    "WP232020",  # Vamco
    # 2021
    "WP242021",  # Rai (Odette)
    # 2022
    "WP032022",  # Malakas
    "WP162022",  # Nanmadol
    "WP252022",  # Nalgae
    # 2023
    "WP022023",  # Mawar
    "WP072023",  # Doksuri
    "WP092023",  # Khanun
    "WP142023",  # Saola
    # 2024
    "WP012024", "WP022024", "WP032024", "WP042024", "WP052024",
    "WP062024", "WP072024", "WP082024", "WP092024", "WP102024",
    "WP112024",  # Yagi (devastating landfall in Vietnam/China)
    "WP122024", "WP132024", "WP142024", "WP152024", "WP162024",
    "WP172024", "WP182024", "WP192024", "WP202024", "WP212024",
    "WP222024", "WP232024", "WP242024", "WP252024",
    # 2025
    "WP012025", "WP022025", "WP032025", "WP042025", "WP052025",
    "WP062025", "WP072025", "WP082025", "WP092025", "WP102025",
    "WP112025", "WP122025", "WP132025", "WP142025", "WP152025",
    "WP162025", "WP172025", "WP182025", "WP192025", "WP202025",
    "WP212025", "WP222025", "WP232025", "WP242025", "WP252025",
    "WP262025", "WP272025", "WP282025", "WP292025", "WP302025",
    # 2026 (current season — fill in as storms develop)
    "WP012026", "WP022026", "WP032026",
    "WP042026",  # Sinlaku (active as of April 2026)
    "WP052026", "WP062026", "WP072026", "WP082026", "WP092026", "WP102026",
]

# Master per-basin catalog so main() can select subsets cleanly.
STORM_IDS_BY_BASIN = {
    "AL": ATLANTIC_STORM_IDS,
    "WP": WESTERN_PACIFIC_STORM_IDS,
}


def is_cached(storm_id: str) -> bool:
    """Check if IKE cache file already exists for this storm."""
    matches = list(CACHE_DIR.glob(f"{storm_id}_*.json"))
    return len(matches) > 0


def fetch_storm(storm_id: str) -> tuple[str, bool, str]:
    """Fetch IKE data for a storm via API. Returns (storm_id, success, message)."""
    url = f"{API_BASE}/storms/{storm_id}/track"
    try:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            count = len(data) if isinstance(data, list) else 0
            return (storm_id, True, f"{count} snapshots")
    except urllib.error.HTTPError as e:
        return (storm_id, False, f"HTTP {e.code}: {e.reason}")
    except urllib.error.URLError as e:
        return (storm_id, False, f"URL error: {e.reason}")
    except Exception as e:
        return (storm_id, False, str(e))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--basin",
        choices=sorted(STORM_IDS_BY_BASIN.keys()) + ["ALL"],
        default="ALL",
        help="Which basin to fetch (default: ALL)",
    )
    args = parser.parse_args()

    if args.basin == "ALL":
        storm_ids = [sid for ids in STORM_IDS_BY_BASIN.values() for sid in ids]
        label = f"{len(storm_ids)} storms across all basins"
    else:
        storm_ids = STORM_IDS_BY_BASIN[args.basin]
        label = f"{len(storm_ids)} {args.basin} storms"

    print(f"Batch-fetching IKE data for {label}")
    print(f"API: {API_BASE}")
    print(f"Cache: {CACHE_DIR}")
    print()

    # Check which storms are already cached
    to_fetch = []
    already_cached = []
    for sid in storm_ids:
        if is_cached(sid):
            already_cached.append(sid)
        else:
            to_fetch.append(sid)

    print(f"Already cached: {len(already_cached)}")
    print(f"Need to fetch: {len(to_fetch)}")
    print()

    if not to_fetch:
        print("All storms already cached!")
        return

    # Fetch in batches of 3 (parallel) to not overload the server
    success = 0
    failed = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_storm, sid): sid for sid in to_fetch}
        done = 0
        total = len(to_fetch)

        for future in as_completed(futures):
            done += 1
            storm_id, ok, msg = future.result()
            status = "OK" if ok else "FAIL"
            pct = done / total * 100
            print(f"  [{done}/{total} {pct:.0f}%] {storm_id}: {status} - {msg}")

            if ok:
                success += 1
            else:
                failed.append((storm_id, msg))

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Success: {success}/{len(to_fetch)}")

    if failed:
        print(f"\nFailed ({len(failed)}):")
        for sid, msg in failed:
            print(f"  {sid}: {msg}")


if __name__ == "__main__":
    main()
