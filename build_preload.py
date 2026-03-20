#!/usr/bin/env python3
"""
Build the static preload bundle for the Hurricane IKE Visualizer.

Reads cached IKE results from data/cache/ike/ and assembles them into
a single JSON file (frontend/preload_bundle.json) that the frontend
loads on startup for instant storm visualization.

Usage:
    python build_preload.py          # Build from existing cache
    python build_preload.py --serve  # Build then start the server

The bundle ships with the code so even ephemeral hosting platforms
(Heroku, Vercel, Railway) have all preset storm data on first deploy.
"""

import json
import os
import sys
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "data" / "cache" / "ike"
OUTPUT_PATH = Path(__file__).parent / "frontend" / "preload_bundle.json"
CACHE_VERSION = "v2"

# Must match PRESET_STORM_IDS in api/routes.py and PRESETS in index.html
PRESET_STORM_IDS = [
    "AL122005",  # Katrina
    "AL092024",  # Helene
    "AL152017",  # Maria
    "AL112017",  # Irma
    "AL092022",  # Ian
    "AL142018",  # Michael
    "AL142024",  # Milton
    "AL182012",  # Sandy
    "AL092008",  # Ike
    "AL092017",  # Harvey
    "AL052019",  # Dorian
    "AL062018",  # Florence
    "AL102023",  # Idalia
    "AL022024",  # Beryl
]


def build_bundle(include_all: bool = False) -> dict:
    """
    Build the preload bundle from cached IKE files.

    Args:
        include_all: If True, include ALL cached storms (not just presets).

    Returns:
        Bundle dict with storm data.
    """
    bundle = {"version": CACHE_VERSION, "storms": {}}

    if not CACHE_DIR.exists():
        print(f"Cache directory not found: {CACHE_DIR}")
        return bundle

    # Determine which storms to include
    if include_all:
        storm_ids = set()
        for f in CACHE_DIR.glob("*.json"):
            parts = f.stem.rsplit("_", 1)
            if len(parts) == 2:
                storm_ids.add(parts[0])
        storm_ids = sorted(storm_ids)
    else:
        storm_ids = PRESET_STORM_IDS

    for sid in storm_ids:
        matches = sorted(CACHE_DIR.glob(f"{sid}_*.json"))
        if not matches:
            print(f"  MISSING: {sid} (no cache file)")
            continue

        try:
            with open(matches[0]) as f:
                data = json.load(f)
            if data.get("_version") != CACHE_VERSION:
                print(f"  STALE: {sid} (version {data.get('_version')} != {CACHE_VERSION})")
                continue
            results = data.get("results", [])
            if results:
                bundle["storms"][sid] = results
                print(f"  {sid}: {len(results)} snapshots")
            else:
                print(f"  EMPTY: {sid} (no results)")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"  ERROR: {sid}: {e}")

    bundle["storm_count"] = len(bundle["storms"])
    return bundle


def main():
    print("Building preload bundle...")
    print(f"Cache dir: {CACHE_DIR}")
    print(f"Output: {OUTPUT_PATH}")
    print()

    include_all = "--all" in sys.argv
    bundle = build_bundle(include_all=include_all)

    # Write compact JSON
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(bundle, f, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\nBundle written: {OUTPUT_PATH}")
    print(f"Storms: {bundle['storm_count']}")
    print(f"Size: {size_kb:.1f} KB")

    missing = [sid for sid in PRESET_STORM_IDS if sid not in bundle["storms"]]
    if missing:
        print(f"\nWARNING: {len(missing)} preset storms missing from cache:")
        for sid in missing:
            print(f"  - {sid}")
        print("Run the server and hit POST /api/v1/preload/generate to compute them.")

    if "--serve" in sys.argv:
        print("\nStarting server...")
        os.execvp("uvicorn", ["uvicorn", "main:app", "--reload", "--port", "8000"])


if __name__ == "__main__":
    main()
