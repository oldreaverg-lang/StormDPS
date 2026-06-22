#!/usr/bin/env python3
"""Pre-warm the persistent-volume track-data caches for every catalog storm.

The frontend renders a preset storm by fetching its track and then POSTing that
track to /sst/track, /rainfall/track and /observed/track. Those endpoints cache
their result on the Railway volume keyed by storm_id + a track *fingerprint*, so
the FIRST viewer of a given storm pays a cold fetch (~7s for SST) and everyone
after gets a ~200ms cache hit.

This script removes that first-viewer cost: it walks the SIDs baked into
frontend/compiled_bundle.json and, against the LIVE API, replicates the exact
frontend flow per storm —

    GET  /storms/{sid}/track?grid_resolution_km=15&skip_points=0   (the canonical
         track the frontend posts; also warms the IKE/track cache)
    POST /sst/track?storm_id={sid}        with those points
    POST /rainfall/track?storm_id={sid}   with those points
    POST /observed/track?storm_id={sid}   with those points

Because it posts the identical points, it produces the SAME fingerprint the
frontend will, so real visitors land on a warm cache. It's idempotent — already
warm storms just return fast — so re-run it after the bundle changes
(e.g. after a compile-cache-bake) to warm any new storms.

Note: this only reaches the public API (the server does the upstream ERDDAP /
Open-Meteo / CO-OPS / NDBC fetching), so it runs from anywhere with internet —
no local access to those data sources needed.

Usage:
    python prewarm_catalog.py [--base URL] [--workers N] [--limit N] [--only SID]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_BASE = "https://stormdps.com/api/v1"
BUNDLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "compiled_bundle.json")
# Must match the frontend's preset track fetch so the cache fingerprint lines up.
TRACK_QS = "grid_resolution_km=15&skip_points=0"


def _get(url: str, timeout: float = 120.0):
    req = urllib.request.Request(url, headers={"User-Agent": "stormdps-prewarm/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _post(url: str, payload, timeout: float = 180.0):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json", "User-Agent": "stormdps-prewarm/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def warm_one(base: str, sid: str):
    """Warm track + SST + rainfall + observed caches for one storm."""
    t0 = time.time()
    try:
        track = _get(f"{base}/storms/{sid}/track?{TRACK_QS}")
    except Exception as e:  # network/HTTP/JSON
        return sid, "track-fail", str(e)[:80], 0.0
    if not isinstance(track, list) or not track:
        return sid, "no-track", "", time.time() - t0

    # Replicate the frontend: sort by timestamp, keep lat/lon/timestamp only.
    pts = sorted(
        ({"lat": s.get("lat"), "lon": s.get("lon"), "timestamp": s.get("timestamp")} for s in track),
        key=lambda p: p["timestamp"] or "",
    )

    counts = {}
    for layer in ("sst", "rainfall", "observed"):
        try:
            r = _post(f"{base}/{layer}/track?storm_id={sid}", pts)
            counts[layer] = len(r) if isinstance(r, list) else 0
        except Exception as e:
            counts[layer] = f"ERR:{type(e).__name__}"
    return sid, "ok", {"pts": len(pts), **counts}, time.time() - t0


def main() -> int:
    ap = argparse.ArgumentParser(description="Pre-warm catalog track-data caches.")
    ap.add_argument("--base", default=DEFAULT_BASE, help="API base URL")
    ap.add_argument("--workers", type=int, default=3, help="concurrent storms (keep low; SST is heavy)")
    ap.add_argument("--limit", type=int, default=0, help="only the first N storms (0 = all)")
    ap.add_argument("--only", default="", help="warm a single SID (debug)")
    args = ap.parse_args()

    with open(BUNDLE, encoding="utf-8") as f:
        bundle = json.load(f)
    storms = bundle.get("storms", bundle)
    sids = list(storms.keys()) if isinstance(storms, dict) else [s.get("id") for s in storms]
    sids = [s for s in sids if s]
    if args.only:
        sids = [s for s in sids if s == args.only]
    if args.limit:
        sids = sids[: args.limit]

    print(f"[prewarm] warming {len(sids)} catalog storms via {args.base} (workers={args.workers})", flush=True)
    t0 = time.time()
    done = ok = 0
    failures = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {ex.submit(warm_one, args.base, sid): sid for sid in sids}
        for fut in as_completed(futs):
            sid, status, info, dt = fut.result()
            done += 1
            if status == "ok":
                ok += 1
                print(f"[{done}/{len(sids)}] {sid} ok {info} {dt:.1f}s", flush=True)
            else:
                failures.append((sid, status, info))
                print(f"[{done}/{len(sids)}] {sid} {status} {info}", flush=True)

    print(f"[prewarm] done: {ok}/{len(sids)} warmed in {time.time() - t0:.0f}s", flush=True)
    if failures:
        print(f"[prewarm] {len(failures)} not warmed (re-run to retry): "
              + ", ".join(f"{s}:{st}" for s, st, _ in failures[:20]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
