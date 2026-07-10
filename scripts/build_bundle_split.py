#!/usr/bin/env python3
"""
Bundle diet (PERF_AUDIT_2026-07 §4 item 5): split compiled_bundle.json into
a slim eager index + per-storm detail files.

The monolith is 3.56 MB raw and was parsed on every first visit; 88% of it
is `raw_snapshots` (per-storm track data used only when THAT storm is
opened) and most of the rest is per-storm heavy fields (dpi_timeseries,
actual_impact, rainfall_text, landfalls, ground_truth) that are likewise
only needed at storm-open. This script derives:

  frontend/bundle_index.json          ~90 KB raw (~25 KB gzipped) — every
      storm's LIGHT fields (name/year/basin/dps/dps_label/category/peaks…)
      plus `detail_ids`, the set of ids that have a detail file. Loaded
      eagerly by the SPA; powers preset/accordion chips, name lookups and
      the static-deploy catalog fallback.
  frontend/bundle_storm/<ID>.json     median ~12 KB — {"storm": <full
      entry>, "snapshots": <raw_snapshots[id]>}. Fetched when a storm is
      opened; fills the SPA's COMPILED_SCORES + PRELOAD_CACHE exactly as
      the monolith did.

PURELY DERIVED — reads compiled_bundle.json, never recomputes a score, so
it is scoring-neutral by construction. Self-cleaning: detail files whose id
left the bundle are deleted on re-run.

CEREMONY: run this after every bake (compile_cache.py → build_actual_impact
→ THIS; scripts/rebake.py and the auto-rebake workflow both chain it) and
bump BUNDLE_VERSION in index.html — the SPA requests both artifacts with
?v=<BUNDLE_VERSION>, served under the standard /frontend static cache
headers (.json → 5-min TTL; only the monolith's explicit route is
immutable). The monolith stays in place: the server (seo.py, analogs,
catalog harmonize) and older cached frontends keep reading it.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"
INDEX_OUT = ROOT / "frontend" / "bundle_index.json"
DETAIL_DIR = ROOT / "frontend" / "bundle_storm"

# Per-storm fields that only matter once the storm is OPEN. Everything else
# rides in the index. (Weights from the 2026-07-10 audit: dpi_timeseries
# 245 KB, actual_impact 32 KB, rainfall_text 24 KB, landfalls 22 KB,
# ground_truth 5 KB — vs ~90 KB for all the light fields combined.)
HEAVY_FIELDS = ("dpi_timeseries", "actual_impact", "rainfall_text",
                "landfalls", "ground_truth")


def build(dry_run: bool = False) -> dict:
    bundle = json.loads(BUNDLE.read_text(encoding="utf-8"))
    storms = bundle.get("storms", {})
    raw = bundle.get("raw_snapshots", {})
    detail_ids = sorted(set(storms) | set(raw))

    index = {
        "version": bundle.get("version"),
        "compiled_at": bundle.get("compiled_at"),
        "storm_count": bundle.get("storm_count", len(storms)),
        "split": 1,
        "detail_ids": detail_ids,
        "storms": {
            sid: {k: v for k, v in entry.items() if k not in HEAVY_FIELDS}
            for sid, entry in storms.items()
        },
    }

    stats = {"details": len(detail_ids), "removed": 0,
             "index_kb": len(json.dumps(index, separators=(",", ":"))) // 1024}
    if dry_run:
        return stats

    DETAIL_DIR.mkdir(exist_ok=True)
    wanted = set()
    for sid in detail_ids:
        payload = {"storm": storms.get(sid), "snapshots": raw.get(sid, [])}
        fname = f"{sid}.json"
        wanted.add(fname)
        (DETAIL_DIR / fname).write_text(
            json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    # self-cleaning: ids that left the bundle lose their detail file
    for old in DETAIL_DIR.glob("*.json"):
        if old.name not in wanted:
            old.unlink()
            stats["removed"] += 1

    INDEX_OUT.write_text(json.dumps(index, separators=(",", ":")),
                         encoding="utf-8")
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    stats = build(dry_run=args.dry_run)
    print(f"index: {stats['index_kb']} KB | detail files: {stats['details']}"
          f" | stale removed: {stats['removed']}"
          f"{'  [dry-run: nothing written]' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
