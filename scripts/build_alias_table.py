#!/usr/bin/env python3
"""
Build data/storm_aliases.json — the ATCF <-> SID identity map.

This is roadmap item #2 in docs/DATA_ARCHITECTURE.md: "the alias table is
ground truth" for storm identity. IBTrACS carries the USA agency id
(USA_ATCF_ID, e.g. AL142018) alongside its own SID (2018280N18273); every
dual-id bug on the site (raw ids shown as names, split cache entries,
catalog duplicates) comes from not having this mapping at hand.

Output shape (compact on purpose — loaded at API startup):
{
  "built": "2026-07-09", "source": "ibtracs.since1980.list.v04r01.csv",
  "min_year": 1980,
  "by_atcf": {"AL142018": "2018280N18273", ...},
  "storms":  {"2018280N18273": {"atcf": "AL142018", "name": "Michael",
              "year": 2018, "basin": "NA"}, ...}
}

Only NAMED storms are included (matching the catalog's filter); a storm can
carry several ATCF ids across agency renumbering — each maps to the same SID,
and `storms[sid]["atcf"]` holds the first one seen (the primary).

Usage (the IBTrACS CSV is ~136 MB — download it once, point --csv at it):
    python scripts/build_alias_table.py --csv path/to/ibtracs.since1980.list.v04r01.csv
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DEFAULT = ROOT / "data" / "storm_aliases.json"

SKIP_NAMES = {"", "NOT_NAMED", "UNNAMED"}


def build(csv_path: Path, min_year: int) -> dict:
    by_atcf: dict[str, str] = {}
    storms: dict[str, dict] = {}
    with open(csv_path, encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = (row.get("SID") or "").strip()
            if not sid:  # header units row and blanks
                continue
            try:
                year = int((row.get("SEASON") or "").strip())
            except ValueError:
                continue
            if year < min_year:
                continue
            name = (row.get("NAME") or "").strip()
            if name.upper() in SKIP_NAMES:
                continue
            atcf = (row.get("USA_ATCF_ID") or "").strip().upper()
            entry = storms.setdefault(sid, {
                "atcf": None, "name": name.title(), "year": year,
                "basin": (row.get("BASIN") or "").strip(),
            })
            if atcf and len(atcf) == 8:
                if entry["atcf"] is None:
                    entry["atcf"] = atcf
                # every distinct agency id resolves to this SID
                by_atcf.setdefault(atcf, sid)
    return {
        "built": date.today().isoformat(),
        "source": csv_path.name,
        "min_year": min_year,
        "by_atcf": by_atcf,
        "storms": storms,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, help="path to ibtracs.since1980.list.v04r01.csv")
    ap.add_argument("--min-year", type=int, default=1980)
    ap.add_argument("--out", default=str(OUT_DEFAULT))
    args = ap.parse_args()

    table = build(Path(args.csv), args.min_year)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(table, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {out} — {len(table['storms'])} storms, "
          f"{len(table['by_atcf'])} atcf ids, {out.stat().st_size // 1024} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
