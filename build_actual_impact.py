#!/usr/bin/env python3
"""
Enrich the compiled bundle with OBSERVED impact data (ground truth) and report
the DPS-vs-damage validation correlation.

Sources (per product decision — curated + FEMA only; NCEI dropped as its damage
figures undercount and its name matching is unreliable):
  - historical_storms_db.csv  -> curated headline damage ($B), validation_target
                                 (the 24-storm CALIBRATION set — wins on conflict
                                 so scoring-audit numbers never drift)
  - data/recorded_damage.csv  -> broader curated headline damage keyed by the
                                 bundle's own storm ids (NCEI billion-dollar
                                 events, NHC TCRs, documented international
                                 totals; "est." rows are early estimates)
  - FEMA OpenFEMA             -> counties declared, states, major-disaster flag
  - fallback                  -> completed ATLANTIC storms with zero detected
                                 landfalls and no damage row get an explicit
                                 "None recorded (no landfall)" so the compare
                                 page can distinguish "no damage" from unknown

For every storm already in frontend/compiled_bundle.json we attach an
``actual_impact`` block joined by (name, year). Does NOT recompute DPS, so the
scoring golden-master is untouched. Re-runnable; safe after a rebake.

Usage:
    python build_actual_impact.py --validate            # print correlation, write nothing
    python build_actual_impact.py --dry-run             # print sample enrichment
    python build_actual_impact.py                       # enrich bundle in place
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
from pathlib import Path
from datetime import datetime, timezone

from services.fema_client import FEMAClient

ROOT = Path(__file__).parent
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"
HIST_CSV = ROOT / "historical_storms_db.csv"
DAMAGE_CSV = ROOT / "data" / "recorded_damage.csv"
CACHE_DIR = ROOT / "data" / "cache" / "observed"


def _fmt_usd_billions(b):
    if not b:
        return None
    if b >= 1.0:
        return f"${b:.0f}B" if b >= 10 else f"${b:.1f}B"
    return f"${b * 1000:.0f}M"


def load_curated() -> dict[tuple[str, int], dict]:
    """(NAME_UPPER, year) -> {damage_billions, validation_target}."""
    out: dict[tuple[str, int], dict] = {}
    if not HIST_CSV.exists():
        return out
    with open(HIST_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("name") or "").strip().upper()
            try:
                year = int(row.get("year") or 0)
            except ValueError:
                continue
            if not name or not year:
                continue
            try:
                dmg = float(row.get("damage_billions") or 0) or None
            except ValueError:
                dmg = None
            out[(name, year)] = {
                "damage_billions": dmg,
                "validation_target": (row.get("validation_target") or "").strip().lower() == "true",
            }
    return out


def load_recorded_damage() -> dict[str, dict]:
    """storm_id (bundle key) -> {damage_billions, source, note}."""
    out: dict[str, dict] = {}
    if not DAMAGE_CSV.exists():
        return out
    with open(DAMAGE_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sid = (row.get("storm_id") or "").strip()
            if not sid:
                continue
            try:
                dmg = float(row.get("damage_billions_usd") or 0) or None
            except ValueError:
                dmg = None
            if dmg is None:
                continue
            out[sid] = {
                "damage_billions": dmg,
                "source": (row.get("source") or "").strip(),
                "note": (row.get("note") or "").strip(),
            }
    return out


async def gather_impact(targets: dict[str, dict]) -> dict[str, dict]:
    """targets: {storm_id: {"name","year","basin","no_landfall"}} -> {storm_id: actual_impact}."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    curated = load_curated()
    recorded = load_recorded_damage()
    fema = FEMAClient(cache_dir=CACHE_DIR)
    await fema.load()

    current_year = datetime.now(timezone.utc).year
    out: dict[str, dict] = {}
    for sid, meta in targets.items():
        name, year = meta.get("name"), meta.get("year")
        if not name or not year:
            continue
        key = (name.upper(), year)
        cur = curated.get(key)
        rec = recorded.get(sid)
        dec = fema.get_declaration(name, year)
        # Atlantic storms whose track never produced a landfall event AND
        # have no damage row: label "None recorded". Gates:
        #  - ATLANTIC only — the landfall detector's coastal boxes are
        #    authoritative there; an empty list for a WP storm may just be
        #    missing coverage.
        #  - season closed a FULL year — post-season damage reports settle
        #    slowly, and the detector's boxes stop south of Atlantic Canada
        #    (Teddy 2020 / Lee 2023 landfalled NS with landfalls == []), so
        #    recent "no landfall" is too weak a signal to assert "none".
        #  - the display string deliberately claims only what the DATASET
        #    shows ("None recorded"), never "no landfall/no damage happened".
        no_damage_fallback = (
            not cur and not rec
            and meta.get("basin") == "ATLANTIC"
            and meta.get("no_landfall")
            and year < current_year - 1
        )
        if not cur and not rec and not dec and not no_damage_fallback:
            continue

        impact = {"sources": [], "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d")}
        if cur and cur.get("damage_billions"):
            impact["damage_billions"] = cur["damage_billions"]
            impact["damage_display"] = _fmt_usd_billions(cur["damage_billions"])
            impact["sources"].append("StormDPS curated dataset")
        elif rec:
            impact["damage_billions"] = rec["damage_billions"]
            display = _fmt_usd_billions(rec["damage_billions"])
            if rec.get("source") == "est.":
                display = f"~{display} (est.)"
            impact["damage_display"] = display
            impact["damage_source"] = rec.get("source") or None
            impact["sources"].append("StormDPS recorded-damage dataset")
        elif no_damage_fallback:
            impact["damage_display"] = "None recorded"
            impact["damage_none_reason"] = "no landfall event on tracked path"
            impact["sources"].append("StormDPS track analysis")
        if dec:
            impact.update({
                "counties_declared": dec.get("counties_declared") or None,
                "states_declared": dec.get("states") or None,
                "major_disaster": dec.get("major_disaster"),
                "individual_assistance": dec.get("individual_assistance"),
                "earliest_declaration": dec.get("earliest_declaration"),
            })
            impact["sources"].append("FEMA OpenFEMA")
        out[sid] = impact
    return out


def _pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return cov / math.sqrt(vx * vy)


def _spearman(xs, ys):
    def ranks(vals):
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        r = [0.0] * len(vals)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r
    return _pearson(ranks(xs), ranks(ys))


def _load_fema_cache() -> dict[tuple[str, int], dict]:
    """Read the cached FEMA index directly (no network) for offline validation."""
    p = CACHE_DIR / "fema_tropical_declarations.json"
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    out = {}
    for k, v in raw.items():
        try:
            nm, yr = k.split("|")
            out[(nm.upper(), int(yr))] = v
        except ValueError:
            continue
    return out


def validate(bundle):
    """Join bundle DPS with curated damage; report correlation across the set."""
    storms = bundle.get("storms", {})
    curated = load_curated()
    rows = []
    for sid, s in storms.items():
        name, year, dps = s.get("name"), s.get("year"), s.get("dps")
        if not name or not year or dps is None:
            continue
        cur = curated.get((name.upper(), year))
        if not cur or not cur.get("damage_billions"):
            continue
        rows.append((name, year, float(dps), float(cur["damage_billions"]), cur["validation_target"]))

    if not rows:
        print("No DPS↔damage overlap found between the bundle and the curated DB.")
        return

    rows.sort(key=lambda r: -r[2])
    print(f"{'Storm':<14}{'Year':>6}{'DPS':>7}{'Damage $B':>12}")
    print("-" * 39)
    for name, year, dps, dmg, vt in rows:
        print(f"{name:<14}{year:>6}{dps:>7.0f}{dmg:>11.0f}B")

    xs = [r[2] for r in rows]
    ys = [r[3] for r in rows]
    pr = _pearson(xs, ys)
    sr = _spearman(xs, ys)
    print("-" * 39)
    print(f"n={len(rows)}   Pearson r={pr:+.3f}   Spearman rho={sr:+.3f}")
    print("(rho is the honest one for a 'does higher DPS mean worse storm' claim - it's rank-based.)")

    # Secondary axis: DPS vs FEMA counties-declared (footprint, not dollars).
    fema = _load_fema_cache()
    crows = []
    for sid, s in storms.items():
        name, year, dps = s.get("name"), s.get("year"), s.get("dps")
        if not name or not year or dps is None:
            continue
        dec = fema.get((name.upper(), year))
        if not dec or not dec.get("counties_declared"):
            continue
        crows.append((name, year, float(dps), int(dec["counties_declared"])))
    if len(crows) >= 2:
        cx = [r[2] for r in crows]
        cy = [r[3] for r in crows]
        print(f"\nDPS vs FEMA counties-declared:  n={len(crows)}  "
              f"Pearson r={_pearson(cx, cy):+.3f}  Spearman rho={_spearman(cx, cy):+.3f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--storms", help="comma-separated storm_ids to limit enrichment to")
    ap.add_argument("--dry-run", action="store_true", help="print sample enrichment, write nothing")
    ap.add_argument("--validate", action="store_true", help="print DPS↔damage correlation, write nothing")
    ap.add_argument("--bundle", default=str(BUNDLE))
    args = ap.parse_args()

    bundle_path = Path(args.bundle)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    storms = bundle.get("storms", {})

    if args.validate:
        validate(bundle)
        return

    only = set(s.strip() for s in args.storms.split(",")) if args.storms else None
    targets = {
        sid: {
            "name": s.get("name"), "year": s.get("year"),
            "basin": s.get("basin"),
            "no_landfall": not (s.get("landfalls") or []),
        }
        for sid, s in storms.items()
        if (only is None or sid in only) and s.get("name") and s.get("year")
    }
    print(f"Looking up observed impact for {len(targets)} storm(s)...")
    impact = asyncio.run(gather_impact(targets))
    print(f"Matched ground-truth for {len(impact)} / {len(targets)} storm(s)\n")

    show = (only or sorted(impact, key=lambda k: -(impact[k].get('damage_billions') or 0)))
    for sid in list(show)[:25]:
        if sid not in impact:
            print(f"  {sid}: no match")
            continue
        imp = impact[sid]
        nm = targets[sid]["name"]
        print(f"  {sid} {nm}: damage={imp.get('damage_display')} "
              f"counties={imp.get('counties_declared')} major={imp.get('major_disaster')} "
              f"sources={imp.get('sources')}")

    if args.dry_run:
        print("\n[dry-run] bundle not modified.")
        return
    # Self-cleaning: a storm evaluated this run but no longer qualifying
    # (tightened gate, removed damage row) must lose its stale block too —
    # otherwise re-runs only ever grow the enrichment.
    removed = 0
    for sid in targets:
        if sid not in impact and storms[sid].pop("actual_impact", None) is not None:
            removed += 1
    for sid, imp in impact.items():
        storms[sid]["actual_impact"] = imp
    bundle_path.write_text(json.dumps(bundle, separators=(",", ":")), encoding="utf-8")
    print(f"\nWrote actual_impact for {len(impact)} storms "
          f"(removed {removed} stale) -> {bundle_path}")


if __name__ == "__main__":
    main()
