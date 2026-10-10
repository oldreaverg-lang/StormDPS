"""Recorded damage for storms the compiled bundle does not carry.

data/recorded_damage.csv is joined into the bundle at bake time
(build_actual_impact.py), so a storm outside the bundle had no "Recorded
damage" anywhere: Opal 1995, the obvious comparison for Isaias 2026 (same
coast, same week of October), showed a dash on the compare page. A row whose
storm_id is not a bundle key is served straight from the CSV by the /dps
identity overlay, in the same shape the bake writes.

Stdlib only.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Optional

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "recorded_damage.csv"

_cache: dict = {"mtime": None, "rows": {}}


def fmt_usd_billions(b: Optional[float]) -> Optional[str]:
    """Same display rule as build_actual_impact._fmt_usd_billions."""
    if not b:
        return None
    if b >= 1.0:
        return f"${b:.0f}B" if b >= 10 else f"${b:.1f}B"
    return f"${b * 1000:.0f}M"


def parse_rows(rows: Iterable[dict]) -> dict:
    """{STORM_ID: {damage_billions, source, note}} for the usable rows."""
    out: dict = {}
    for row in rows:
        sid = str(row.get("storm_id") or "").strip().upper()
        if not sid:
            continue
        try:
            dmg = float(row.get("damage_billions_usd") or 0)
        except (TypeError, ValueError):
            continue
        if dmg <= 0:
            continue
        out[sid] = {
            "damage_billions": dmg,
            "source": str(row.get("source") or "").strip(),
            "note": str(row.get("note") or "").strip(),
        }
    return out


def _rows(path: Path = CSV_PATH) -> dict:
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return {}
    if _cache["mtime"] != (str(path), mtime):
        with open(path, encoding="utf-8", newline="") as f:
            _cache["rows"] = parse_rows(csv.DictReader(f))
        _cache["mtime"] = (str(path), mtime)
    return _cache["rows"]


def impact_from(rec: dict) -> dict:
    """An actual_impact block shaped like the bake's recorded-damage branch."""
    display = fmt_usd_billions(rec["damage_billions"])
    if rec.get("source") == "est.":
        display = f"~{display} (est.)"
    return {
        "damage_billions": rec["damage_billions"],
        "damage_display": display,
        "damage_source": rec.get("source") or None,
        "sources": ["StormDPS recorded-damage dataset"],
    }


def lookup(ids: Iterable[Optional[str]], path: Path = CSV_PATH) -> Optional[dict]:
    """actual_impact for the first id form with a row, else None."""
    rows = _rows(path)
    for sid in ids:
        rec = rows.get(str(sid or "").strip().upper())
        if rec:
            return impact_from(rec)
    return None
