"""
Storm identity + catalog harmonization (dependency-light; stdlib only).

Owns two things:

1. The ALIAS TABLE (data/storm_aliases.json, built by
   scripts/build_alias_table.py from IBTrACS USA_ATCF_ID): canonical
   ATCF <-> SID identity for every named storm since 1980. api/routes.py
   re-exports these helpers; they live here so the offline test suite can
   import them without FastAPI.

2. harmonize_catalog() — "one storm, one story" for the sidebar catalog
   (docs/audits/CROSS_SURFACE_SCORE_AUDIT_2026-07-10.md found 62% of
   sidebar storms telling a different severity story than their own hero
   card). For storms baked into the compiled bundle, the sidebar's
   peak_dps/dps_label are REPLACED by the hero's engine score, so the two
   surfaces agree by construction. Remaining estimate rows are re-banded
   through the canonical scheme, and SID+ATCF duplicate rows are collapsed.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_ALIAS_PATH = Path(__file__).resolve().parent.parent / "data" / "storm_aliases.json"
_ALIAS_TABLE: Optional[dict] = None

ID_FORM_RE = re.compile(r"^(?:[A-Z]{2}\d{6}|\d{7}[NS]\d{5})$", re.IGNORECASE)


def alias_table() -> dict:
    global _ALIAS_TABLE
    if _ALIAS_TABLE is None:
        try:
            _ALIAS_TABLE = json.loads(_ALIAS_PATH.read_text(encoding="utf-8"))
        except Exception as e:  # missing/corrupt table = feature off, not fatal
            logger.warning(f"[alias] storm_aliases.json unavailable: {e}")
            _ALIAS_TABLE = {}
    return _ALIAS_TABLE


def storm_identity(storm_id: str) -> dict:
    """Canonical {sid, atcf, name, year, basin} for any id form, or {}."""
    t = alias_table()
    if not t:
        return {}
    s = (storm_id or "").strip().upper()
    sid = t.get("by_atcf", {}).get(s)
    if sid is None and s in t.get("storms", {}):
        sid = s
    if sid is None:
        return {}
    meta = t.get("storms", {}).get(sid) or {}
    return {"sid": sid, "atcf": meta.get("atcf"), "name": meta.get("name"),
            "year": meta.get("year"), "basin": meta.get("basin")}


def _bundle_entry(storm_id: str, bundle_storms: dict, ident: dict) -> Optional[dict]:
    """The compiled bundle keys AL storms by ATCF and others by SID — try
    the requested id plus both alias forms."""
    for key in (storm_id, (storm_id or "").upper(),
                ident.get("atcf"), ident.get("sid")):
        if key and key in bundle_storms and isinstance(bundle_storms[key], dict):
            return bundle_storms[key]
    return None


def harmonize_catalog(rows: list, bundle_storms: dict) -> list:
    """Return a harmonized copy of the catalog rows (input never mutated).

    - Baked storms: peak_dps/dps_label overlaid from the bundle's engine
      score (rounded like the hero's big number; label is the bundle's own
      canonical dps_label). Marked score_source="engine".
    - Unbaked scored rows: label re-banded via core.dpi.categorize_dpi so
      the pre-canon "Minor"/"Catastrophic" vocabulary can never surface.
    - SID+ATCF twins collapsed onto one row (engine-scored row preferred,
      then any scored row) — the "two Sinlakus in the sidebar" fix.
    - Re-sorted by (-year, -peak_dps), the order the endpoint always used.

    Fail-open: any unexpected error returns the original rows.
    """
    try:
        from core.dpi import categorize_dpi
        out: list = []
        slot_by_key: dict = {}

        def rank(row: dict) -> tuple:
            return (row.get("score_source") == "engine",
                    row.get("peak_dps") is not None)

        for row in rows or []:
            r = dict(row)
            rid = str(r.get("id") or "")
            ident = storm_identity(rid)
            entry = _bundle_entry(rid, bundle_storms, ident)
            if entry and entry.get("dps") is not None:
                score = float(entry["dps"])
                r["peak_dps"] = int(round(score))
                r["dps_label"] = entry.get("dps_label") or categorize_dpi(score)
                r["score_source"] = "engine"
            elif r.get("peak_dps") is not None:
                r["dps_label"] = categorize_dpi(float(r["peak_dps"]))

            key = ident.get("sid") or rid.upper()
            slot = slot_by_key.get(key)
            if slot is None:
                slot_by_key[key] = len(out)
                out.append(r)
            elif rank(r) > rank(out[slot]):
                out[slot] = r

        out.sort(key=lambda s: (-(s.get("year") or 0), -(s.get("peak_dps") or 0)))
        return out
    except Exception:
        logger.exception("[catalog] harmonize failed — serving raw catalog")
        return rows
