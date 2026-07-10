"""Offline validation of data/storm_aliases.json and data/recorded_damage.csv.

Pure data checks — no FastAPI import, so the suite stays runnable on the
dependency-light embedded interpreter.
"""
import csv
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parent.parent
ALIASES = ROOT / "data" / "storm_aliases.json"
DAMAGE = ROOT / "data" / "recorded_damage.csv"
BUNDLE = ROOT / "frontend" / "compiled_bundle.json"

ATCF_RE = re.compile(r"^[A-Z]{2}\d{6}$")
SID_RE = re.compile(r"^\d{7}[NS]\d{5}$")


def _table():
    return json.loads(ALIASES.read_text(encoding="utf-8"))


def test_alias_table_shape_and_size():
    t = _table()
    assert t["min_year"] == 1980
    assert len(t["storms"]) > 3000, "expect ~3.8k named storms since 1980"
    assert len(t["by_atcf"]) > 3000
    for atcf, sid in list(t["by_atcf"].items())[:200]:
        assert ATCF_RE.match(atcf), atcf
        assert sid in t["storms"], f"{atcf} -> dangling SID {sid}"


def test_alias_known_storms():
    t = _table()
    known = {
        "AL122005": ("Katrina", 2005),
        "AL142018": ("Michael", 2018),
        "AL092021": ("Ida", 2021),
        "AL182012": ("Sandy", 2012),
    }
    for atcf, (name, year) in known.items():
        sid = t["by_atcf"].get(atcf)
        assert sid, f"{atcf} missing from alias table"
        meta = t["storms"][sid]
        assert meta["name"] == name and meta["year"] == year, (atcf, meta)


def test_alias_covers_named_bundle_storms():
    t = _table()
    storms = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]
    unresolved = [
        sid for sid, s in storms.items()
        # raw-id "names" are unnamed systems — nothing to alias
        if not ATCF_RE.match(str(s.get("name") or ""))
        and sid not in t["by_atcf"] and sid not in t["storms"]
    ]
    # Known drift class: current-season ingest synthesizes SID-format ids
    # before IBTrACS assigns the official one (e.g. Ragasa 2025 is
    # 2025262N16133 in the bundle but 2025260N13138 in IBTrACS). Those
    # entries already carry proper names + damage in the bundle, so users
    # see nothing wrong — but the count must stay small; growth means the
    # ingest/bundle dedup is drifting.
    for sid in unresolved:
        assert not SID_RE.match(str(storms[sid].get("name") or "")), \
            f"{sid}: bundle name is a raw SID and no alias resolves it"
        assert storms[sid].get("name"), f"{sid}: unresolved AND nameless"
    assert len(unresolved) <= 10, f"synthesized-SID drift growing: {unresolved}"


def test_recorded_damage_rows_join_the_bundle():
    storms = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]
    with open(DAMAGE, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) >= 60
    for row in rows:
        sid = row["storm_id"]
        assert sid in storms, f"damage row {sid} ({row['name']}) not a bundle key"
        dmg = float(row["damage_billions_usd"])
        assert 0 < dmg < 400, f"{sid}: implausible damage {dmg}B"
        assert row["source"].strip(), f"{sid}: missing source tag"
        # name/year must agree with the bundle entry it decorates
        entry = storms[sid]
        assert int(row["year"]) == int(entry["year"]), (sid, row["year"], entry["year"])
        if not ATCF_RE.match(str(entry.get("name") or "")):
            assert row["name"].upper() == str(entry["name"]).upper(), (sid, row["name"], entry["name"])


def test_bundle_actual_impact_coverage():
    storms = json.loads(BUNDLE.read_text(encoding="utf-8"))["storms"]
    with_impact = [s for s in storms.values() if s.get("actual_impact")]
    with_display = [s for s in with_impact
                    if s["actual_impact"].get("damage_display")]
    assert len(with_impact) >= 150, "enrichment regression: expect 190-ish"
    assert len(with_display) >= 150
    # spot-check the two storms from the operator's compare screenshot
    assert storms["AL142018"]["actual_impact"]["damage_display"] == "$26B"
    assert storms["AL092021"]["actual_impact"]["damage_display"] == "$75B"
