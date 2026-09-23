"""Active-storm sidebar rows are display-ready (present_active_storms).

2026-09-23 sidebar: JTWC's unnamed North Indian system showed as "ONE",
a West Pacific name as "SURIGAE", and every chip wore a SLOW badge. The
presenter names numbered JTWC systems by designation, title-cases JTWC
names, and flags near_land so the STALL badge can't fire over open ocean.
"""
import re
from pathlib import Path

import pytest

from api.routes import _jtwc_designation, present_active_storms

ROOT = Path(__file__).resolve().parent.parent


def _row(sid, name, lat, lon, **kw):
    return dict(id=sid, name=name, classification="TS", lat=lat, lon=lon, **kw)


def test_names():
    rows = present_active_storms([
        _row("IO012026", "ONE", 17.8, 83.7),
        _row("WP252026", "SURIGAE", 16.5, 137.6),
        _row("WP262026", "FUNG-WONG", 15.0, 130.0),
        _row("EP152026", "Fifteen-E", 14.5, -154.4),
        _row("EP162026", "Odalys", 16.0, -127.9),
    ])
    assert [r["name"] for r in rows] == ["01B", "Surigae", "Fung-Wong", "Fifteen-E", "Odalys"]


@pytest.mark.parametrize("sid,lon,want", [
    ("WP252026", 137.6, "25W"),
    ("IO012026", 83.7, "01B"),    # Bay of Bengal
    ("IO022026", 65.0, "02A"),    # Arabian Sea
    ("SH122026", 60.0, "12S"),    # South Indian
    ("SH132026", 160.0, "13P"),   # South Pacific
    ("AL092026", -70.0, None),
])
def test_jtwc_designation(sid, lon, want):
    assert _jtwc_designation(sid, lon) == want


def test_near_land():
    rows = present_active_storms([
        _row("IO012026", "ONE", 17.8, 83.7),       # off Visakhapatnam
        _row("EP162026", "Odalys", 16.0, -127.9),  # ~1,100 km offshore
        _row("EP992026", "Nolan", None, None),
    ])
    assert [r["near_land"] for r in rows] == [True, False, None]


def test_input_rows_not_mutated():
    src = [_row("WP252026", "SURIGAE", 16.5, 137.6)]
    present_active_storms(src)
    assert src[0]["name"] == "SURIGAE" and "near_land" not in src[0]


def test_storms_index_uses_lifetime_category():
    import json
    bundle = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))
    html = (ROOT / "frontend" / "storms.html").read_text(encoding="utf-8")
    rows = dict(re.findall(r'href="/storm/([^"]+)">[^<]*</a><span class="bs">[^<]*· ([^<]+)</span>', html))
    wrong = []
    for sid, s in bundle["storms"].items():
        cat = s.get("category_lifetime") or s.get("category")
        want = f"Cat {cat}" if isinstance(cat, int) and cat > 0 else "TS/TD"
        if rows.get(sid) != want:
            wrong.append((sid, s.get("name"), rows.get(sid), want))
    assert not wrong, wrong[:10]


def test_most_destructive_first(monkeypatch):
    # Homepage auto-loads row 0: it opened Odalys (DPS 14) over Polo (DPS 85).
    import api.routes as r
    scores = {"EP162026": 14.4, "EP172026": 85.4, "EP152026": None}
    monkeypatch.setattr(r, "_active_dps", lambda sid: scores.get(sid.upper()))
    rows = present_active_storms([
        _row("ep162026", "Odalys", 16.0, -127.9, intensity_knots=65),
        _row("ep172026", "Polo", 15.6, -102.1, intensity_knots=125),
        _row("ep152026", "Fifteen-E", 14.5, -154.4, intensity_knots=30),
        _row("WP252026", "SURIGAE", 16.5, 137.6, intensity_knots=45),
    ])
    assert [x["name"] for x in rows] == ["Polo", "Odalys", "Surigae", "Fifteen-E"]
    assert rows[0]["dps"] == 85.4 and rows[2]["dps"] is None


def test_scale_anchor_ids_are_scored_storms():
    # The "for scale" chips (frontend SCALE_ANCHORS) read scores from the
    # compiled bundle; an id that drops out of a rebake would silently vanish.
    import json
    html = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    block = re.search(r"const SCALE_ANCHORS = \[(.*?)\];", html, re.S).group(1)
    ids = re.findall(r"'([A-Z]{2}\d{6})'", block)
    storms = json.loads((ROOT / "frontend" / "compiled_bundle.json").read_text(encoding="utf-8"))["storms"]
    assert len(ids) >= 15
    assert [i for i in ids if i not in storms] == []
    scores = [storms[i]["dps"] for i in ids]
    assert scores == sorted(scores), "keep the list in ascending score order for readability"


def test_malformed_rows_never_500():
    # 2026-09-23: a new feed row 500'd /storms/active for every storm. Odd
    # types must degrade per row, and an invalid row is dropped, not fatal.
    from api.routes import _active_summaries
    rows = [
        _row("SH012027", "TWO", "-14.2", "65.3", intensity_knots="35"),    # numeric strings
        dict(id="IO022026", name=None, classification="TS", lat=None, lon=None),
        dict(id="WP262026", name="KONG", classification=None, lat=10.0, lon=130.0),  # invalid raw
        _row("EP172026", "Polo", 15.6, -102.1, intensity_knots=125),
    ]
    rows_out = present_active_storms(rows)
    assert len(rows_out) == 4
    out = _active_summaries(rows)
    ids = [s.id for s in out]
    assert "EP172026" in ids and "SH012027" in ids and "IO022026" in ids
    assert "WP262026" not in ids   # classification=None can't be a StormSummary


def test_presenter_exception_serves_raw(monkeypatch):
    import api.routes as r
    monkeypatch.setattr(r, "present_active_storms", lambda s: (_ for _ in ()).throw(RuntimeError("boom")))
    out = r._active_summaries([_row("EP172026", "Polo", 15.6, -102.1)])
    assert [s.name for s in out] == ["Polo"]
