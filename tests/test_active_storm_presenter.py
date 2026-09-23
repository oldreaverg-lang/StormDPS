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
