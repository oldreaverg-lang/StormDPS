"""Per-storm IBTrACS lookups stream the cached CSV instead of loading it as text.

2026-09-24 buffer census: every live-storm hourly refresh read the 315 MB
full archive into a str and wrapped it in io.StringIO (~1.6 GB transient),
and two finished get_storm_track frames were found pinning 2 x 315 MB.
"""
import asyncio
import csv

import pytest

from services.noaa_client import NOAAClient

COLS = ["SID", "SEASON", "BASIN", "NAME", "ISO_TIME", "LAT", "LON", "USA_ATCF_ID",
        "USA_WIND", "USA_PRES", "WMO_WIND", "WMO_PRES", "STORM_SPEED", "STORM_DIR", "USA_RMW"]


def _rows(sid, season, name, atcf, n=3):
    return [{"SID": sid, "SEASON": str(season), "BASIN": "NA", "NAME": name,
             "ISO_TIME": f"{season}-09-0{i + 1} 00:00:00", "LAT": f"{20 + i}.0",
             "LON": f"{-80 - i}.0", "USA_ATCF_ID": atcf, "USA_WIND": "80", "USA_PRES": "970",
             "WMO_WIND": "", "WMO_PRES": "", "STORM_SPEED": "10", "STORM_DIR": "300",
             "USA_RMW": "20"} for i in range(n)]


def _write(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        w.writerow({c: "" for c in COLS})          # IBTrACS units row
        w.writerows(rows)


@pytest.fixture
def client(tmp_path, monkeypatch):
    _write(tmp_path / "ibtracs_recent.csv",
           _rows("2024280N20270", 2024, "MILTON", "AL142024")
           + _rows("2025250N20270", 2025, "TESTNAME", "AL092025"))
    _write(tmp_path / "ibtracs_all.csv",
           _rows("2005236N23285", 2005, "KATRINA", "AL122005")
           + _rows("1990200N20270", 1990, "TESTNAME", "AL051990")
           + _rows("2024280N20270", 2024, "MILTON", "AL142024"))

    async def _no_text(*a, **k):
        raise AssertionError("the CSV must be streamed from disk, not loaded as text")

    c = NOAAClient(cache_dir=str(tmp_path))
    monkeypatch.setattr(c, "_fetch_ibtracs", _no_text)
    return c


def test_sid_lookup_streams_recent_then_archive(client):
    assert len(asyncio.run(client.get_ibtracs_track("2024280N20270"))) == 3
    assert len(asyncio.run(client.get_ibtracs_track("2005236N23285"))) == 3   # archive fallback


def test_atcf_lookup_and_full_archive_gate(client):
    assert len(asyncio.run(client.get_ibtracs_by_atcf_id("AL142024", full_archive=False))) == 3
    assert len(asyncio.run(client.get_ibtracs_by_atcf_id("AL122005"))) == 3
    # a current-window id never pays for the full-archive scan
    assert asyncio.run(client.get_ibtracs_by_atcf_id("AL122005", full_archive=False)) == []


def test_name_lookups(client):
    assert len(asyncio.run(client.get_ibtracs_by_name_year("Milton", 2024))) == 3
    latest = asyncio.run(client.get_ibtracs_by_name("TestName"))
    assert len(latest) == 3 and all(s.timestamp.year == 2025 for s in latest)
