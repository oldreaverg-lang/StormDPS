"""Active-storm feed freshness (2026-09-24).

- Rows carry the advisory time of their position (NHC lastUpdate / JTWC
  warning-position time), so a newer intermediate advisory is recognised as
  newer instead of as a "frozen feed".
- NHC and JTWC refresh independently: one failing source keeps its previous
  rows, never freezes the other or drops its own storms.
- Selfcheck check 7 pages only when /active is genuinely OLDER than the
  advisory (or the cache stopped refreshing), and reports the cache age.
"""
import asyncio
from datetime import datetime, timedelta, timezone

import api.routes as routes
from services.noaa_client import NOAAClient


def _row(sid, src, lat, lon, when=None):
    return {"id": sid, "name": sid, "classification": "HU", "lat": lat, "lon": lon,
            "source": src, "last_update_utc": when}


# ── advisory times on the rows ────────────────────────────────────────────
def test_nhc_rows_carry_last_update(monkeypatch):
    class _Resp:
        def raise_for_status(self): pass
        def json(self):
            return {"activeStorms": [{"id": "ep172026", "name": "Polo", "classification": "HU",
                                      "latitudeNumeric": 16.7, "longitudeNumeric": -104.0,
                                      "intensity": 140, "lastUpdate": "2026-09-24T12:00:00.000Z"}]}

    async def _fake(self, *a, **k):
        return _Resp()

    monkeypatch.setattr(NOAAClient, "_request_with_retry", _fake)
    rows = asyncio.run(NOAAClient()._fetch_nhc_active_storms())
    assert rows[0]["last_update_utc"] == "2026-09-24T12:00:00.000Z"


def test_jtwc_rows_carry_warning_position_time():
    from services.jtwc_client import JTWCClient
    now = datetime.now(timezone.utc)
    text = (f"1. TROPICAL STORM 25W (SURIGAE) WARNING NR 012\n"
            f"   WARNING POSITION:\n   {now:%d}0600Z --- NEAR 18.9N 133.1E\n"
            f"   MOVEMENT PAST SIX HOURS - 300 DEGREES AT 10 KTS\n"
            f"   MAX SUSTAINED WINDS - 045 KT, GUSTS 055 KT\n")
    row = JTWCClient()._parse_warning_text(text, {"id": "WP252026", "basin": "WP", "name": "SURIGAE"})
    assert row["last_update_utc"].startswith(f"{now:%Y-%m-%d}T06:00")


# ── per-source refresh ────────────────────────────────────────────────────
def _patch_sources(monkeypatch, nhc, jtwc, jtwc_down=False):
    async def _nhc(self):
        if isinstance(nhc, Exception):
            raise nhc
        return nhc

    async def _jtwc(self):
        if isinstance(jtwc, Exception):
            raise jtwc
        return jtwc

    from services.jtwc_client import JTWCClient
    monkeypatch.setattr(NOAAClient, "_fetch_nhc_active_storms", _nhc)
    monkeypatch.setattr(NOAAClient, "_fetch_jtwc_active_storms", _jtwc)
    monkeypatch.setattr(JTWCClient, "jtwc_is_down", classmethod(lambda cls: jtwc_down))


PREV = [_row("ep172026", "NHC", 16.5, -103.5), _row("WP252026", "JTWC", 18.0, 134.0)]


def test_nhc_failure_keeps_previous_nhc_rows(monkeypatch):
    _patch_sources(monkeypatch, RuntimeError("nhc down"), [_row("WP252026", "JTWC", 18.9, 133.1)])
    rows, ok = asyncio.run(routes._fetch_active_merged(None, PREV))
    by = {r["id"]: r for r in rows}
    assert ok and set(by) == {"ep172026", "WP252026"}
    assert by["ep172026"]["lat"] == 16.5 and by["WP252026"]["lat"] == 18.9


def test_slow_jtwc_does_not_freeze_nhc(monkeypatch):
    _patch_sources(monkeypatch, [_row("ep172026", "NHC", 16.7, -104.0)], asyncio.TimeoutError())
    rows, ok = asyncio.run(routes._fetch_active_merged(None, PREV))
    by = {r["id"]: r for r in rows}
    assert ok and by["ep172026"]["lat"] == 16.7 and by["WP252026"]["lat"] == 18.0


def test_jtwc_marked_down_is_a_failure_not_an_empty_basin(monkeypatch):
    _patch_sources(monkeypatch, [_row("ep172026", "NHC", 16.7, -104.0)], [], jtwc_down=True)
    rows, _ = asyncio.run(routes._fetch_active_merged(None, PREV))
    assert "WP252026" in {r["id"] for r in rows}


def test_both_sources_down_reports_not_ok(monkeypatch):
    _patch_sources(monkeypatch, RuntimeError("x"), RuntimeError("y"))
    rows, ok = asyncio.run(routes._fetch_active_merged(None, PREV))
    assert not ok and len(rows) == 2


# ── selfcheck check 7 (judgement lives in live_position.py: stdlib-only) ──
import live_position as lp  # noqa: E402


def _now():
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def test_intermediate_advisory_ahead_of_tau0_is_not_a_freeze():
    now = _now()
    tau0 = {"lat": 16.5, "lon": -103.5, "valid_time_utc": (now - timedelta(hours=3)).isoformat()}
    act = _row("ep172026", "NHC", 16.7, -104.0, now.isoformat())          # 58 km ahead, 3 h newer
    r = lp.compare_row(act, tau0, now=now)
    assert r["diverge_km"] > 50 and r["active_lag_h"] == -3.0
    assert lp.row_failures(r) == []


def test_active_older_than_advisory_and_diverged_pages():
    now = _now()
    tau0 = {"lat": 16.7, "lon": -104.0, "valid_time_utc": now.isoformat()}
    act = _row("ep172026", "NHC", 16.0, -102.5, (now - timedelta(hours=6)).isoformat())
    fails = lp.row_failures(lp.compare_row(act, tau0, now=now))
    assert any("older than the advisory" in f for f in fails)


def test_row_without_advisory_time_keeps_distance_test():
    now = _now()
    tau0 = {"lat": 16.7, "lon": -104.0, "valid_time_utc": now.isoformat()}
    fails = lp.row_failures(lp.compare_row(_row("ep172026", "NHC", 16.0, -102.5), tau0, now=now))
    assert any("km from the fresh advisory" in f for f in fails)


def test_cache_age_uses_naive_utc_stamp_and_pages_when_stale():
    now = datetime.now(timezone.utc)
    fresh = (now - timedelta(minutes=5)).replace(tzinfo=None)     # timeutil.utcnow() style
    age, msg = lp.cache_age_failure(fresh, 3, now)
    assert age == 5.0 and msg is None                              # was always None before
    age, msg = lp.cache_age_failure((now - timedelta(hours=4)).replace(tzinfo=None), 3, now)
    assert age == 240.0 and "not refreshed for 240 min" in msg
    assert lp.cache_age_failure((now - timedelta(hours=4)).replace(tzinfo=None), 0, now)[1] is None
