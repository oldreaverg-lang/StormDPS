"""The landfall record behind the compare page's landfall rows.

Peak wind is a lifetime figure, often reached far offshore: Opal 1995 peaked
at 130 kt in the central Gulf and came ashore at Pensacola Beach at 100 kt.
The /dps payload's `landfalls` list is scoring machinery (coastal-box events
at the peak wind inside the box), so the display reads the best track itself.
"""
import asyncio
import json
from datetime import datetime, timezone

import pytest

import api.routes as routes
from core import landfall_record as lr

HEADER = ("SID,SEASON,NUMBER,BASIN,SUBBASIN,NAME,ISO_TIME,NATURE,LAT,LON,WMO_WIND,WMO_PRES,"
          "TRACK_TYPE,DIST2LAND,LANDFALL,USA_ATCF_ID,USA_RECORD,USA_WIND,USA_PRES")
UNITS = " ,Year, , , , , , ,degrees_north,degrees_east,kts,mb, ,km,km, , ,kts,mb"
OPAL, OPAL_ATCF = "1995271N19273", "AL171995"


def _row(sid, time, lat, lon, wind, pres, d2l, record=" ", atcf="", track="main", name="X", wmo=" "):
    return ",".join(str(x) for x in (
        sid, time[:4], 1, "NA", "GM", name, time, "TS", lat, lon, wmo, " ",
        track, d2l, 0, atcf, record, wind, pres))


OPAL_ROWS = [
    _row(OPAL, "1995-09-28 00:00:00", 19.4, -87.5, 25, 1004, 0, name="OPAL"),              # formed over Yucatan
    _row(OPAL, "1995-10-04 10:00:00", 26.6, -88.8, 130, 916, 269, "I", OPAL_ATCF, name="OPAL"),
    _row(OPAL, "1995-10-04 18:00:00", 29.0, -87.7, 110, 938, 110, " ", OPAL_ATCF, name="OPAL"),
    _row(OPAL, "1995-10-04 22:00:00", 30.3, -87.1, 100, 942, 9, "L", OPAL_ATCF, name="OPAL"),
    _row(OPAL, "1995-10-05 00:00:00", 31.0, -86.8, 80, 950, 0, " ", OPAL_ATCF, name="OPAL"),
]


def _dicts(lines):
    cols = HEADER.split(",")
    return [dict(zip(cols, ln.split(","))) for ln in lines]


def _csv(tmp_path, name, lines):
    fp = tmp_path / name
    fp.write_text("\n".join([HEADER, UNITS] + lines) + "\n", encoding="utf-8")
    return fp


# ── official records ────────────────────────────────────────────────────────

def test_the_agency_landfall_record_is_used_as_published():
    res = lr.from_ibtracs(_dicts(OPAL_ROWS))
    assert res["source"] == "official"
    assert res["landfalls"] == [{
        "time_utc": "1995-10-04T22:00:00Z", "lat": 30.3, "lon": -87.1,
        "wind_kt": 100, "pressure_mb": 942, "kind": "official"}]
    # Not the lifetime peak (130 kt, far offshore), and not the fix after it.
    assert lr.strongest(res["landfalls"])["wind_kt"] == 100


def test_spur_tracks_and_unusable_rows_are_ignored():
    rows = _dicts(OPAL_ROWS + [
        _row(OPAL, "1995-10-04 23:00:00", 30.6, -87.0, 140, 900, 0, "L", track="spur"),
        _row(OPAL, "not a time", 30.6, -87.0, 140, 900, 0, "L"),
        _row(OPAL, "1995-10-04 23:30:00", "", -87.0, 140, 900, 0, "L"),
    ])
    assert [x["wind_kt"] for x in lr.from_ibtracs(rows)["landfalls"]] == [100]


def test_wind_falls_back_to_the_wmo_agency_value():
    pts = lr.points_from_ibtracs(_dicts([
        _row("S", "2020-05-20 06:00:00", 20.9, 88.0, " ", " ", 70, wmo=85)]))
    assert pts[0]["wind"] == 85 and pts[0]["pres"] is None


# ── coast crossings (agencies that publish no landfall record) ──────────────

def _wp(d2ls, winds=(140, 150, 130, 90)):
    times = ("2013-11-07 15:00:00", "2013-11-07 18:00:00", "2013-11-07 21:00:00", "2013-11-08 00:00:00")
    lons = (128.6, 127.5, 126.4, 125.3)
    return _dicts([_row("W", t, 10.8, lo, w, 900, d) for t, lo, w, d in zip(times, lons, winds, d2ls)])


def test_a_crossing_is_interpolated_to_the_coast():
    # 21Z fix is 60 km offshore, 00Z fix is over land, ~120 km further on.
    res = lr.from_ibtracs(_wp((300, 180, 60, 0)))
    assert res["source"] == "crossing" and len(res["landfalls"]) == 1
    lf = res["landfalls"][0]
    assert lf["kind"] == "crossing"
    assert lf["time_utc"] == "2013-11-07T22:30:00Z"          # half way along the leg, to 10 minutes
    assert 125.7 < lon_of(lf) < 126.0
    assert lf["wind_kt"] == 110                                # between the two fixes, not the peak


def lon_of(lf):
    return lf["lon"]


def test_leaving_land_and_forming_over_land_are_not_landfalls():
    assert lr.from_ibtracs(_wp((0, 0, 40, 200)))["landfalls"] == []      # land -> sea
    assert lr.from_ibtracs(_wp((0, 0, 0, 0)))["landfalls"] == []         # never at sea
    assert lr.from_ibtracs(_wp((300, 200, 100, 50)))["landfalls"] == []  # never ashore


def test_weak_crossings_and_gaps_in_the_record_are_skipped():
    assert lr.from_ibtracs(_wp((300, 180, 60, 0), winds=(20, 20, 20, 15)))["landfalls"] == []
    rows = _wp((300, 180, 60, 0))
    rows[3]["ISO_TIME"] = "2013-11-09 00:00:00"                           # 27 h after the last sea fix
    assert lr.from_ibtracs(rows)["landfalls"] == []


def test_a_storm_absent_from_the_archive_has_no_source():
    assert lr.from_ibtracs([]) == {"source": None, "landfalls": []}


# ── reading one storm from the CSV ──────────────────────────────────────────

def test_scan_reads_one_contiguous_block(tmp_path):
    fp = _csv(tmp_path, "ib.csv", [
        _row("1995200N10300", "1995-07-19 00:00:00", 10, -60, 30, 1005, 500, atcf="AL051995"),
        *OPAL_ROWS,
        _row("1995280N12320", "1995-10-07 00:00:00", 12, -40, 30, 1005, 900, atcf="AL181995"),
    ])
    rows = lr.scan_ibtracs(fp, sid=OPAL)
    assert len(rows) == len(OPAL_ROWS) and {r["SID"] for r in rows} == {OPAL}
    assert rows[3]["USA_RECORD"] == "L" and rows[3]["DIST2LAND"] == "9"
    # By ATCF id: the whole block, including the early fix that has no id yet.
    assert len(lr.scan_ibtracs(fp, atcf=OPAL_ATCF.lower())) == len(OPAL_ROWS)
    assert lr.scan_ibtracs(fp, sid="2099001N00000") == []
    assert lr.scan_ibtracs(fp, atcf="AL991995") == []
    assert lr.scan_ibtracs(fp) == []


# ── the estimate for a storm with no record yet ─────────────────────────────

COAST = [(30.40, -86.50, "Destin, FL"), (30.33, -87.14, "Pensacola Beach, FL"), (29.70, -85.00, "Apalachicola, FL")]


def _pt(day, hour, lat, lon, wind, pres=970):
    return {"t": datetime(2026, 10, day, hour, tzinfo=timezone.utc), "lat": lat, "lon": lon, "wind": wind, "pres": pres}


def test_the_closest_pass_to_the_coast_is_the_estimate():
    track = [_pt(9, 18, 28.5, -87.1, 105, 959), _pt(10, 0, 30.2, -86.6, 95, 967), _pt(10, 2, 30.5, -86.4, 85, 978)]
    est = lr.estimate(track, COAST)
    assert len(est) == 1
    lf = est[0]
    assert lf["kind"] == "estimate" and lf["place"] == "Destin, FL" and lf["distance_km"] < 10
    assert "2026-10-10T01:00:00Z" <= lf["time_utc"] <= "2026-10-10T01:40:00Z"
    assert 86 <= lf["wind_kt"] <= 92 and 970 <= lf["pressure_mb"] <= 976     # between the fixes either side
    assert lf["time_utc"].endswith("0:00Z")                                   # nearest 10 minutes


def test_a_storm_that_stays_offshore_has_no_estimate():
    # Parallel to the coast, 45 km out: near it, never at it.
    offshore = [_pt(9, 18, 30.0, -88.2, 90), _pt(10, 0, 30.0, -86.5, 90), _pt(10, 6, 30.0, -84.8, 90)]
    assert lr.estimate(offshore, COAST) == []
    far = [_pt(9, 18, 25.0, -90.0, 90), _pt(10, 0, 26.0, -90.0, 90)]
    assert lr.estimate(far, COAST) == []
    assert lr.estimate(far[:1], COAST) == [] and lr.estimate(offshore, []) == []


def test_two_separate_approaches_are_two_estimates():
    track = [_pt(8, 0, 28.5, -85.0, 60), _pt(8, 12, 29.7, -85.0, 55),      # over Apalachicola
             _pt(9, 0, 28.6, -86.0, 50), _pt(9, 12, 28.6, -86.6, 70),      # back out over the Gulf
             _pt(10, 0, 30.4, -86.5, 80)]                                   # into Destin
    est = lr.estimate(track, COAST)
    assert [x["place"] for x in est] == ["Apalachicola, FL", "Destin, FL"]
    assert lr.strongest(est)["place"] == "Destin, FL"


def test_strongest_prefers_the_higher_wind_then_the_earlier_time():
    a = {"time_utc": "2005-08-25T22:30:00Z", "wind_kt": 70}
    b = {"time_utc": "2005-08-29T11:10:00Z", "wind_kt": 110}
    c = {"time_utc": "2005-08-29T14:45:00Z", "wind_kt": 110}
    assert lr.strongest([a, b, c]) is b and lr.strongest([c, b]) is b
    assert lr.strongest([]) is None
    out = lr.summary("official", [c, a, b])
    assert out["count"] == 3 and out["strongest"] is b and "NHC" in out["note"]
    assert [x["wind_kt"] for x in out["landfalls"]] == [70, 110, 110]        # time order
    assert lr.summary(None, [])["note"] is None


# ── the endpoint ────────────────────────────────────────────────────────────

def _get(storm_id):
    resp = asyncio.run(routes.get_storm_landfall(storm_id))
    return json.loads(resp.body), resp.headers.get("cache-control")


@pytest.fixture
def volume(tmp_path, monkeypatch):
    (tmp_path / "cache").mkdir()
    monkeypatch.setattr(routes, "_PERSISTENT_DATA", tmp_path)
    monkeypatch.setattr(routes, "_LANDFALL_CACHE_DIR", tmp_path / "cache" / "landfall")
    monkeypatch.setattr(routes, "_active_storms_cache", [])
    monkeypatch.setattr(routes, "_landfall_live_memo", {})
    return tmp_path


def test_places_are_real_towns_then_regions():
    assert routes._landfall_place(30.3, -87.1) == "Pensacola Beach, FL"      # Opal
    assert routes._landfall_place(30.0, -83.7) == "FL Big Bend"              # no listed town in reach
    assert routes._landfall_place(10.84, 125.68) == "Philippines"
    assert routes._landfall_place(30.0, -50.0) is None                       # the page shows coordinates


def test_endpoint_serves_the_record_for_either_id_form_and_caches_it(volume):
    csv_fp = _csv(volume / "cache", "ibtracs_all.csv", OPAL_ROWS)
    for sid in (OPAL, OPAL_ATCF, OPAL_ATCF.lower()):
        d, cache = _get(sid)
        assert d["source"] == "official" and d["count"] == 1 and d["name"] == "Opal" and d["year"] == 1995
        assert d["strongest"]["wind_kt"] == 100 and d["strongest"]["pressure_mb"] == 942
        assert d["strongest"]["place"] == "Pensacola Beach, FL"
        assert "3600" in cache
    # Immutable history: served from the volume cache, no second archive scan.
    csv_fp.unlink()
    assert _get(OPAL)[0]["strongest"]["time_utc"] == "1995-10-04T22:00:00Z"


def test_an_archived_storm_that_cannot_be_looked_up_is_unknown_not_estimated(volume, monkeypatch):
    # No IBTrACS file on this volume. A cached track must not turn a 1995
    # storm into an "estimate" labelled as awaiting its official record.
    monkeypatch.setattr(routes, "_load_ike_cache", lambda *a, **k: [
        {"timestamp": "1995-10-04T18:00:00", "lat": 29.0, "lon": -87.7, "max_wind_ms": 56.0, "min_pressure_hpa": 938},
        {"timestamp": "1995-10-05T00:00:00", "lat": 31.0, "lon": -86.8, "max_wind_ms": 41.0, "min_pressure_hpa": 950},
    ])
    d, _ = _get(OPAL)
    assert d["source"] is None and d["count"] == 0 and d["strongest"] is None


def test_a_live_storm_gets_a_labelled_estimate_from_track_plus_advisory(volume, monkeypatch):
    monkeypatch.setattr(routes, "_active_storms_cache", [{
        "id": "al092026", "name": "Isaias", "lat": 30.5, "lon": -86.4,
        "intensity_knots": 85.0, "pressure_mb": 978.0, "last_update_utc": "2026-10-10T02:00:00.000Z"}])
    monkeypatch.setattr(routes, "_load_ike_cache", lambda sid, g, s, max_age_s=None: [
        {"timestamp": "2026-10-09T18:00:00+00:00", "lat": 28.5, "lon": -87.1, "max_wind_ms": 54.0, "min_pressure_hpa": 959},
        {"timestamp": "2026-10-10T00:00:00+00:00", "lat": 30.2, "lon": -86.6, "max_wind_ms": 48.9, "min_pressure_hpa": 967},
    ] if str(sid).upper() == "AL092026" else None)
    d, cache = _get("AL092026")
    assert d["source"] == "estimate" and "not published yet" in d["note"]
    assert d["name"] == "Isaias" and d["year"] == 2026
    top = d["strongest"]
    assert top["place"] == "Destin, FL" and top["kind"] == "estimate"
    assert "2026-10-10T01:00:00Z" <= top["time_utc"] <= "2026-10-10T01:40:00Z"
    assert 86 <= top["wind_kt"] <= 92                    # between the 00Z fix and the 02Z advisory
    assert "120" in cache                                 # short-lived: it moves with the track
    assert not (volume / "cache" / "landfall").exists()   # estimates are never written to the volume


def test_a_live_storm_still_at_sea_reads_none_so_far(volume, monkeypatch):
    monkeypatch.setattr(routes, "_active_storms_cache", [{"id": "ep182026", "name": "Rachel"}])
    monkeypatch.setattr(routes, "_load_ike_cache", lambda *a, **k: [
        {"timestamp": "2026-10-09T18:00:00+00:00", "lat": 23.0, "lon": -122.0, "max_wind_ms": 50.0},
        {"timestamp": "2026-10-10T00:00:00+00:00", "lat": 23.6, "lon": -123.7, "max_wind_ms": 50.0},
    ])
    d, _ = _get("ep182026")
    assert d["source"] == "estimate" and d["count"] == 0 and d["strongest"] is None


def test_the_endpoint_never_errors(volume, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("identity table unreadable")

    monkeypatch.setattr(routes, "_storm_identity", boom)
    d, _ = _get("AL171995")
    assert d == {"storm_id": "AL171995", "source": None, "note": None, "count": 0, "landfalls": [], "strongest": None}
