"""
The stall banner warns of inland flooding, so it must only fire when a slow or
stalling forecast segment is near a coast — and only call it "Harvey-like" for a
genuine multi-day stall. Polo EP172026 showed "EXTREME ... Harvey-like" from a
9-hour slowdown ~250 km off Guerrero because the old test used wide coastal boxes.
"""
from api.routes import _compute_stall_risk, _stall_near_land


def _track(points):
    return [{"hour": h, "lat": la, "lon": lo} for h, la, lo in points]


# NHC forecast for Polo, 2026-09-23 — slow at first, never within ~200 km of land.
POLO = _track([(0, 14.7, -101.3), (9, 15.0, -101.6), (21, 15.9, -102.8),
               (33, 16.4, -104.3), (45, 16.6, -106.1), (57, 16.8, -108.0),
               (69, 17.0, -110.0), (93, 18.2, -112.7), (117, 20.9, -113.7)])

# Fay AL062026 — near-stall in the open mid-Atlantic.
FAY = _track([(0, 28.0, -38.0), (33, 29.4, -41.6), (45, 29.5, -43.1),
              (57, 29.5, -44.3), (69, 30.0, -46.0), (93, 30.2, -46.5)])

# Crawling ~30-60 km off Acapulco for 36 h (serious, not multi-day).
ACAPULCO = _track([(0, 16.30, -99.70), (12, 16.45, -99.80),
                   (24, 16.55, -99.85), (36, 16.60, -99.90)])

# Three-day stall on the upper Texas coast (the Harvey case).
HOUSTON = _track([(0, 28.0, -92.0), (24, 29.0, -93.0),
                  (48, 29.5, -93.5), (72, 29.7, -94.0)])


def test_polo_offshore_crawl_is_suppressed():
    r = _compute_stall_risk(POLO)
    assert r["risk_level"] == "none" and r["risk_score"] == 0
    assert r["segments"], "segment speeds kept for the map's track coloring"


def test_open_ocean_stall_is_suppressed():
    assert _compute_stall_risk(FAY)["risk_level"] == "none"


def test_stall_just_off_acapulco_still_warns_without_harvey_label():
    r = _compute_stall_risk(ACAPULCO)
    assert r["risk_level"] in ("moderate", "high", "extreme")
    assert "Harvey" not in r["description"]


def test_multi_day_coastal_stall_gets_harvey_label():
    r = _compute_stall_risk(HOUSTON)
    assert r["risk_level"] == "extreme"
    assert r["stall_hours"] >= 48
    assert "Harvey-like" in r["description"]


def test_near_land_distances():
    assert _stall_near_land(16.60, -99.90)        # ~30 km off Acapulco
    assert not _stall_near_land(15.0, -101.6)     # ~290 km off Guerrero
    assert _stall_near_land(21.0, -157.5)         # Kaiwi Channel, Hawaii
    assert not _stall_near_land(30.0, -45.0)      # mid-Atlantic
