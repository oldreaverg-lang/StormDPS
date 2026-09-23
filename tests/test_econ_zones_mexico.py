"""
Mexico Pacific-coast economic-exposure coverage for the ERS.

Before this, econ_zones.json's only Mexican zone was Cancun (Caribbean side), so
every East Pacific storm threatening Acapulco, Manzanillo, Puerto Vallarta or
Los Cabos scored the 0.05 "Open Ocean / Uncharted" floor (Polo 2026 showed ERS 3
as a Cat 5 paralleling the Guerrero coast). These lock in the coverage and keep
the new boxes from contaminating other basins' matches.
"""
from core.ike import get_economic_exposure, _ECON_ZONES

_MEXICO_PACIFIC = (
    "Oaxaca Coast / Huatulco (MX)",
    "Acapulco (MX)",
    "Zihuatanejo / Lazaro Cardenas (MX)",
    "Manzanillo / Colima Coast (MX)",
    "Puerto Vallarta / Banderas Bay (MX)",
    "Mazatlan (MX)",
    "Los Cabos (MX)",
    "La Paz (MX)",
)


def _boxes():
    return {z[0]: z for z in _ECON_ZONES}


def test_mexico_pacific_zones_present():
    names = set(_boxes())
    for z in _MEXICO_PACIFIC:
        assert z in names, f"missing econ zone: {z}"


def test_acapulco_direct_hit_is_high_exposure():
    e = get_economic_exposure(16.85, -99.88, use_nri=False)
    assert e["name"] == "Acapulco (MX)"
    assert e["exposure"] >= 0.5


def test_otis_style_approach_matches_acapulco():
    # Just south of the Acapulco box (Otis 2023's final approach): the wind-field
    # reach must pick up the city instead of the open-ocean floor.
    e = get_economic_exposure(16.35, -99.85, use_nri=False, r34_nm=60)
    assert e["name"] == "Acapulco (MX)"
    assert e["exposure"] > 0.05


def test_los_cabos_direct_hit():
    e = get_economic_exposure(22.95, -109.8, use_nri=False)
    assert e["name"] == "Los Cabos (MX)"


def test_open_east_pacific_still_uncharted():
    # Well west of Mexico: no zone within reach.
    e = get_economic_exposure(15.0, -115.0, use_nri=False, r34_nm=100)
    assert e["name"] == "Open Ocean / Uncharted"


def test_gulf_side_point_not_captured_by_pacific_zones():
    # Veracruz sits on the Gulf side; the Oaxaca box is ~170 nm away across the
    # isthmus, beyond the 90 nm maximum reach.
    e = get_economic_exposure(19.2, -96.1, use_nri=False, r34_nm=150)
    assert not e["name"].endswith("(MX)")


def test_mexico_boxes_do_not_overlap_any_other_zone():
    boxes = _boxes()
    for mx in _MEXICO_PACIFIC:
        _, _, _, _, a_lat0, a_lat1, a_lon0, a_lon1 = boxes[mx]
        for other, (_, _, _, _, b_lat0, b_lat1, b_lon0, b_lon1) in boxes.items():
            if other == mx:
                continue
            overlap = (a_lat0 < b_lat1 and b_lat0 < a_lat1 and
                       a_lon0 < b_lon1 and b_lon0 < a_lon1)
            assert not overlap, f"{mx} overlaps {other}"
