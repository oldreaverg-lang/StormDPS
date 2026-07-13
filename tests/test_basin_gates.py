"""Basin-activation gate invariants for the WP (Tranche B) and SH living-legs
scoring (WP_DPS_AUDIT_V2, SH_DPS_AUDIT).

The whole safety argument for both tranches is that their coastal profiles are
UNREACHABLE outside a tight geographic window, so no other basin's baked score
can change. These tests lock that: NH coords never see an sh_* region, SH
coords never see a wp_* region, and adding the waypoints didn't perturb the
nearest-coast answer for any Atlantic/EP/US query.
"""
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import land_proximity as lp


def test_sh_window_excludes_northern_hemisphere():
    # Every baked storm is Northern-Hemisphere; none may resolve to an sh_*.
    for lat, lon in [(29.0, -95.0), (25.8, -80.2), (18.4, -66.1),   # Atlantic/US
                     (14.6, 121.0), (35.4, 139.8), (22.2, 114.1),   # WP
                     (15.0, 88.0), (0.0, 130.0)]:                    # NI / equator
        assert lp.nearest_sh_coast(lat, lon) is None, (lat, lon)


def test_wp_window_excludes_southern_hemisphere():
    for lat, lon in [(-19.8, 34.8), (-18.1, 178.4), (-38.7, 178.0), (-27.5, 114.0)]:
        assert lp.nearest_wp_coast(lat, lon) is None, (lat, lon)


def test_ni_window_excludes_other_basins():
    # NI must not fire for Atlantic/US, WP (lon > 97), SH (lat < 0), or far
    # north — every baked storm is one of those.
    for lat, lon in [(29.0, -95.0), (25.8, -80.2),      # Atlantic/US
                     (14.6, 121.0), (22.2, 114.1),       # WP (lon > 97)
                     (-19.8, 34.8),                       # SH
                     (35.0, 60.0)]:                       # too far north
        assert lp.nearest_ni_coast(lat, lon) is None, (lat, lon)


def test_ni_and_wp_do_not_both_claim_the_myanmar_overlap():
    # The 95-97 E band overlaps both windows; NI must own it (Myanmar coast),
    # and WP must still own the Gulf of Thailand just east of it.
    myanmar = lp.nearest_ni_coast(16.8, 96.2)   # Yangon
    assert myanmar is not None and myanmar[1].startswith("ni_")
    gulf_of_thailand = lp.nearest_wp_coast(13.4, 100.6)  # Bangkok/Gulf head
    assert gulf_of_thailand is not None and gulf_of_thailand[1].startswith("wp_")


def test_ni_resolves_north_indian_coasts():
    cases = [
        (22.0, 89.6, "ni_bangladesh"),          # Sundarbans
        (19.8, 85.8, "ni_odisha"),               # Puri (Fani)
        (17.7, 83.3, "ni_andhra"),               # Vizag (Hudhud)
        (16.3, 94.8, "ni_myanmar"),              # Irrawaddy (Nargis)
        (20.9, 70.4, "ni_gujarat_pakistan"),     # Diu (Tauktae)
        (17.0, 54.1, "ni_oman_yemen"),           # Salalah (Mekunu)
    ]
    for lat, lon, key in cases:
        hit = lp.nearest_ni_coast(lat, lon)
        assert hit is not None and hit[1] == key and hit[0] < 40, (lat, lon, hit)


def test_ni_waypoints_are_all_in_window():
    db = lp._get_coastline_db()
    assert db.ni_waypoints
    assert all(0 <= w.lat <= 31 and 42 <= w.lon <= 97 for w in db.ni_waypoints)
    assert all(w.region_key.startswith("ni_") for w in db.ni_waypoints)


def test_sh_resolves_southern_coasts():
    # (lat, lon, expected sh_* key)
    cases = [
        (-19.8, 34.8, "sh_mozambique"),   # Beira
        (-18.1, 178.4, "sh_fiji"),         # Suva
        (-17.7, 168.3, "sh_vanuatu"),      # Port Vila
        (-19.3, 146.8, "sh_e_australia"),  # Townsville
        (-38.7, 178.0, "sh_new_zealand"),  # Gisborne
        (-21.2, 48.3, "sh_madagascar"),    # Mananjary
    ]
    for lat, lon, key in cases:
        hit = lp.nearest_sh_coast(lat, lon)
        assert hit is not None and hit[1] == key and hit[0] < 40, (lat, lon, hit)


def test_empty_coast_low_population_discriminator():
    # The empty-Pilbara vs dense-Beira population split is what keeps a Cat 5
    # on an unpopulated coast (Ilsa) from reading like Beira/Suva.
    pilbara = lp.nearest_sh_coast(-19.6, 120.0)
    beira = lp.nearest_sh_coast(-19.8, 34.8)
    assert pilbara[2] <= 0.10 < 0.50 <= beira[2]


def test_adding_sh_wp_waypoints_did_not_move_nonlocal_nearest():
    # Non-SH/non-WP queries must still resolve to their own coast — the new
    # waypoints are all in the SH (lat<0) or WP window, so a US/Atlantic query
    # can never pick one.
    db = lp._get_coastline_db()
    for lat, lon in [(29.0, -95.0), (25.8, -80.2), (40.7, -74.0),
                     (18.4, -66.1), (14.0, -90.0), (32.3, -64.9)]:
        wp, _d = db.nearest_waypoint(lat, lon)
        assert not wp.region_key.startswith("sh_")
        assert not wp.region_key.startswith("wp_")
        assert not wp.region_key.startswith("ni_")


def test_sh_waypoints_are_all_southern():
    db = lp._get_coastline_db()
    assert db.sh_waypoints, "SH waypoint subset must be populated"
    assert all(w.lat < 0 for w in db.sh_waypoints)
    assert all(w.region_key.startswith("sh_") for w in db.sh_waypoints)
