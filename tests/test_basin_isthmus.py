"""The Atlantic / East Pacific line follows the isthmus, not 100W.

Otis 2023 came ashore at Acapulco (99.9W) as a Cat 5 and is the costliest
East Pacific hurricane on record. Basin detection used the storm's mean
longitude with 100W as the line, so Otis, whose whole life was east of it in
the Gulf of Tehuantepec, was filed as ATLANTIC: Atlantic tuning, no East
Pacific rapid-intensification bonus, no Mexico multiplier. It read 67 while
Lidia, a comparable storm that cost a hundredth as much, read 81.
"""
import json
import pathlib
import re
import shutil
import subprocess

import pytest

from compile_cache import ISTHMUS_DIVIDE, detect_basin, on_pacific_side

ROOT = pathlib.Path(__file__).resolve().parent.parent

PLACES = [
    # name, lat, lon, on the Pacific side?
    ("Acapulco", 16.85, -99.9, True),
    ("Gulf of Tehuantepec", 15.3, -94.8, True),
    ("Puerto Escondido", 15.86, -97.07, True),
    ("Guatemala's Pacific coast", 13.9, -90.8, True),
    ("El Salvador's coast", 13.3, -88.5, True),
    ("San Juan del Sur, Nicaragua", 11.25, -85.87, True),
    ("Quepos, Costa Rica", 9.43, -84.16, True),
    ("Panama City, Panama", 8.95, -79.53, True),
    ("Manzanillo", 19.05, -104.3, True),
    ("Mazatlan", 23.2, -106.4, True),
    ("Guaymas", 27.9, -110.9, True),
    ("Cabo San Lucas", 22.9, -109.9, True),
    ("open East Pacific", 15.0, -125.0, True),
    ("Bay of Campeche", 19.5, -94.5, False),
    ("Coatzacoalcos", 18.15, -94.4, False),
    ("Veracruz", 19.2, -96.13, False),
    ("Tampico", 22.2, -97.85, False),
    ("Gulf of Honduras", 16.0, -88.0, False),
    ("Bluefields, Nicaragua", 12.0, -83.77, False),
    ("Limon, Costa Rica", 10.0, -83.03, False),
    ("Colon, Panama", 9.36, -79.9, False),
    ("central Gulf of Mexico", 25.0, -90.0, False),
    ("central Caribbean", 15.0, -75.0, False),
    ("Monterrey", 25.7, -100.3, False),           # inland: a Gulf storm's remnant, not the Pacific
    ("Chihuahua", 28.6, -106.1, False),
    ("southern hemisphere", -10.0, -95.0, False),
]


@pytest.mark.parametrize("name, lat, lon, pacific", PLACES)
def test_which_side_of_the_isthmus(name, lat, lon, pacific):
    assert on_pacific_side(lat, lon) is pacific, name


def test_longitudes_past_180_are_normalised():
    assert on_pacific_side(16.85, 360 - 99.9) is True
    assert on_pacific_side(19.5, 360 - 94.5) is False


def _track(points):
    return [{"lat": la, "lon": lo} for la, lo in points]


OTIS = _track([(10.2, -97.0), (11.0, -97.2), (12.0, -97.5), (13.2, -98.0), (14.4, -98.6),
               (15.6, -99.3), (16.8, -99.9), (17.6, -100.4)])


def test_otis_is_an_east_pacific_storm():
    mean_lon = sum(s["lon"] for s in OTIS) / len(OTIS)
    assert mean_lon > -100                          # east of the old line: this was "ATLANTIC"
    assert detect_basin(OTIS) == "EASTERN_PACIFIC"


def test_storms_on_the_gulf_side_of_the_same_longitudes_stay_atlantic():
    campeche = _track([(19.0, -92.5), (19.5, -93.5), (20.0, -94.5), (20.5, -95.5), (20.8, -96.8), (20.9, -98.0)])
    assert detect_basin(campeche) == "ATLANTIC"
    nicaragua = _track([(13.0, -76.0), (13.3, -78.5), (13.5, -81.0), (13.6, -83.4), (13.8, -85.5), (13.8, -88.0)])
    assert detect_basin(nicaragua) == "ATLANTIC"


def test_a_crossover_belongs_to_the_ocean_it_lived_in():
    # Julia 2022: a Caribbean hurricane that ended its life in the Pacific.
    julia = _track([(12.0, -70.0), (12.3, -73.0), (12.5, -76.0), (12.6, -79.0), (12.5, -82.0),
                    (12.4, -84.5), (12.5, -87.2), (13.0, -89.5)])
    assert detect_basin(julia) == "ATLANTIC"
    # Bonnie 2022: a Caribbean tropical storm that became a major hurricane in the Pacific.
    bonnie = _track([(11.7, -68.0), (12.0, -74.9), (11.2, -81.8), (11.2, -87.4), (12.2, -93.4),
                     (13.8, -99.6), (15.4, -105.1), (16.3, -109.5), (17.5, -114.5)])
    assert detect_basin(bonnie) == "EASTERN_PACIFIC"


def test_a_gulf_storm_with_a_long_inland_remnant_stays_atlantic():
    # Dolly 2008: Brownsville, then two days over northern Mexico west of 100W.
    dolly = _track([(22.0, -87.0), (23.5, -91.0), (25.0, -94.5), (26.3, -97.2), (26.9, -98.6),
                    (27.6, -100.2), (28.5, -101.8), (29.6, -103.5), (30.8, -105.0), (32.0, -106.0)])
    assert detect_basin(dolly) == "ATLANTIC"


def test_everything_outside_the_shared_longitudes_is_as_before():
    assert detect_basin(_track([(15.0, -45.0), (20.0, -60.0), (28.0, -75.0)])) == "ATLANTIC"
    assert detect_basin(_track([(26.0, -88.0), (28.0, -89.5), (30.0, -89.6)])) == "ATLANTIC"   # Katrina's Gulf leg
    assert detect_basin(_track([(14.0, -105.0), (16.0, -108.0), (18.0, -111.0)])) == "EASTERN_PACIFIC"
    assert detect_basin(_track([(18.0, -150.0), (20.0, -156.0)])) == "EASTERN_PACIFIC"         # Central Pacific
    assert detect_basin(_track([(15.0, 130.0), (18.0, 125.0)])) == "WESTERN_PACIFIC"
    assert detect_basin([]) == "ATLANTIC"


# ── the page uses the same line ─────────────────────────────────────────────

def _js_table(text):
    m = re.search(r"const ISTHMUS_DIVIDE\s*=\s*(\[\[.*?\]\]);", text, re.S)
    assert m, "ISTHMUS_DIVIDE not found"
    return [tuple(float(x) for x in pair) for pair in json.loads(m.group(1))]


@pytest.mark.parametrize("rel", ["frontend/index.html", "frontend/basin_dps_coefficients.js"])
def test_the_page_carries_the_same_table(rel):
    text = (ROOT / rel).read_text(encoding="utf-8")
    assert _js_table(text) == [tuple(float(x) for x in pair) for pair in ISTHMUS_DIVIDE]
    assert "onPacificSide" in text


def test_the_page_classifies_points_the_way_the_engine_does():
    node = shutil.which("node")
    if not node:
        pytest.skip("node not available")
    html = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
    table = re.search(r"const ISTHMUS_DIVIDE=.*?;", html).group(0)
    side = re.search(r"function onPacificSide\(lat,n\)\{.*?return false\}", html, re.S).group(0)
    detect = re.search(r"function detectBasin\(lat,lon\)\{.*?return\"ATLANTIC\"\}", html, re.S).group(0)
    pts = [[lat, lon] for _n, lat, lon, _p in PLACES]
    script = (table + side + detect +
              "const pts=" + json.dumps(pts) + ";"
              "console.log(JSON.stringify(pts.map(p=>[onPacificSide(p[0],p[1]),detectBasin(p[0],p[1])])));")
    out = json.loads(subprocess.run([node, "-e", script], capture_output=True, text=True, check=True).stdout)
    for (name, lat, lon, pacific), (js_side, js_basin) in zip(PLACES, out):
        assert js_side is pacific, name
        if -100 <= lon <= -77 and lat >= 0:
            assert js_basin == ("EASTERN_PACIFIC" if pacific else "ATLANTIC"), name
