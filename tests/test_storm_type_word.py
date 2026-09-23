"""Storm pages name the storm the way its basin does.

Every scored title used to read "Hurricane {name}" — Typhoon Yagi, Cyclone
Fina and 116 Atlantic tropical storms that never reached Cat 1 included.
"""
import re

import pytest

import og_card
import seo


@pytest.mark.parametrize("cat,basin,word", [
    (5, "Atlantic", "Hurricane"),
    (0, "Atlantic", "Tropical Storm"),
    (None, "Atlantic", "Tropical Storm"),
    (3, "Eastern Pacific", "Hurricane"),
    (1, "East Pacific", "Hurricane"),
    (5, "Western Pacific", "Typhoon"),   # bundle spelling
    (4, "West Pacific", "Typhoon"),      # catalog-fallback spelling
    (0, "Western Pacific", "Tropical Storm"),
    (3, "South Pacific", "Cyclone"),
    (0, "South Pacific", "Cyclone"),     # every named SH storm is a cyclone
    (4, "South Indian", "Cyclone"),
    (2, "North Indian", "Cyclone"),
    (2, "", "Hurricane"),
])
def test_storm_type_word(cat, basin, word):
    assert seo.storm_type_word(cat, basin) == word


def test_og_card_uses_same_word():
    assert og_card._storm_type(3, "South Pacific") == "Cyclone"
    assert og_card._storm_type(5, "Western Pacific") == "Typhoon"


def _title(storm_id):
    page = seo.render_storm_page(storm_id)
    if not page:
        pytest.skip("index template not available")
    return re.search(r"<title>([^<]*)</title>", page).group(1)


def test_scored_titles_match_basin_and_intensity():
    storms = seo._read_compiled_bundle().get("storms", {})
    if not storms:
        pytest.skip("compiled bundle not available")
    wrong = []
    for sid, s in storms.items():
        want = seo.storm_type_word(s.get("category_lifetime") or s.get("category"),
                                   s.get("basin_name"))
        if not _title(sid).startswith(f"{want} "):
            wrong.append((sid, _title(sid)))
    assert not wrong, wrong[:10]


def test_known_titles():
    assert _title("2024244N09137").startswith("Typhoon Yagi (2024)")
    assert _title("2025322S10131").startswith("Cyclone Fina (2025)")
    assert _title("AL122005").startswith("Hurricane Katrina (2005)")
    assert "· Category 5 | StormDPS" in _title("AL122005")
