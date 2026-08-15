"""Central Pacific (CP / CPHC) basin support — added 2026-08-14 after Lala
(CP012026) was live in the active feed but invisible to the catalog, search,
and storm page. AL/EP-only assumptions ran through the whole chain — b-deck
client, season ingest, basin detection, and four prefix gates in the track /
forecast path (the last found by review, not the author) — and these tests
pin each so CP can't silently regress.

Offline: no network, no fixtures. Imports reach services/ (httpx) and
compile_cache -> core (numpy is pulled in by other collected tests) — both are
in requirements-dev.txt; verified in a clean venv per that file's rule.
api/routes.py is deliberately NOT imported (it would pull fastapi and friends
into the offline suite); its gates are pinned by source scan instead.
"""

import re
from pathlib import Path

from compile_cache import detect_basin
from services.atcf_bdeck_client import _NHC_BASINS
from services.current_season_ingest import (
    _BDECK_RE,
    _PREFIX_TO_BASIN,
    _catalog_basin,
)

_REPO = Path(__file__).resolve().parent.parent


# --- b-deck client: CP ids must route to the NHC FTP source ------------------

def test_cp_is_an_nhc_basin():
    # CPHC b-decks live in the same ftp.nhc.noaa.gov/atcf/btk/ directory as
    # AL/EP. Routing CP through the JTWC-mirror branch dead-ends (no region).
    assert "CP" in _NHC_BASINS
    assert {"AL", "EP"} <= _NHC_BASINS


# --- season ingest: the btk index regex must list CP storms ------------------

def test_bdeck_regex_matches_cp():
    m = _BDECK_RE.search("bcp012026.dat")
    assert m is not None
    assert (m.group(1).upper(), m.group(2), m.group(3)) == ("CP", "01", "2026")


def test_bdeck_regex_still_matches_al_ep_and_skips_jtwc():
    assert _BDECK_RE.search("bal022026.dat") is not None
    assert _BDECK_RE.search("bep082026.dat") is not None
    # JTWC basins come from the UCAR mirror, never the NHC index.
    assert _BDECK_RE.search("bwp142026.dat") is None
    assert _BDECK_RE.search("bio012026.dat") is None


def test_cp_maps_to_ep_catalog_basin():
    # IBTrACS codes Central Pacific storms as basin EP (CP is a subbasin),
    # and the frontend's BASIN_NAMES has no CP chip — so the catalog row for
    # a CP storm must carry EP, not a raw CP code.
    assert _PREFIX_TO_BASIN["CP"] == "EP"
    assert _catalog_basin("CP", []) == "EP"


# --- scoring: detect_basin must not drop the Central Pacific into ATLANTIC ---

def _track(lat, lon, n=5):
    return [{"lat": lat, "lon": lon} for _ in range(n)]


def test_central_pacific_scores_as_eastern_pacific():
    # Lala CP012026's actual position band: ~17N, 140W-180.
    assert detect_basin(_track(17.2, -150.9)) == "EASTERN_PACIFIC"
    # Western edge of the band, short of the dateline.
    assert detect_basin(_track(15.0, -179.5)) == "EASTERN_PACIFIC"


def test_neighbouring_basins_unchanged():
    assert detect_basin(_track(15.6, -132.1)) == "EASTERN_PACIFIC"  # Hernan EP08
    assert detect_basin(_track(25.0, -75.0)) == "ATLANTIC"
    assert detect_basin(_track(20.0, 135.0)) == "WESTERN_PACIFIC"
    # Southern hemisphere near the same longitudes must stay SOUTH_PACIFIC —
    # the widened EASTERN_PACIFIC box requires lat >= 0.
    assert detect_basin(_track(-18.0, -150.0)) == "SOUTH_PACIFIC"


# --- track/forecast path: the prefix gates that made the first fix a no-op ---
#
# get_storm_track (api/routes.py) filters by id prefix at three points before
# the b-deck client is ever reached, and noaa_client gates the NHC forecast
# fetch by basin. The 2026-08-14 review found all four still excluded CP after
# the layers above were fixed — the storm page would have kept 404ing. These
# scan the source because importing api.routes would pull fastapi into the
# offline suite. A refactor that renames things will fail here loudly; update
# the pattern AND re-check CP is still in the gate.

def _source(rel):
    return (_REPO / rel).read_text(encoding="utf-8", errors="replace")


def test_routes_prefix_gates_include_cp():
    src = _source("api/routes.py")
    live_nhc = re.findall(r'_is_live_nhc = False\s*\n\s*if prefix in \(([^)]*)\)', src)
    assert live_nhc and '"CP"' in live_nhc[0], "_is_live_nhc gate lost CP"

    gates = re.findall(r'if not snapshots and prefix in \(([^)]*)\)', src)
    assert len(gates) == 4, f"expected 4 track fallback gates, found {len(gates)} — pattern stale?"
    with_cp = [g for g in gates if '"CP"' in g]
    without_cp = [g for g in gates if '"CP"' not in g]
    # Steps 3 (IBTrACS-by-ATCF), 3a (b-deck) and 4 (HURDAT2 — the "nepac"
    # file carries CP storms) must include CP. Step 3b (JTWC warning
    # bulletins) must NOT: the JTWC scraper has no central-pacific region
    # directory, so a CP id there would only probe nonexistent URLs.
    assert len(with_cp) == 3, f"track fallback gates excluding CP: {without_cp}"
    assert without_cp == ['"WP", "IO", "SH"'], (
        f"the one CP-less gate should be the JTWC bulletin step, got: {without_cp}"
    )

    tags = re.findall(r'source_tag = "nhc_bdeck" if prefix in \(([^)]*)\)', src)
    assert tags and '"CP"' in tags[0], "CP b-deck would be mislabeled jtwc_bdeck"


def test_hurdat2_file_selection_covers_cp():
    # Importable without fastapi: noaa_client only needs httpx & stdlib.
    from services.noaa_client import _hurdat2_url_for
    url, cache = _hurdat2_url_for("CP111992")   # Iniki
    assert "nepac" in url and cache == "hurdat2_epac.txt"
    url_ep, _ = _hurdat2_url_for("EP082026")
    assert url_ep == url
    url_al, cache_al = _hurdat2_url_for("AL092021")
    assert "nepac" not in url_al and cache_al == "hurdat2_atl.txt"


def test_forecast_basin_gate_includes_cp():
    src = _source("services/noaa_client.py")
    m = re.search(r'if basin not in \(([^)]*)\):\s*\n\s*logger\.debug\(f"Skipping NHC forecast', src)
    assert m and '"cp"' in m.group(1), "NHC forecast fetch still skips CP storms"
