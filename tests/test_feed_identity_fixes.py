"""Two identity mix-ups found during Hurricane Isaias (2026-10-09).

1. JTWC bulletin names: 15E (ex-Nolo, across the dateline) was listed as
   "KOGUMA" beside the real Koguma (27W), because its bulletin cross-references
   27W and the header pattern did not accept an "E" designator.
2. Sidebar catalog vs storm page: a live-cache score filed under a storm's
   IBTrACS SID displaced the compiled bundle's score for the same storm
   (Sally 2020: sidebar 53, page 39), tripping the self-check's
   "catalog/hero score drift".
"""
import api.routes as routes
from api.routes import present_active_storms
from services.jtwc_client import JTWCClient, _id_number, _parse_header_name

# Trimmed from the real ep1526web.txt bulletin of 2026-10-09 (public domain).
NOLO_BULLETIN = """WTPN31 PGTW 091500
SUBJ/TROPICAL STORM 15E (NOLO) WARNING NR 077/
RMKS/
1. TROPICAL STORM 15E (NOLO) WARNING NR 077
   01 ACTIVE TROPICAL CYCLONE IN NORTHWESTPAC
   WARNING POSITION:
   091200Z --- NEAR 28.8N 148.7E
   MAX SUSTAINED WINDS - 050 KT, GUSTS 065 KT
09OCT26. TROPICAL STORM 15E (NOLO), LOCATED APPROXIMATELY 495 NM
EAST-SOUTHEAST OF YOKOSUKA, JAPAN, HAS TRACKED NORTHWESTWARD.
REFER TO TYPHOON 27W (KOGUMA) WARNINGS (WTPN33 PGTW)
FOR SIX-HOURLY UPDATES.//
"""


def test_bulletin_name_is_the_bulletins_own_storm():
    assert _id_number("EP152026") == "15" and _id_number("bogus") is None
    assert _parse_header_name(NOLO_BULLETIN, "15") == "NOLO"
    assert _parse_header_name(NOLO_BULLETIN, "27") == "KOGUMA"
    # Only a cross-reference to another storm: no name (caller keeps the RSS name).
    assert _parse_header_name("REFER TO TYPHOON 27W (KOGUMA) WARNINGS", "15") is None
    assert _parse_header_name("TROPICAL CYCLONE 03B (MAN-YI) WARNING NR 004", "03") == "MAN-YI"
    assert _parse_header_name("HURRICANE 20E (SIMON) WARNING NR 010", "20") == "SIMON"
    assert _parse_header_name("no storm here", "15") is None


def test_warning_text_keeps_rss_name_when_header_is_another_storms():
    c = JTWCClient()
    w = {"id": "EP152026", "name": "NOLO", "basin": "EP"}
    assert c._parse_warning_text(NOLO_BULLETIN, w)["name"] == "NOLO"
    only_xref = "WARNING POSITION:\n091200Z --- NEAR 28.8N 148.7E\nREFER TO TYPHOON 27W (KOGUMA) WARNINGS"
    assert c._parse_warning_text(only_xref, w)["name"] == "NOLO"


def test_jtwc_row_with_an_east_pacific_id_is_title_cased(monkeypatch):
    monkeypatch.setattr(routes, "_active_dps", lambda sid: None)
    monkeypatch.setattr(routes, "_stall_near_land", lambda la, lo: False)
    rows = present_active_storms([
        dict(id="EP152026", name="NOLO", lat=28.8, lon=148.7, source="JTWC"),
        dict(id="WP272026", name="KOGUMA", lat=19.3, lon=155.5, source="JTWC"),
        dict(id="al092026", name="Isaias", lat=27.0, lon=-87.7),
    ])
    assert sorted(r["name"] for r in rows) == ["Isaias", "Koguma", "Nolo"]


def test_bundle_score_wins_over_a_cache_entry_under_the_storms_other_id(monkeypatch):
    import seo
    bundle = {"storms": {"AL192020": {"dps": 39.38, "dps_label": "Moderate", "name": "Sally", "year": 2020}}}
    monkeypatch.setattr(seo, "_read_compiled_bundle", lambda: bundle)
    monkeypatch.setattr(routes, "_dps_cache_scores", lambda: {
        "2020256N25281": {"dps": 52.67, "dps_label": "Severe"},     # Sally's SID: must NOT win
        "AL092026": {"dps": 69.2, "dps_label": "Extreme"},          # live storm, not in the bundle
    })
    out = routes._harmonized([
        {"id": "2020256N25281", "name": "Sally", "year": 2020, "basin": "NA", "peak_dps": 30, "dps_label": "Moderate"},
        {"id": "al092026", "name": "Isaias", "year": 2026, "basin": "NA", "peak_dps": 10, "dps_label": "Low"},
    ])
    by_name = {r["name"]: r for r in out}
    assert (by_name["Sally"]["peak_dps"], by_name["Sally"]["dps_label"]) == (39, "Moderate")
    assert (by_name["Isaias"]["peak_dps"], by_name["Isaias"]["dps_label"]) == (69, "Extreme")
