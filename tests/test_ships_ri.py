"""Offline tests for the SHIPS-RII rapid-intensification parser.

Fixture text is trimmed verbatim from the real product
(26080200EP0726_ships.txt, Genevieve at 2026-08-02 00Z) so the column
spacing under test is the spacing NHC actually publishes.
"""

from datetime import datetime, timezone

from services.ships_client import (
    _cycle_filename,
    parse_ships_ri,
)

# Real spacing, including the decoy "PRELIM RI PROB" line and the
# POT = MPI-VMAX line that must NOT be mistaken for the tau-0 MPI.
SAMPLE = """
TIME (HR)          0     6    12    18    24    36    48    60    72
POT. INT. (KT)   104    99    97   101    98    95    99    97    92
  % GOES IR PIXELS WITH T < -20 C    50-200 KM RAD:  32.0 (MEAN=65.0)
  PRELIM RI PROB (DV .GE. 35 KT IN 36 HR):            0.1

                CURRENT MAX WIND (KT):   40. LAT, LON:   23.4   135.5

       **2026 E. Pacific RI INDEX EP072026 GENEVIEVE  08/02/26  00 UTC **
 (SHIPS-RII PREDICTOR TABLE for 30 KT OR MORE MAXIMUM WIND INCREASE IN NEXT 24-h)
 POT = MPI-VMAX (KT)         :   59.9       30.0  to    148.5        0.25   0.7
 MAXIMUM WIND (KT)           :   40.0       22.5  to    132.0        0.50   0.9

 SHIPS Prob RI for 20kt/ 12hr RI threshold=    3% is    0.5 times climatological mean ( 6.3%)
 SHIPS Prob RI for 25kt/ 24hr RI threshold=    4% is    0.4 times climatological mean (12.5%)
 SHIPS Prob RI for 30kt/ 24hr RI threshold=    3% is    0.3 times climatological mean ( 8.6%)
 SHIPS Prob RI for 35kt/ 24hr RI threshold=    0% is    0.0 times climatological mean ( 6.2%)
 SHIPS Prob RI for 40kt/ 24hr RI threshold=    0% is    0.0 times climatological mean ( 4.2%)
 SHIPS Prob RI for 45kt/ 36hr RI threshold=    0% is    0.0 times climatological mean ( 6.7%)
 SHIPS Prob RI for 55kt/ 48hr RI threshold=    0% is    0.0 times climatological mean ( 5.9%)
 SHIPS Prob RI for 65kt/ 72hr RI threshold=    0% is    0.0 times climatological mean ( 4.7%)
"""


def test_parses_all_eight_thresholds():
    out = parse_ships_ri(SAMPLE)
    assert out is not None
    assert len(out["thresholds"]) == 8
    assert [(t["threshold_kt"], t["hours"]) for t in out["thresholds"]] == [
        (20, 12), (25, 24), (30, 24), (35, 24),
        (40, 24), (45, 36), (55, 48), (65, 72)]


def test_headline_is_the_canonical_30kt_24h_criterion():
    out = parse_ships_ri(SAMPLE)
    assert out["headline"] == {
        "threshold_kt": 30, "hours": 24, "probability_pct": 3,
        "climo_ratio": 0.3, "climatology_pct": 8.6}
    # Present and canonical -> no flag.
    assert "headline_is_canonical" not in out


def test_missing_canonical_threshold_is_flagged_not_silently_substituted():
    trimmed = "\n".join(
        l for l in SAMPLE.splitlines() if "for 30kt/ 24hr" not in l)
    out = parse_ships_ri(trimmed)
    assert out["headline_is_canonical"] is False
    # A 12-hour criterion must never masquerade as the canonical RI number.
    assert out["headline"]["hours"] == 12


def test_mpi_comes_from_pot_int_tau0_not_the_mean_headroom_line():
    """POT = MPI-VMAX (59.9) is the 0-24h MEAN; tau-0 MPI is 104.

    Deriving MPI as vmax + 59.9 = 99.9 understates the real 104 and mixes a
    SHIPS-cycle number with a possibly-newer advisory wind.
    """
    out = parse_ships_ri(SAMPLE)
    assert out["mpi_kt"] == 104
    assert out["ships_vmax_kt"] == 40.0
    assert out["headroom_kt"] == 64.0
    assert out["mpi_kt"] != round(out["ships_vmax_kt"] + 59.9)


def test_prelim_ri_prob_line_is_not_captured():
    out = parse_ships_ri(SAMPLE)
    # The decoy line mentions 35 KT / 36 HR; no threshold may match that pair.
    assert (35, 36) not in [(t["threshold_kt"], t["hours"])
                            for t in out["thresholds"]]


def test_returns_none_without_an_ri_block():
    assert parse_ships_ri("") is None
    assert parse_ships_ri("nothing useful here") is None
    # Potential-intensity data alone is not an RI outlook.
    assert parse_ships_ri("POT. INT. (KT)   104    99") is None


def test_cycle_filenames():
    cyc = datetime(2026, 8, 2, 0, 0, tzinfo=timezone.utc)
    assert _cycle_filename("EP072026", cyc) == "26080200EP0726_ships.txt"
    assert _cycle_filename("ep072026", cyc) == "26080200EP0726_ships.txt"
    assert _cycle_filename("AL022026", cyc) == "26080200AL0226_ships.txt"
    # Cycle year and storm year are independent (Jan cycle, prior-year storm).
    assert _cycle_filename(
        "AL012026", datetime(2027, 1, 1, 0, 0, tzinfo=timezone.utc)
    ) == "27010100AL0126_ships.txt"


def test_non_nhc_and_malformed_ids_get_no_filename():
    cyc = datetime(2026, 8, 2, 0, 0, tzinfo=timezone.utc)
    for bad in ("WP122026", "IO012026", "SH012026",   # JTWC: no SHIPS product
                "EP07", "", "2026213N12345", "GENEVIEVE"):
        assert _cycle_filename(bad, cyc) is None


def test_synoptic_cycle_flooring_covers_every_hour():
    """The client floors 'now' to 00/06/12/18Z before building a filename."""
    for hour in range(24):
        t = datetime(2026, 8, 2, hour, 37, tzinfo=timezone.utc)
        floored = t.replace(minute=0, second=0, microsecond=0)
        floored -= __import__("datetime").timedelta(hours=floored.hour % 6)
        assert floored.hour in (0, 6, 12, 18)
        assert floored <= t
