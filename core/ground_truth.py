"""
Ground-truth registry for major historical hurricanes.

This is the empirical "answer key" we tune the DPS formula against. Every
value here comes from a published authoritative source (NCEI Storm Events,
NOAA/NHC Tropical Cyclone Reports, USGS HWM surveys, CO-OPS tide gauges,
or NWS observed rainfall totals). Sources are listed next to each record
so the next person can verify without re-doing the research.

The registry serves three purposes:

  1. **Validation.** ``bin/validate_dps.py`` (compiled separately) joins
     this table to ``compiled_bundle.json`` and reports the correlation
     between modeled DPS and real-world damage.

  2. **Formula calibration.** The engine can look up the observed peak
     rainfall (inches) for a storm and use it in place of the estimated
     value when present, eliminating one of the biggest error sources
     (the current rainfall estimator is a stall-hour heuristic).

  3. **Frontend enrichment.** ``compile_cache.py`` pulls from here to
     decorate each compiled entry with a ``ground_truth`` sub-object
     (damage $, fatalities, peak observed surge, peak observed rainfall,
     FEMA declaration, etc.) that the hero card and accordion can show.

Whenever a live client (CO-OPS, NCEI, OpenFEMA, MRMS) returns data for a
storm, ``refresh_ground_truth`` merges the live values into the registry,
preferring live data over the hardcoded values.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class GroundTruth:
    storm_id: str
    name: str
    year: int
    # Damage (sources: NCEI Storm Events + official NHC TCRs)
    damage_usd: Optional[float] = None         # 2020 US$ adjusted
    deaths_total: Optional[int] = None         # direct + indirect
    # Observed peaks (sources: NOAA/NHC/USGS)
    peak_surge_ft: Optional[float] = None      # highest HWM / tide gauge
    peak_surge_location: Optional[str] = None
    peak_rainfall_in: Optional[float] = None   # highest station-observed total
    peak_rainfall_location: Optional[str] = None
    peak_wind_landfall_mph: Optional[int] = None
    landfall_category_saffir: Optional[int] = None
    landfall_pressure_mb: Optional[int] = None
    # FEMA (sources: OpenFEMA DR declarations)
    fema_counties_declared: Optional[int] = None
    fema_states: list[str] = field(default_factory=list)
    fema_major_disaster: Optional[bool] = None
    # Provenance
    sources: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None and v != []}


# ---------------------------------------------------------------------------
# Curated registry. Values drawn from NHC Tropical Cyclone Reports.
# Every row lists the primary TCR that gave us the numbers so a future
# maintainer can verify.
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, GroundTruth] = {
    "AL122005": GroundTruth(
        storm_id="AL122005",
        name="Katrina",
        year=2005,
        damage_usd=125_000_000_000,     # NHC TCR AL122005 (Knabb et al. 2005)
        deaths_total=1833,
        peak_surge_ft=27.8,
        peak_surge_location="Pass Christian, MS",
        peak_rainfall_in=15.0,
        peak_rainfall_location="Big Branch, LA",
        peak_wind_landfall_mph=125,
        landfall_category_saffir=3,
        landfall_pressure_mb=920,
        fema_counties_declared=90,
        fema_states=["LA", "MS", "AL", "FL"],
        fema_major_disaster=True,
        sources=["NHC TCR AL122005", "USGS OFR 2006-1020", "NCEI Storm Events"],
    ),
    "AL092017": GroundTruth(
        storm_id="AL092017",
        name="Harvey",
        year=2017,
        damage_usd=125_000_000_000,
        deaths_total=103,
        peak_surge_ft=12.5,
        peak_surge_location="Aransas Pass, TX",
        peak_rainfall_in=60.58,
        peak_rainfall_location="Nederland, TX (Jefferson County)",
        peak_wind_landfall_mph=130,
        landfall_category_saffir=4,
        landfall_pressure_mb=937,
        fema_counties_declared=60,
        fema_states=["TX", "LA"],
        fema_major_disaster=True,
        sources=["NHC TCR AL092017 (Blake & Zelinsky 2018)", "NWS WFO HGX"],
    ),
    "AL112017": GroundTruth(
        storm_id="AL112017",
        name="Irma",
        year=2017,
        damage_usd=77_160_000_000,
        deaths_total=134,
        peak_surge_ft=10.0,
        peak_surge_location="Cudjoe Key / Big Pine Key, FL",
        peak_rainfall_in=21.66,
        peak_rainfall_location="Fort Pierce, FL",
        peak_wind_landfall_mph=130,
        landfall_category_saffir=4,
        landfall_pressure_mb=931,
        fema_counties_declared=67,
        fema_states=["FL", "GA", "SC", "PR", "VI"],
        fema_major_disaster=True,
        sources=["NHC TCR AL112017 (Cangialosi et al. 2018)"],
    ),
    "AL092022": GroundTruth(
        storm_id="AL092022",
        name="Ian",
        year=2022,
        damage_usd=112_900_000_000,
        deaths_total=156,
        peak_surge_ft=15.0,
        peak_surge_location="Fort Myers Beach, FL",
        peak_rainfall_in=26.95,
        peak_rainfall_location="Grove City, FL",
        peak_wind_landfall_mph=150,
        landfall_category_saffir=5,     # reclassified to Cat 5 in April 2023
        landfall_pressure_mb=936,
        fema_counties_declared=26,
        fema_states=["FL", "SC", "NC"],
        fema_major_disaster=True,
        sources=["NHC TCR AL092022 (Bucci et al. 2023)", "USGS STN"],
    ),
    "AL182012": GroundTruth(
        storm_id="AL182012",
        name="Sandy",
        year=2012,
        damage_usd=70_200_000_000,
        deaths_total=233,
        peak_surge_ft=13.88,
        peak_surge_location="Kings Point, NY (The Battery: 11.3 ft)",
        peak_rainfall_in=12.55,
        peak_rainfall_location="Easton, MD",
        peak_wind_landfall_mph=80,
        landfall_category_saffir=1,     # extratropical at landfall
        landfall_pressure_mb=945,
        fema_counties_declared=120,
        fema_states=["NY", "NJ", "CT", "MD", "DE", "WV", "NH", "RI"],
        fema_major_disaster=True,
        sources=["NHC TCR AL182012 (Blake et al. 2013)"],
    ),
    "AL142018": GroundTruth(
        storm_id="AL142018",
        name="Michael",
        year=2018,
        damage_usd=25_500_000_000,
        deaths_total=74,
        peak_surge_ft=14.73,
        peak_surge_location="Mexico Beach, FL",
        peak_rainfall_in=9.08,
        peak_rainfall_location="Havana, FL",
        peak_wind_landfall_mph=160,
        landfall_category_saffir=5,     # upgraded to Cat 5 in April 2019
        landfall_pressure_mb=919,
        fema_counties_declared=12,
        fema_states=["FL", "GA", "AL"],
        fema_major_disaster=True,
        sources=["NHC TCR AL142018 (Beven et al. 2019)"],
    ),
    # ── Extra calibration data for neighboring reference storms ──
    "AL092024": GroundTruth(
        storm_id="AL092024",
        name="Helene",
        year=2024,
        damage_usd=78_700_000_000,
        deaths_total=230,
        peak_surge_ft=15.0,
        peak_surge_location="Keaton Beach, FL",
        peak_rainfall_in=31.33,
        peak_rainfall_location="Busick, NC",
        peak_wind_landfall_mph=140,
        landfall_category_saffir=4,
        landfall_pressure_mb=938,
        fema_counties_declared=100,
        fema_states=["FL", "GA", "SC", "NC", "TN", "VA"],
        fema_major_disaster=True,
        sources=["NHC TCR preliminary 2024", "NOAA NWS AHPS"],
    ),
    "AL142024": GroundTruth(
        storm_id="AL142024",
        name="Milton",
        year=2024,
        damage_usd=34_000_000_000,
        deaths_total=35,
        peak_surge_ft=10.0,
        peak_surge_location="Manasota Key, FL",
        peak_rainfall_in=19.0,
        peak_rainfall_location="St. Petersburg, FL",
        peak_wind_landfall_mph=120,
        landfall_category_saffir=3,
        landfall_pressure_mb=954,
        fema_counties_declared=34,
        fema_states=["FL"],
        fema_major_disaster=True,
        sources=["NHC preliminary", "NWS WFO TBW"],
    ),
    "AL052019": GroundTruth(
        storm_id="AL052019",
        name="Dorian",
        year=2019,
        damage_usd=5_100_000_000,   # US damages only — Bahamas much higher
        deaths_total=84,
        peak_surge_ft=24.0,
        peak_surge_location="Great Abaco, Bahamas",
        peak_rainfall_in=24.0,
        peak_rainfall_location="Hope Town, Bahamas",
        peak_wind_landfall_mph=90,    # US landfall at Cape Hatteras
        landfall_category_saffir=1,
        landfall_pressure_mb=956,
        fema_counties_declared=17,
        fema_states=["NC", "SC", "FL"],
        fema_major_disaster=True,
        sources=["NHC TCR AL052019 (Avila et al. 2020)"],
    ),
    "AL062018": GroundTruth(
        storm_id="AL062018",
        name="Florence",
        year=2018,
        damage_usd=24_230_000_000,
        deaths_total=52,
        peak_surge_ft=10.1,
        peak_surge_location="Emerald Isle, NC",
        peak_rainfall_in=35.93,
        peak_rainfall_location="Elizabethtown, NC",
        peak_wind_landfall_mph=90,
        landfall_category_saffir=1,
        landfall_pressure_mb=956,
        fema_counties_declared=34,
        fema_states=["NC", "SC"],
        fema_major_disaster=True,
        sources=["NHC TCR AL062018 (Stewart & Berg 2019)"],
    ),
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get(storm_id: str) -> Optional[GroundTruth]:
    """Return the curated ground-truth record for a storm, if known."""
    return _REGISTRY.get(storm_id)


def get_by_name_year(name: str, year: int) -> Optional[GroundTruth]:
    target = name.upper()
    for row in _REGISTRY.values():
        if row.name.upper() == target and row.year == year:
            return row
    return None


def all_records() -> dict[str, GroundTruth]:
    return dict(_REGISTRY)


def merge_live(storm_id: str, live: dict) -> Optional[GroundTruth]:
    """
    Merge a ``live`` dict from any of the clients into the curated record.
    Live values take precedence over curated ones because observed data is
    more accurate than a hand-transcribed TCR number.

    Unknown fields are silently ignored.
    """
    gt = _REGISTRY.get(storm_id)
    if gt is None:
        return None
    for field_name, val in live.items():
        if val is None:
            continue
        if hasattr(gt, field_name):
            setattr(gt, field_name, val)
    return gt
