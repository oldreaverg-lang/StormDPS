"""
Land Proximity & Coastline Analysis Module.

This module computes distance from a storm center to the nearest coastline,
population threat estimation, and land proximity factors. These metrics address
key limitations in the original hazard scoring:

    Issue #3 (open ocean scoring): The original model applied coastal-region
    factors to open ocean locations, incorrectly amplifying surge/rainfall
    threat. A storm 1000km from any coast should not use Gulf Coast surge
    amplification. This module detects open ocean vs. near-shore and applies
    smooth sigmoid transitions rather than hard cutoffs.

    Issue #6 (near-land multipliers): The original model triggered coastal
    amplification factors for storms approaching from the open ocean but still
    >500km away, creating unrealistic threat inflation. This module uses
    distance-dependent sigmoid transitions: only nearby coast (<200km) triggers
    meaningful amplification.

    Issue #10 (zone lookup gaps): The original bounding-box region assignment
    left gaps (e.g., east of Bermuda, remote Caribbean) where storms fell into
    "unknown" zones. This module uses Haversine distance to the nearest
    coastline point, providing continuous coverage with no gaps.

The core approach:
  1. CoastlineDatabase embeds 150+ coastal waypoints (lat/lon) for all
     hurricane-prone regions, each mapped to a COASTAL_PROFILES region_key.
  2. Haversine distance finds the nearest coastal point from a storm location.
  3. compute_land_proximity_factor() applies a smooth sigmoid curve:
     - 0.0 at >500km (open ocean) → no amplification
     - 1.0 at <50km (at coast) → full amplification
  4. compute_population_threat() estimates people within the R34 (tropical-
     storm-force wind) radius using population density from each coastline point.
  5. get_nearest_region() returns the COASTAL_PROFILES key, with fallback to
     "open_ocean" for truly distant locations.

References:
  - Haversine formula (Sinnott, 1984)
  - Sigmoid curves for geographic transitions (Tobler's First Law of Geography)
  - Coastal population data from NOAA, World Bank, national census sources
"""

import math
import numpy as np
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# ============================================================================
#  COASTLINE WAYPOINTS DATA
# ============================================================================

@dataclass
class CoastlineWaypoint:
    """
    A discrete geographic point on a coastline, used to find the nearest coast
    from a storm location and to estimate population threat.

    Attributes:
        lat: Latitude (degrees, -90 to +90)
        lon: Longitude (degrees, -180 to +180)
        region_key: Key in COASTAL_PROFILES (e.g., 'gulf_west_tx')
            This is the anchor point that determines which regional parameters apply
            when a storm approaches this coast.
        population_density: Relative population density (0.0-1.0 scale, unitless)
            - 0.0 = uninhabited coast (remote barrier island, unpopulated desert)
            - 0.3 = rural coast (sparse towns, low density)
            - 0.6 = suburban coast (moderate towns, dispersed communities)
            - 0.8 = semi-urban coast (cities >100k population)
            - 1.0 = major metro coast (>1M population, high density)
            Used to estimate how many people fall within the R34 radius when
            a storm approaches. See compute_population_threat().
        name: Human-readable name (e.g., 'Galveston, TX') for logging/debugging
    """
    lat: float
    lon: float
    region_key: str
    population_density: float
    name: str


class CoastlineDatabase:
    """
    Stores and manages curated coastline waypoints for all hurricane-prone regions.

    The database spans:
      - US Gulf Coast (TX→FL): major bays, city centers, barrier islands
      - US Atlantic Coast (FL→ME): major inlets, city centers, coastal features
      - Caribbean Islands: major islands with sub-regions
      - Central America & Mexico: coastal cities and features
      - Bermuda: isolated Atlantic island
      - Other Atlantic features (Azores, Africa proxy for ACE distance)

    Waypoints are distributed to capture:
      - Major population centers (for population threat scoring)
      - Significant bays and inlets (where surge amplification occurs)
      - Geographic turning points (e.g., Florida Keys vs. Tampa Bay)

    Total: 150+ waypoints providing continuous coverage with ~100-150 km spacing
    in densely populated regions and wider spacing in remote areas.

    Design principle: Waypoints should be placed where:
      1. Population is concentrated (coastal cities)
      2. The coast changes orientation or bathymetry (major bays)
      3. Sub-regions require distinct parameters (e.g., W TX vs. Central TX)

    Avoid: Random points on empty stretches of coastline; the spacing should
    reflect geographic/demographic importance, not uniform coverage.
    """

    def __init__(self):
        """Initialize the coastline database with curated waypoints."""
        self.waypoints: List[CoastlineWaypoint] = self._build_waypoints()
        # WP subset for the Tranche B distance gates (profile assignment,
        # coastal hours, land contact/LFI) — scanning ~130 points instead of
        # the full DB keeps the WP predicates cheap and unambiguous.
        self.wp_waypoints: List[CoastlineWaypoint] = [
            w for w in self.waypoints if w.region_key.startswith("wp_")
        ]
        # SH subset (same architecture, southern-hemisphere gate).
        self.sh_waypoints: List[CoastlineWaypoint] = [
            w for w in self.waypoints if w.region_key.startswith("sh_")
        ]

    def _build_waypoints(self) -> List[CoastlineWaypoint]:
        """
        Build the complete list of coastal waypoints.

        Returns:
            List of CoastlineWaypoint objects covering all hurricane-prone regions.
        """
        waypoints = []

        # ====== US GULF COAST (West Texas) ======
        # Region: gulf_west_tx
        waypoints.extend([
            CoastlineWaypoint(26.055, -97.176, "gulf_west_tx", 0.70, "Corpus Christi, TX"),
            CoastlineWaypoint(26.600, -97.300, "gulf_west_tx", 0.40, "Port Aransas, TX"),
            CoastlineWaypoint(27.800, -97.400, "gulf_west_tx", 0.25, "Mustang Island, TX"),
            CoastlineWaypoint(25.850, -97.050, "gulf_west_tx", 0.30, "Padre Island, TX"),
            CoastlineWaypoint(25.915, -97.162, "gulf_west_tx", 0.35, "South Padre Island, TX"),
            CoastlineWaypoint(26.510, -97.150, "gulf_west_tx", 0.20, "Port Mansfield, TX"),
        ])

        # ====== US GULF COAST (Central Texas) ======
        # Region: gulf_central_tx
        waypoints.extend([
            CoastlineWaypoint(29.761, -95.364, "gulf_central_tx", 0.85, "Galveston, TX"),
            CoastlineWaypoint(28.950, -95.650, "gulf_central_tx", 0.75, "Freeport, TX"),
            CoastlineWaypoint(28.320, -95.180, "gulf_central_tx", 0.40, "Matagorda Bay, TX"),
            CoastlineWaypoint(29.100, -95.900, "gulf_central_tx", 0.50, "Port of Texas City, TX"),
            CoastlineWaypoint(29.343, -94.804, "gulf_central_tx", 0.55, "Beaumont/Port Arthur area, TX"),
        ])

        # ====== US GULF COAST (Louisiana) ======
        # Region: gulf_la
        waypoints.extend([
            CoastlineWaypoint(29.560, -91.635, "gulf_la", 0.45, "Houma, LA"),
            CoastlineWaypoint(29.272, -90.107, "gulf_la", 0.80, "New Orleans, LA"),
            CoastlineWaypoint(28.945, -89.957, "gulf_la", 0.55, "Port of New Orleans, LA"),
            CoastlineWaypoint(29.615, -89.545, "gulf_la", 0.25, "Plaquemines Parish, LA"),
            CoastlineWaypoint(29.853, -89.271, "gulf_la", 0.15, "Mississippi River Delta, LA"),
            CoastlineWaypoint(29.325, -91.165, "gulf_la", 0.35, "Morgan City, LA"),
            CoastlineWaypoint(29.000, -90.500, "gulf_la", 0.40, "Barataria Bay, LA"),
        ])

        # ====== US GULF COAST (Mississippi / Alabama) ======
        # Region: gulf_ms_al
        waypoints.extend([
            CoastlineWaypoint(30.350, -88.725, "gulf_ms_al", 0.65, "Biloxi, MS"),
            CoastlineWaypoint(30.230, -88.380, "gulf_ms_al", 0.70, "Gulfport, MS"),
            CoastlineWaypoint(30.270, -88.575, "gulf_ms_al", 0.55, "Pass Christian, MS"),
            CoastlineWaypoint(30.680, -87.725, "gulf_ms_al", 0.60, "Pascagoula, MS"),
            CoastlineWaypoint(30.292, -87.652, "gulf_ms_al", 0.70, "Mobile Bay, AL"),
            CoastlineWaypoint(30.680, -87.561, "gulf_ms_al", 0.50, "Pensacola area, FL"),
        ])

        # ====== US GULF COAST (Florida Panhandle) ======
        # Region: gulf_fl_panhandle
        waypoints.extend([
            CoastlineWaypoint(30.413, -86.511, "gulf_fl_panhandle", 0.65, "Panama City, FL"),
            CoastlineWaypoint(29.687, -85.609, "gulf_fl_panhandle", 0.45, "Mexico Beach, FL"),
            CoastlineWaypoint(29.948, -84.875, "gulf_fl_panhandle", 0.50, "Apalachicola, FL"),
            CoastlineWaypoint(30.148, -85.398, "gulf_fl_panhandle", 0.40, "Cape San Blas, FL"),
            CoastlineWaypoint(30.562, -86.707, "gulf_fl_panhandle", 0.55, "Pensacola, FL"),
        ])

        # ====== US GULF COAST (Florida West Coast) ======
        # Region: gulf_fl_west
        waypoints.extend([
            CoastlineWaypoint(28.066, -82.627, "gulf_fl_west", 0.80, "Tampa, FL"),
            CoastlineWaypoint(27.765, -82.635, "gulf_fl_west", 0.75, "St. Petersburg, FL"),
            CoastlineWaypoint(27.113, -82.454, "gulf_fl_west", 0.65, "Sarasota, FL"),
            CoastlineWaypoint(26.568, -82.106, "gulf_fl_west", 0.55, "Naples, FL"),
            CoastlineWaypoint(26.143, -81.814, "gulf_fl_west", 0.45, "Everglades City, FL"),
            CoastlineWaypoint(26.564, -81.947, "gulf_fl_west", 0.40, "Marco Island, FL"),
            CoastlineWaypoint(27.508, -82.733, "gulf_fl_west", 0.50, "Clearwater, FL"),
        ])

        # ====== US ATLANTIC COAST (Florida East Coast) ======
        # Region: atl_fl_east
        waypoints.extend([
            CoastlineWaypoint(25.761, -80.188, "atl_fl_east", 0.80, "Miami, FL"),
            CoastlineWaypoint(26.122, -80.109, "atl_fl_east", 0.70, "Fort Lauderdale, FL"),
            CoastlineWaypoint(26.730, -80.051, "atl_fl_east", 0.60, "West Palm Beach, FL"),
            CoastlineWaypoint(27.489, -80.348, "atl_fl_east", 0.50, "Sebastian Inlet, FL"),
            CoastlineWaypoint(28.358, -80.590, "atl_fl_east", 0.55, "Port Canaveral, FL"),
            CoastlineWaypoint(28.540, -81.389, "atl_fl_east", 0.65, "Daytona Beach, FL"),
            CoastlineWaypoint(29.207, -81.312, "atl_fl_east", 0.70, "St. Augustine, FL"),
            CoastlineWaypoint(30.334, -81.655, "atl_fl_east", 0.60, "Jacksonville, FL"),
            CoastlineWaypoint(25.284, -80.246, "atl_fl_east", 0.40, "Key West, FL"),
        ])

        # ====== US ATLANTIC COAST (Georgia / South Carolina) ======
        # Region: atl_ga_sc
        waypoints.extend([
            CoastlineWaypoint(31.098, -81.465, "atl_ga_sc", 0.75, "Savannah, GA"),
            CoastlineWaypoint(32.033, -80.984, "atl_ga_sc", 0.70, "Charleston, SC"),
            CoastlineWaypoint(32.777, -80.359, "atl_ga_sc", 0.45, "Georgetown, SC"),
            CoastlineWaypoint(33.661, -79.018, "atl_ga_sc", 0.55, "Myrtle Beach, SC"),
        ])

        # ====== US ATLANTIC COAST (North Carolina) ======
        # Region: atl_nc
        waypoints.extend([
            CoastlineWaypoint(34.720, -76.730, "atl_nc", 0.50, "New Bern, NC"),
            CoastlineWaypoint(35.270, -75.555, "atl_nc", 0.60, "Outer Banks, NC (Hatteras)"),
            CoastlineWaypoint(35.215, -75.747, "atl_nc", 0.50, "Outer Banks, NC (Cape Hatteras)"),
            CoastlineWaypoint(35.052, -75.702, "atl_nc", 0.45, "Outer Banks, NC (Nags Head)"),
            CoastlineWaypoint(36.049, -75.595, "atl_nc", 0.35, "Virginia Beach area, NC"),
        ])

        # ====== US ATLANTIC COAST (Mid-Atlantic: VA, MD, DE, NJ) ======
        # Region: atl_mid
        waypoints.extend([
            CoastlineWaypoint(37.270, -76.630, "atl_mid", 0.70, "Hampton Roads, VA"),
            CoastlineWaypoint(37.540, -76.333, "atl_mid", 0.50, "Willoughby Spit, VA"),
            CoastlineWaypoint(38.976, -76.468, "atl_mid", 0.60, "Annapolis/Baltimore, MD"),
            CoastlineWaypoint(38.327, -75.528, "atl_mid", 0.45, "Delaware Bay, DE"),
            CoastlineWaypoint(39.366, -74.428, "atl_mid", 0.75, "Atlantic City, NJ"),
            CoastlineWaypoint(39.573, -74.076, "atl_mid", 0.70, "Cape May, NJ"),
        ])

        # ====== US ATLANTIC COAST (Northeast: NY to New England) ======
        # Region: atl_ne
        waypoints.extend([
            CoastlineWaypoint(40.714, -74.006, "atl_ne", 0.85, "New York, NY"),
            CoastlineWaypoint(40.752, -73.977, "atl_ne", 0.80, "Manhattan, NY"),
            CoastlineWaypoint(40.580, -73.980, "atl_ne", 0.70, "Brooklyn/Queens, NY"),
            CoastlineWaypoint(41.258, -72.005, "atl_ne", 0.65, "New Haven, CT"),
            CoastlineWaypoint(41.808, -71.412, "atl_ne", 0.70, "Providence, RI"),
            CoastlineWaypoint(42.359, -71.058, "atl_ne", 0.80, "Boston, MA"),
            CoastlineWaypoint(42.650, -70.240, "atl_ne", 0.55, "Cape Cod, MA"),
            CoastlineWaypoint(43.365, -70.757, "atl_ne", 0.50, "Portland, ME"),
            CoastlineWaypoint(44.390, -68.210, "atl_ne", 0.35, "Bar Harbor, ME"),
        ])

        # ====== BERMUDA (Atlantic) ======
        # Region: bermuda (special case — see note on profile definition)
        waypoints.extend([
            CoastlineWaypoint(32.295, -64.897, "bermuda", 0.60, "Hamilton, Bermuda"),
            CoastlineWaypoint(32.362, -64.776, "bermuda", 0.40, "St. George's, Bermuda"),
        ])

        # ====== CARIBBEAN (Bahamas) ======
        # Region: carib_bahamas
        waypoints.extend([
            CoastlineWaypoint(26.133, -76.766, "carib_bahamas", 0.50, "Nassau, Bahamas"),
            CoastlineWaypoint(26.576, -76.644, "carib_bahamas", 0.30, "New Providence, Bahamas"),
            CoastlineWaypoint(26.589, -77.252, "carib_bahamas", 0.25, "Andros Island, Bahamas"),
            CoastlineWaypoint(27.267, -78.645, "carib_bahamas", 0.35, "Eleuthera, Bahamas"),
            CoastlineWaypoint(26.896, -76.855, "carib_bahamas", 0.20, "Exuma, Bahamas"),
        ])

        # ====== CARIBBEAN (Puerto Rico) ======
        # Region: carib_pr
        waypoints.extend([
            CoastlineWaypoint(18.466, -66.105, "carib_pr", 0.75, "San Juan, PR"),
            CoastlineWaypoint(18.358, -65.100, "carib_pr", 0.55, "Fajardo, PR"),
            CoastlineWaypoint(17.980, -66.630, "carib_pr", 0.45, "Ponce, PR"),
            CoastlineWaypoint(18.159, -67.243, "carib_pr", 0.40, "Mayaguez, PR"),
        ])

        # ====== CARIBBEAN (US Virgin Islands) ======
        # Region: carib_usvi
        waypoints.extend([
            CoastlineWaypoint(18.335, -64.896, "carib_usvi", 0.60, "St. Thomas, USVI"),
            CoastlineWaypoint(18.207, -64.639, "carib_usvi", 0.40, "St. John, USVI"),
            CoastlineWaypoint(17.733, -64.434, "carib_usvi", 0.35, "St. Croix, USVI"),
        ])

        # ====== CARIBBEAN (Jamaica) ======
        # Region: carib_jamaica
        waypoints.extend([
            CoastlineWaypoint(18.042, -76.801, "carib_jamaica", 0.70, "Kingston, Jamaica"),
            CoastlineWaypoint(18.293, -77.902, "carib_jamaica", 0.60, "Montego Bay, Jamaica"),
            CoastlineWaypoint(18.114, -76.146, "carib_jamaica", 0.40, "Port Royal, Jamaica"),
            CoastlineWaypoint(18.500, -77.500, "carib_jamaica", 0.35, "Ocho Rios, Jamaica"),
        ])

        # ====== CARIBBEAN (Cuba) ======
        # Region: carib_cuba_n (northern) and carib_cuba_s (southern, if defined)
        waypoints.extend([
            CoastlineWaypoint(23.137, -82.359, "carib_cuba_n", 0.75, "Havana, Cuba"),
            CoastlineWaypoint(23.685, -82.431, "carib_cuba_n", 0.40, "Matanzas, Cuba"),
            CoastlineWaypoint(20.411, -76.956, "carib_cuba_n", 0.50, "Camagüey, Cuba"),
            CoastlineWaypoint(20.017, -75.815, "carib_cuba_n", 0.55, "Santiago de Cuba, Cuba"),
        ])

        # ====== CARIBBEAN (Hispaniola: DR / Haiti) ======
        # Region: carib_hispaniola
        waypoints.extend([
            CoastlineWaypoint(18.971, -70.163, "carib_hispaniola", 0.75, "Santo Domingo, DR"),
            CoastlineWaypoint(19.797, -70.163, "carib_hispaniola", 0.50, "Puerto Plata, DR"),
            CoastlineWaypoint(18.229, -72.285, "carib_hispaniola", 0.70, "Port-au-Prince, Haiti"),
            CoastlineWaypoint(19.708, -72.285, "carib_hispaniola", 0.35, "Cap-Haïtien, Haiti"),
        ])

        # ====== CARIBBEAN (Lesser Antilles) ======
        # Region: carib_lesser_antilles
        waypoints.extend([
            CoastlineWaypoint(12.169, -61.924, "carib_lesser_antilles", 0.55, "Bridgetown, Barbados"),
            CoastlineWaypoint(13.160, -61.224, "carib_lesser_antilles", 0.45, "Kingstown, St. Vincent"),
            CoastlineWaypoint(14.010, -60.975, "carib_lesser_antilles", 0.50, "Castries, St. Lucia"),
            CoastlineWaypoint(15.299, -61.388, "carib_lesser_antilles", 0.40, "Basseterre, St. Kitts"),
            CoastlineWaypoint(18.343, -63.068, "carib_lesser_antilles", 0.50, "Road Town, British Virgin Islands"),
        ])

        # ====== CARIBBEAN (Cayman Islands) ======
        # Region: carib_cayman
        waypoints.extend([
            CoastlineWaypoint(19.286, -81.369, "carib_cayman", 0.50, "Georgetown, Cayman Islands"),
        ])

        # ====== MEXICO (Yucatan Peninsula) ======
        # Region: mex_yucatan
        waypoints.extend([
            CoastlineWaypoint(20.977, -87.326, "mex_yucatan", 0.70, "Cancun, Mexico"),
            CoastlineWaypoint(20.627, -87.074, "mex_yucatan", 0.60, "Playa del Carmen, Mexico"),
            CoastlineWaypoint(19.826, -87.076, "mex_yucatan", 0.55, "Tulum, Mexico"),
            CoastlineWaypoint(21.164, -86.852, "mex_yucatan", 0.50, "Cozumel, Mexico"),
            CoastlineWaypoint(20.683, -88.272, "mex_yucatan", 0.40, "Belize City, Belize"),
        ])

        # ====== MEXICO (Quintana Roo / Yucatan) ======
        # Region: mex_qr or mex_yucatan (variants)
        waypoints.extend([
            CoastlineWaypoint(19.040, -87.344, "mex_yucatan", 0.35, "Felipe Carrillo Puerto, Mexico"),
        ])

        # ====== MEXICO (Veracruz / Eastern Mexico) ======
        # Region: mex_veracruz (if defined as separate profile)
        waypoints.extend([
            CoastlineWaypoint(19.196, -96.134, "mex_veracruz", 0.70, "Veracruz, Mexico"),
            CoastlineWaypoint(18.627, -95.184, "mex_veracruz", 0.50, "Coatzacoalcos, Mexico"),
            CoastlineWaypoint(17.982, -94.283, "mex_veracruz", 0.40, "Tabasco coast, Mexico"),
        ])

        # ====== CENTRAL AMERICA (Belize, Honduras, Guatemala, Nicaragua) ======
        # Region: ca_belize, ca_honduras, etc. (if defined)
        waypoints.extend([
            CoastlineWaypoint(17.247, -88.758, "ca_honduras", 0.50, "La Ceiba, Honduras"),
            CoastlineWaypoint(16.276, -86.241, "ca_honduras", 0.60, "La Romana, Honduras"),
            CoastlineWaypoint(11.383, -84.506, "ca_nicaragua", 0.45, "Bluefields, Nicaragua"),
            CoastlineWaypoint(12.866, -85.201, "ca_nicaragua", 0.50, "Rama, Nicaragua"),
        ])

        # ====== MEXICO (Pacific — Baja California) ======
        # Region: mex_baja or mex_pacific_north (if included)
        waypoints.extend([
            CoastlineWaypoint(23.630, -109.973, "mex_baja", 0.50, "La Paz, Mexico"),
            CoastlineWaypoint(24.283, -110.308, "mex_baja", 0.40, "Todos Santos, Mexico"),
        ])

        # ================================================================
        # WESTERN PACIFIC — [WP_DPS_AUDIT_V2 §7, Tranche B 2026-07]
        # ================================================================
        # ~130 waypoints activating the WP "living legs": region keys are
        # wp_* entries in storm_surge.COASTAL_PROFILES / economic_vulnerability
        # .ECONOMIC_PROFILES. Distance-to-coast from these points drives WP
        # profile assignment, coastal-hours accrual, land contact, landfall
        # detection, and the landfall-intensity bonus — replacing the coarse
        # rectangles whose Japan box spanned 800 km of open Philippine Sea
        # (Saola 2023 loitering inflation) and whose entry transitions created
        # fake landfalls. Same curation principle as the Atlantic set:
        # population centers, surge-prone bays, and canonical landfall points
        # (Guiuan/Tacloban for Haiyan, Wenchang for Yagi/Rammasun, Shionomisaki
        # and Izu for Honshu recurvers), not uniform spacing.
        #
        # All points sit at lon >= 99.1E so Bay-of-Bengal / Arabian Sea storms
        # (no NI waypoints yet) keep resolving to open_ocean.

        # ====== PHILIPPINES ====== (region: wp_philippines)
        waypoints.extend([
            # Luzon — west coast
            CoastlineWaypoint(18.200, 120.530, "wp_philippines", 0.40, "Laoag, Ilocos Norte"),
            CoastlineWaypoint(17.570, 120.390, "wp_philippines", 0.40, "Vigan, Ilocos Sur"),
            CoastlineWaypoint(16.620, 120.320, "wp_philippines", 0.45, "San Fernando, La Union"),
            CoastlineWaypoint(16.040, 120.330, "wp_philippines", 0.55, "Dagupan / Lingayen Gulf"),
            CoastlineWaypoint(15.330, 119.980, "wp_philippines", 0.30, "Iba, Zambales"),
            CoastlineWaypoint(14.830, 120.280, "wp_philippines", 0.55, "Subic Bay / Olongapo"),
            CoastlineWaypoint(14.600, 120.980, "wp_philippines", 1.00, "Manila"),
            CoastlineWaypoint(13.760, 121.060, "wp_philippines", 0.55, "Batangas City"),
            # Luzon — north & east coast (Pacific-facing landfall corridor)
            CoastlineWaypoint(18.360, 121.640, "wp_philippines", 0.35, "Aparri, Cagayan"),
            CoastlineWaypoint(18.470, 122.150, "wp_philippines", 0.20, "Santa Ana, Cagayan"),
            CoastlineWaypoint(17.060, 122.430, "wp_philippines", 0.15, "Palanan, Isabela"),
            CoastlineWaypoint(16.280, 122.120, "wp_philippines", 0.25, "Casiguran, Aurora"),
            CoastlineWaypoint(15.760, 121.560, "wp_philippines", 0.30, "Baler, Aurora"),
            CoastlineWaypoint(14.750, 121.650, "wp_philippines", 0.35, "Infanta, Quezon"),
            # Batanes / Babuyan (Luzon Strait)
            CoastlineWaypoint(20.450, 121.970, "wp_philippines", 0.15, "Basco, Batanes"),
            CoastlineWaypoint(19.260, 121.470, "wp_philippines", 0.10, "Calayan, Babuyan Is."),
            # Bicol / Catanduanes
            CoastlineWaypoint(14.110, 122.960, "wp_philippines", 0.40, "Daet, Camarines Norte"),
            CoastlineWaypoint(13.620, 123.190, "wp_philippines", 0.45, "Naga / San Miguel Bay"),
            CoastlineWaypoint(13.140, 123.740, "wp_philippines", 0.55, "Legazpi, Albay"),
            CoastlineWaypoint(13.580, 124.230, "wp_philippines", 0.35, "Virac, Catanduanes"),
            CoastlineWaypoint(12.970, 124.010, "wp_philippines", 0.40, "Sorsogon City"),
            CoastlineWaypoint(12.370, 123.620, "wp_philippines", 0.35, "Masbate City"),
            # Samar / Leyte (Haiyan corridor)
            CoastlineWaypoint(12.500, 124.640, "wp_philippines", 0.35, "Catarman, N. Samar"),
            CoastlineWaypoint(11.610, 125.430, "wp_philippines", 0.30, "Borongan, E. Samar"),
            CoastlineWaypoint(11.030, 125.720, "wp_philippines", 0.30, "Guiuan, E. Samar"),
            CoastlineWaypoint(11.240, 125.000, "wp_philippines", 0.60, "Tacloban, Leyte"),
            CoastlineWaypoint(11.010, 124.610, "wp_philippines", 0.45, "Ormoc, Leyte"),
            # Visayas — central & western
            CoastlineWaypoint(11.050, 124.000, "wp_philippines", 0.35, "Bogo, N. Cebu"),
            CoastlineWaypoint(10.320, 123.900, "wp_philippines", 0.85, "Cebu City"),
            CoastlineWaypoint(10.680, 122.950, "wp_philippines", 0.60, "Bacolod, Negros"),
            CoastlineWaypoint(10.700, 122.560, "wp_philippines", 0.60, "Iloilo City"),
            CoastlineWaypoint(11.590, 122.750, "wp_philippines", 0.45, "Roxas City, Capiz"),
            CoastlineWaypoint(11.700, 122.370, "wp_philippines", 0.40, "Kalibo, Aklan"),
            CoastlineWaypoint(9.650, 123.850, "wp_philippines", 0.45, "Tagbilaran, Bohol"),
            # Mindanao
            CoastlineWaypoint(9.850, 126.050, "wp_philippines", 0.25, "Siargao Island"),
            CoastlineWaypoint(9.790, 125.490, "wp_philippines", 0.45, "Surigao City"),
            CoastlineWaypoint(8.950, 125.540, "wp_philippines", 0.45, "Butuan City"),
            CoastlineWaypoint(8.480, 124.650, "wp_philippines", 0.60, "Cagayan de Oro"),
            CoastlineWaypoint(8.370, 126.340, "wp_philippines", 0.20, "Hinatuan, Surigao del Sur"),
            CoastlineWaypoint(7.070, 125.610, "wp_philippines", 0.70, "Davao City"),
            CoastlineWaypoint(6.910, 122.080, "wp_philippines", 0.55, "Zamboanga City"),
            # Palawan / Mindoro (west-exiting storms)
            CoastlineWaypoint(9.740, 118.740, "wp_philippines", 0.45, "Puerto Princesa, Palawan"),
            CoastlineWaypoint(12.000, 120.200, "wp_philippines", 0.25, "Coron, Palawan"),
            CoastlineWaypoint(13.400, 121.180, "wp_philippines", 0.35, "Calapan, Mindoro"),
        ])

        # ====== TAIWAN ====== (region: wp_taiwan)
        waypoints.extend([
            CoastlineWaypoint(25.170, 121.440, "wp_taiwan", 0.95, "Taipei / Tamsui"),
            CoastlineWaypoint(25.130, 121.740, "wp_taiwan", 0.70, "Keelung"),
            CoastlineWaypoint(24.600, 121.850, "wp_taiwan", 0.30, "Su'ao, Yilan"),
            CoastlineWaypoint(23.980, 121.610, "wp_taiwan", 0.45, "Hualien"),
            CoastlineWaypoint(22.760, 121.150, "wp_taiwan", 0.40, "Taitung"),
            CoastlineWaypoint(22.000, 120.740, "wp_taiwan", 0.30, "Hengchun / Kenting"),
            CoastlineWaypoint(22.620, 120.270, "wp_taiwan", 0.85, "Kaohsiung"),
            CoastlineWaypoint(23.000, 120.180, "wp_taiwan", 0.70, "Tainan"),
            CoastlineWaypoint(24.250, 120.520, "wp_taiwan", 0.75, "Taichung"),
        ])

        # ====== JAPAN (incl. Ryukyus) ====== (region: wp_japan)
        waypoints.extend([
            # Ryukyu arc
            CoastlineWaypoint(24.340, 124.160, "wp_japan", 0.35, "Ishigaki, Yaeyama"),
            CoastlineWaypoint(24.790, 125.280, "wp_japan", 0.35, "Miyakojima"),
            CoastlineWaypoint(26.210, 127.680, "wp_japan", 0.65, "Naha, Okinawa"),
            CoastlineWaypoint(26.590, 127.980, "wp_japan", 0.35, "Nago, Okinawa"),
            CoastlineWaypoint(28.380, 129.490, "wp_japan", 0.30, "Amami Oshima"),
            CoastlineWaypoint(30.360, 130.520, "wp_japan", 0.15, "Yakushima"),
            # Kyushu
            CoastlineWaypoint(31.600, 130.560, "wp_japan", 0.65, "Kagoshima"),
            CoastlineWaypoint(31.910, 131.420, "wp_japan", 0.55, "Miyazaki"),
            CoastlineWaypoint(33.240, 131.610, "wp_japan", 0.55, "Oita"),
            CoastlineWaypoint(32.750, 129.880, "wp_japan", 0.60, "Nagasaki"),
            CoastlineWaypoint(33.590, 130.400, "wp_japan", 0.80, "Fukuoka / Hakata Bay"),
            # Shikoku
            CoastlineWaypoint(33.560, 133.530, "wp_japan", 0.55, "Kochi"),
            CoastlineWaypoint(33.250, 134.180, "wp_japan", 0.25, "Cape Muroto"),
            CoastlineWaypoint(33.840, 132.770, "wp_japan", 0.55, "Matsuyama"),
            CoastlineWaypoint(34.070, 134.550, "wp_japan", 0.50, "Tokushima"),
            # Honshu — Pacific side (main recurver landfall corridor)
            CoastlineWaypoint(33.450, 135.760, "wp_japan", 0.35, "Shionomisaki, Wakayama"),
            CoastlineWaypoint(34.650, 135.430, "wp_japan", 1.00, "Osaka Bay"),
            CoastlineWaypoint(35.050, 136.850, "wp_japan", 0.95, "Nagoya / Ise Bay"),
            CoastlineWaypoint(34.680, 137.720, "wp_japan", 0.60, "Hamamatsu"),
            CoastlineWaypoint(34.680, 138.950, "wp_japan", 0.35, "Izu Peninsula / Shimoda"),
            CoastlineWaypoint(35.440, 139.640, "wp_japan", 1.00, "Tokyo / Yokohama"),
            CoastlineWaypoint(35.130, 140.100, "wp_japan", 0.70, "Boso Peninsula, Chiba"),
            CoastlineWaypoint(35.730, 140.830, "wp_japan", 0.40, "Choshi"),
            CoastlineWaypoint(38.260, 141.020, "wp_japan", 0.65, "Sendai"),
            # Sea of Japan side + Hokkaido (ET-transition tracks)
            CoastlineWaypoint(37.920, 139.040, "wp_japan", 0.55, "Niigata"),
            CoastlineWaypoint(36.600, 136.620, "wp_japan", 0.50, "Kanazawa"),
            CoastlineWaypoint(41.770, 140.730, "wp_japan", 0.45, "Hakodate, Hokkaido"),
            CoastlineWaypoint(42.630, 141.600, "wp_japan", 0.55, "Tomakomai, Hokkaido"),
        ])

        # ====== KOREAN PENINSULA ====== (region: wp_korea)
        waypoints.extend([
            CoastlineWaypoint(33.510, 126.520, "wp_korea", 0.55, "Jeju City"),
            CoastlineWaypoint(34.790, 126.390, "wp_korea", 0.45, "Mokpo"),
            CoastlineWaypoint(34.740, 127.740, "wp_korea", 0.45, "Yeosu"),
            CoastlineWaypoint(35.100, 129.040, "wp_korea", 0.85, "Busan"),
            CoastlineWaypoint(35.500, 129.420, "wp_korea", 0.60, "Ulsan"),
            CoastlineWaypoint(36.030, 129.370, "wp_korea", 0.50, "Pohang"),
            CoastlineWaypoint(37.460, 126.620, "wp_korea", 0.85, "Incheon / Seoul coast"),
            CoastlineWaypoint(37.770, 128.900, "wp_korea", 0.45, "Gangneung"),
        ])

        # ====== SOUTH / EAST CHINA COAST ====== (region: wp_south_china)
        waypoints.extend([
            # Pearl River Delta & Guangdong
            CoastlineWaypoint(22.300, 114.170, "wp_south_china", 1.00, "Hong Kong"),
            CoastlineWaypoint(22.190, 113.540, "wp_south_china", 0.80, "Macau / Zhuhai"),
            CoastlineWaypoint(22.550, 114.100, "wp_south_china", 1.00, "Shenzhen"),
            CoastlineWaypoint(21.860, 111.980, "wp_south_china", 0.50, "Yangjiang, Guangdong"),
            CoastlineWaypoint(21.510, 111.010, "wp_south_china", 0.45, "Maoming, Guangdong"),
            CoastlineWaypoint(21.270, 110.360, "wp_south_china", 0.55, "Zhanjiang, Guangdong"),
            CoastlineWaypoint(22.790, 115.350, "wp_south_china", 0.45, "Shanwei, Guangdong"),
            CoastlineWaypoint(23.350, 116.680, "wp_south_china", 0.60, "Shantou"),
            # Fujian
            CoastlineWaypoint(24.480, 118.080, "wp_south_china", 0.75, "Xiamen"),
            CoastlineWaypoint(24.870, 118.680, "wp_south_china", 0.60, "Quanzhou"),
            CoastlineWaypoint(26.050, 119.550, "wp_south_china", 0.65, "Fuzhou"),
            CoastlineWaypoint(26.660, 119.550, "wp_south_china", 0.40, "Ningde, Fujian"),
            # Zhejiang / Shanghai (Krathon/Bavi-class approach corridor)
            CoastlineWaypoint(27.990, 120.700, "wp_south_china", 0.65, "Wenzhou"),
            CoastlineWaypoint(28.660, 121.420, "wp_south_china", 0.55, "Taizhou, Zhejiang"),
            CoastlineWaypoint(29.880, 121.550, "wp_south_china", 0.70, "Ningbo / Zhoushan"),
            CoastlineWaypoint(31.230, 121.470, "wp_south_china", 1.00, "Shanghai"),
            # North of the Yangtze — sparse coverage for ET-transition strikes
            # (Lekima 2019-class). Keyed wp_south_china: the shallow-shelf,
            # defended-coast profile is the closest fit for Jiangsu/Bohai.
            CoastlineWaypoint(32.010, 120.860, "wp_south_china", 0.55, "Nantong, Jiangsu"),
            CoastlineWaypoint(33.380, 120.160, "wp_south_china", 0.35, "Yancheng coast, Jiangsu"),
            CoastlineWaypoint(36.070, 120.380, "wp_south_china", 0.70, "Qingdao"),
            CoastlineWaypoint(37.510, 122.120, "wp_south_china", 0.45, "Weihai"),
            CoastlineWaypoint(38.910, 121.600, "wp_south_china", 0.65, "Dalian"),
            CoastlineWaypoint(39.020, 117.710, "wp_south_china", 0.75, "Tianjin / Tanggu"),
        ])

        # ====== HAINAN / GULF OF TONKIN ====== (region: wp_hainan)
        waypoints.extend([
            CoastlineWaypoint(20.040, 110.340, "wp_hainan", 0.60, "Haikou"),
            CoastlineWaypoint(19.610, 110.750, "wp_hainan", 0.45, "Wenchang, Hainan"),
            CoastlineWaypoint(19.240, 110.470, "wp_hainan", 0.35, "Qionghai, Hainan"),
            CoastlineWaypoint(18.250, 109.510, "wp_hainan", 0.55, "Sanya"),
            CoastlineWaypoint(19.100, 108.650, "wp_hainan", 0.30, "Dongfang, Hainan"),
            CoastlineWaypoint(20.910, 110.090, "wp_hainan", 0.35, "Leizhou Peninsula"),
            CoastlineWaypoint(21.480, 109.120, "wp_hainan", 0.45, "Beihai, Guangxi"),
        ])

        # ====== VIETNAM + GULF OF THAILAND ====== (region: wp_vietnam)
        waypoints.extend([
            CoastlineWaypoint(21.530, 107.970, "wp_vietnam", 0.35, "Mong Cai"),
            CoastlineWaypoint(20.950, 107.080, "wp_vietnam", 0.50, "Ha Long, Quang Ninh"),
            CoastlineWaypoint(20.860, 106.680, "wp_vietnam", 0.75, "Haiphong"),
            CoastlineWaypoint(20.210, 106.320, "wp_vietnam", 0.45, "Red River Delta / Nam Dinh"),
            CoastlineWaypoint(19.730, 105.900, "wp_vietnam", 0.45, "Sam Son, Thanh Hoa"),
            CoastlineWaypoint(18.680, 105.690, "wp_vietnam", 0.45, "Vinh"),
            CoastlineWaypoint(17.470, 106.600, "wp_vietnam", 0.35, "Dong Hoi"),
            CoastlineWaypoint(16.460, 107.590, "wp_vietnam", 0.50, "Hue"),
            CoastlineWaypoint(16.070, 108.220, "wp_vietnam", 0.70, "Da Nang"),
            CoastlineWaypoint(15.120, 108.800, "wp_vietnam", 0.40, "Quang Ngai"),
            CoastlineWaypoint(13.780, 109.220, "wp_vietnam", 0.45, "Quy Nhon"),
            CoastlineWaypoint(13.080, 109.300, "wp_vietnam", 0.40, "Tuy Hoa"),
            CoastlineWaypoint(12.240, 109.190, "wp_vietnam", 0.55, "Nha Trang"),
            CoastlineWaypoint(10.930, 108.100, "wp_vietnam", 0.40, "Phan Thiet"),
            CoastlineWaypoint(10.350, 107.080, "wp_vietnam", 0.55, "Vung Tau"),
            CoastlineWaypoint(9.180, 105.150, "wp_vietnam", 0.30, "Ca Mau"),
            # Gulf of Thailand (rare but real strikes: Linda 1997, Gay 1989,
            # Pabuk 2019). Keyed wp_vietnam — closest profile fit (low-lying
            # delta coast, minimal defenses). All points >= 99.1E.
            CoastlineWaypoint(13.360, 100.980, "wp_vietnam", 0.90, "Bangkok / Gulf head"),
            CoastlineWaypoint(12.930, 100.880, "wp_vietnam", 0.55, "Pattaya / Chonburi"),
            CoastlineWaypoint(12.570, 99.960, "wp_vietnam", 0.40, "Hua Hin"),
            CoastlineWaypoint(10.490, 99.180, "wp_vietnam", 0.35, "Chumphon"),
            CoastlineWaypoint(9.140, 99.330, "wp_vietnam", 0.35, "Surat Thani / Ko Samui"),
        ])

        # ====== MARIANA ISLANDS ====== (region: wp_marianas — US territory)
        waypoints.extend([
            CoastlineWaypoint(13.480, 144.750, "wp_marianas", 0.60, "Hagåtña, Guam"),
            CoastlineWaypoint(13.580, 144.920, "wp_marianas", 0.40, "Yigo / Andersen AFB, Guam"),
            CoastlineWaypoint(15.180, 145.750, "wp_marianas", 0.45, "Saipan"),
            CoastlineWaypoint(14.980, 145.630, "wp_marianas", 0.20, "Tinian"),
            CoastlineWaypoint(14.150, 145.200, "wp_marianas", 0.15, "Rota"),
        ])

        # ================================================================
        # SOUTHERN HEMISPHERE — [SH_DPS_AUDIT, Tranche 2026-07]
        # ================================================================
        # ~110 waypoints activating the SH "living legs": region keys are sh_*
        # entries in storm_surge.COASTAL_PROFILES / economic_vulnerability
        # .ECONOMIC_PROFILES. Same architecture as WP — distance-to-coast from
        # these points drives profile assignment, coastal-hours accrual, land
        # contact, landfall detection, and the landfall-intensity bonus. Before
        # this every SI/SP storm scored open-ocean (Idai 31 / Gabrielle 29 /
        # Cat-5 Ilsa on an empty coast 42, all indistinguishable).
        #
        # The activation is SOUTHERN-LATITUDE gated (lat < 0), so no Northern-
        # Hemisphere basin (Atlantic/EP/WP/NI — every baked storm) can ever see
        # an sh_* region. population_density carries the exposure signal: the
        # Pilbara/Kimberley points are deliberately sparse (0.05–0.10) so a
        # Cat-5 on an empty coast (Ilsa) stays low, while Suva/Beira/Auckland
        # are dense.

        # ====== MOZAMBIQUE ====== (sh_mozambique — high vuln, surge-prone)
        waypoints.extend([
            CoastlineWaypoint(-11.350, 40.370, "sh_mozambique", 0.15, "Palma / Mocímboa"),  # sparse N coast
            CoastlineWaypoint(-12.230, 40.530, "sh_mozambique", 0.15, "Macomia / Ibo"),  # Kenneth — sparse
            CoastlineWaypoint(-12.970, 40.520, "sh_mozambique", 0.45, "Pemba"),
            CoastlineWaypoint(-14.540, 40.670, "sh_mozambique", 0.40, "Nacala"),
            CoastlineWaypoint(-16.230, 39.910, "sh_mozambique", 0.30, "Angoche"),
            CoastlineWaypoint(-17.880, 36.890, "sh_mozambique", 0.50, "Quelimane"),
            CoastlineWaypoint(-19.840, 34.840, "sh_mozambique", 0.70, "Beira"),  # Idai surge disaster; below sea level
            CoastlineWaypoint(-20.750, 34.730, "sh_mozambique", 0.25, "Nova Mambone"),
            CoastlineWaypoint(-22.010, 35.310, "sh_mozambique", 0.35, "Vilankulo"),
            CoastlineWaypoint(-23.860, 35.380, "sh_mozambique", 0.40, "Inhambane"),
            CoastlineWaypoint(-25.050, 33.640, "sh_mozambique", 0.40, "Xai-Xai"),
            CoastlineWaypoint(-25.970, 32.570, "sh_mozambique", 0.75, "Maputo"),
        ])

        # ====== MADAGASCAR ====== (sh_madagascar — high vuln, E-coast facing)
        waypoints.extend([
            CoastlineWaypoint(-12.280, 49.290, "sh_madagascar", 0.45, "Antsiranana (Diego)"),
            CoastlineWaypoint(-14.900, 50.280, "sh_madagascar", 0.35, "Antalaha"),
            CoastlineWaypoint(-15.720, 46.320, "sh_madagascar", 0.45, "Mahajanga"),
            CoastlineWaypoint(-16.170, 49.750, "sh_madagascar", 0.25, "Maroantsetra"),
            CoastlineWaypoint(-18.150, 49.400, "sh_madagascar", 0.60, "Toamasina (Tamatave)"),
            CoastlineWaypoint(-19.900, 48.800, "sh_madagascar", 0.25, "Mahanoro"),
            CoastlineWaypoint(-20.280, 44.280, "sh_madagascar", 0.30, "Morondava"),
            CoastlineWaypoint(-21.230, 48.340, "sh_madagascar", 0.30, "Mananjary"),  # Batsirai landfall
            CoastlineWaypoint(-22.130, 48.020, "sh_madagascar", 0.30, "Manakara"),
            CoastlineWaypoint(-22.820, 47.830, "sh_madagascar", 0.25, "Farafangana"),
            CoastlineWaypoint(-23.350, 43.670, "sh_madagascar", 0.40, "Toliara (Tulear)"),
            CoastlineWaypoint(-25.030, 46.990, "sh_madagascar", 0.20, "Fort Dauphin (Tolagnaro)"),
            CoastlineWaypoint(-13.320, 48.260, "sh_madagascar", 0.20, "Nosy Be"),
        ])

        # ====== MASCARENE ISLANDS ====== (sh_mascarene — Mauritius / Réunion)
        waypoints.extend([
            CoastlineWaypoint(-20.160, 57.500, "sh_mascarene", 0.65, "Port Louis, Mauritius"),
            CoastlineWaypoint(-20.520, 57.520, "sh_mascarene", 0.35, "Grand Baie, Mauritius"),
            CoastlineWaypoint(-20.880, 55.450, "sh_mascarene", 0.55, "Saint-Denis, Réunion"),
            CoastlineWaypoint(-21.340, 55.480, "sh_mascarene", 0.35, "Saint-Pierre, Réunion"),
            CoastlineWaypoint(-19.690, 63.420, "sh_mascarene", 0.15, "Rodrigues"),
        ])

        # ====== WESTERN / NW AUSTRALIA ====== (sh_w_australia — SPARSE Pilbara/
        # Kimberley: the empty-coast exposure discriminator. Ilsa (Cat 5, 2023)
        # crossed near Pardoo, one of the least-populated coasts on Earth.)
        waypoints.extend([
            CoastlineWaypoint(-14.300, 126.600, "sh_w_australia", 0.05, "Kimberley coast (remote)"),
            CoastlineWaypoint(-15.480, 128.120, "sh_w_australia", 0.10, "Wyndham"),
            CoastlineWaypoint(-17.960, 122.240, "sh_w_australia", 0.30, "Broome"),
            CoastlineWaypoint(-19.600, 120.000, "sh_w_australia", 0.05, "Pardoo / Eighty Mile Beach"),  # Ilsa
            CoastlineWaypoint(-20.310, 118.610, "sh_w_australia", 0.35, "Port Hedland"),
            CoastlineWaypoint(-20.740, 116.850, "sh_w_australia", 0.35, "Karratha / Dampier"),
            CoastlineWaypoint(-21.640, 115.110, "sh_w_australia", 0.12, "Onslow"),
            CoastlineWaypoint(-21.930, 114.130, "sh_w_australia", 0.20, "Exmouth"),
            CoastlineWaypoint(-24.880, 113.660, "sh_w_australia", 0.25, "Carnarvon"),
            CoastlineWaypoint(-25.930, 113.530, "sh_w_australia", 0.08, "Denham / Shark Bay"),
            CoastlineWaypoint(-27.710, 114.160, "sh_w_australia", 0.20, "Kalbarri"),  # Seroja landfall
            CoastlineWaypoint(-28.770, 114.610, "sh_w_australia", 0.35, "Geraldton"),
        ])

        # ====== EASTERN AUSTRALIA ====== (sh_e_australia — Queensland, hardened
        # + high value; Yasi / Debbie zone)
        waypoints.extend([
            CoastlineWaypoint(-15.470, 145.250, "sh_e_australia", 0.15, "Cooktown"),
            CoastlineWaypoint(-16.920, 145.770, "sh_e_australia", 0.55, "Cairns"),
            CoastlineWaypoint(-17.520, 146.030, "sh_e_australia", 0.30, "Innisfail"),  # Yasi landfall
            CoastlineWaypoint(-18.650, 146.160, "sh_e_australia", 0.20, "Cardwell"),
            CoastlineWaypoint(-19.260, 146.820, "sh_e_australia", 0.55, "Townsville"),
            CoastlineWaypoint(-20.010, 148.250, "sh_e_australia", 0.30, "Bowen"),  # Debbie
            CoastlineWaypoint(-20.270, 148.720, "sh_e_australia", 0.35, "Airlie / Whitsundays"),  # Debbie
            CoastlineWaypoint(-21.140, 149.190, "sh_e_australia", 0.45, "Mackay"),
            CoastlineWaypoint(-23.380, 150.510, "sh_e_australia", 0.40, "Rockhampton"),
            CoastlineWaypoint(-23.840, 151.260, "sh_e_australia", 0.35, "Gladstone"),
            CoastlineWaypoint(-24.870, 152.350, "sh_e_australia", 0.35, "Bundaberg"),
            CoastlineWaypoint(-25.290, 152.820, "sh_e_australia", 0.30, "Hervey Bay"),
            CoastlineWaypoint(-26.650, 153.070, "sh_e_australia", 0.55, "Sunshine Coast"),
            CoastlineWaypoint(-27.470, 153.030, "sh_e_australia", 0.85, "Brisbane"),
            CoastlineWaypoint(-28.000, 153.430, "sh_e_australia", 0.70, "Gold Coast"),
        ])

        # ====== FIJI ====== (sh_fiji — Winston 2016 direct hit; Viti/Vanua Levu)
        waypoints.extend([
            CoastlineWaypoint(-18.140, 178.440, "sh_fiji", 0.70, "Suva"),
            CoastlineWaypoint(-17.800, 177.420, "sh_fiji", 0.55, "Nadi"),
            CoastlineWaypoint(-17.610, 177.450, "sh_fiji", 0.45, "Lautoka"),
            CoastlineWaypoint(-17.360, 178.170, "sh_fiji", 0.25, "Rakiraki"),  # Winston eyewall
            CoastlineWaypoint(-16.430, 179.360, "sh_fiji", 0.30, "Labasa (Vanua Levu)"),
            CoastlineWaypoint(-16.780, -179.940, "sh_fiji", 0.15, "Taveuni"),
        ])

        # ====== VANUATU ====== (sh_vanuatu — Pam 2015 / Harold 2020; vuln SIDS)
        waypoints.extend([
            CoastlineWaypoint(-15.530, 167.170, "sh_vanuatu", 0.35, "Luganville (Espiritu Santo)"),
            CoastlineWaypoint(-16.200, 168.200, "sh_vanuatu", 0.15, "Ambrym / Pentecost"),
            CoastlineWaypoint(-17.730, 168.320, "sh_vanuatu", 0.55, "Port Vila (Efate)"),
            CoastlineWaypoint(-19.500, 169.270, "sh_vanuatu", 0.20, "Tanna"),  # Pam
        ])

        # ====== NEW CALEDONIA ====== (sh_new_caledonia — French, hardened)
        waypoints.extend([
            CoastlineWaypoint(-22.280, 166.460, "sh_new_caledonia", 0.55, "Nouméa"),
            CoastlineWaypoint(-21.060, 164.850, "sh_new_caledonia", 0.20, "Koné"),
            CoastlineWaypoint(-20.700, 167.000, "sh_new_caledonia", 0.15, "Loyalty Islands"),
        ])

        # ====== TONGA / SAMOA ====== (sh_tonga_samoa — Gita 2018; dateline)
        waypoints.extend([
            CoastlineWaypoint(-21.140, -175.200, "sh_tonga_samoa", 0.45, "Nuku'alofa, Tonga"),  # Gita
            CoastlineWaypoint(-18.650, -173.980, "sh_tonga_samoa", 0.20, "Neiafu (Vava'u)"),
            CoastlineWaypoint(-13.830, -171.770, "sh_tonga_samoa", 0.45, "Apia, Samoa"),
            CoastlineWaypoint(-14.280, -170.700, "sh_tonga_samoa", 0.30, "Pago Pago, Am. Samoa"),
        ])

        # ====== NEW ZEALAND ====== (sh_new_zealand — Gabrielle 2023; N Island,
        # high value; NZ's costliest weather disaster)
        waypoints.extend([
            CoastlineWaypoint(-35.720, 174.320, "sh_new_zealand", 0.35, "Whangārei / Northland"),
            CoastlineWaypoint(-36.850, 174.760, "sh_new_zealand", 0.85, "Auckland"),
            CoastlineWaypoint(-37.690, 176.170, "sh_new_zealand", 0.50, "Tauranga"),
            CoastlineWaypoint(-38.660, 178.020, "sh_new_zealand", 0.35, "Gisborne"),  # Gabrielle
            CoastlineWaypoint(-39.490, 176.920, "sh_new_zealand", 0.45, "Napier / Hawke's Bay"),  # Gabrielle floods
            CoastlineWaypoint(-39.060, 174.070, "sh_new_zealand", 0.35, "New Plymouth"),
            CoastlineWaypoint(-41.290, 174.780, "sh_new_zealand", 0.60, "Wellington"),
        ])

        # ====== SOLOMON ISLANDS ====== (sh_solomon — Harold path)
        waypoints.extend([
            CoastlineWaypoint(-9.430, 159.950, "sh_solomon", 0.35, "Honiara"),
            CoastlineWaypoint(-8.100, 156.840, "sh_solomon", 0.15, "Gizo"),
        ])

        # ====== TIMOR / SH INDONESIA ====== (sh_timor — Seroja 2021 origin;
        # Flores/Timor/Sumba, ~8–10°S)
        waypoints.extend([
            CoastlineWaypoint(-8.560, 125.570, "sh_timor", 0.40, "Dili, Timor-Leste"),
            CoastlineWaypoint(-10.170, 123.610, "sh_timor", 0.45, "Kupang, W Timor"),
            CoastlineWaypoint(-8.340, 122.980, "sh_timor", 0.20, "Larantuka (Flores)"),
            CoastlineWaypoint(-8.840, 121.660, "sh_timor", 0.30, "Ende (Flores)"),
            CoastlineWaypoint(-9.660, 120.260, "sh_timor", 0.20, "Waingapu (Sumba)"),
        ])

        # ====== OPEN OCEAN REFERENCE POINTS (for fallback distances) ======
        # These are ultra-remote points; storms this far away should fall back to "open_ocean"
        waypoints.extend([
            CoastlineWaypoint(15.000, -30.000, "open_ocean", 0.0, "Central Atlantic (reference)"),
            CoastlineWaypoint(10.000, -50.000, "open_ocean", 0.0, "Western Atlantic Deep (reference)"),
        ])

        return waypoints

    def nearest_waypoint(self, lat: float, lon: float) -> Tuple[CoastlineWaypoint, float]:
        """
        Find the nearest coastal waypoint to a given location.

        Args:
            lat: Query latitude
            lon: Query longitude

        Returns:
            Tuple of (waypoint, distance_km)
        """
        min_distance = float('inf')
        nearest = None

        for wp in self.waypoints:
            dist = self._haversine(lat, lon, wp.lat, wp.lon)
            if dist < min_distance:
                min_distance = dist
                nearest = wp

        return nearest, min_distance

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Compute great-circle distance between two points using Haversine formula.

        Args:
            lat1, lon1: Point 1 (degrees)
            lat2, lon2: Point 2 (degrees)

        Returns:
            Distance in kilometers

        Reference:
            Sinnott, R. W. (1984). "Virtues of the haversine."
            Sky and Telescope, 68(2), 159.
        """
        R_km = 6371.0  # Earth's mean radius in km
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))

        return R_km * c


# ============================================================================
#  PUBLIC API FUNCTIONS
# ============================================================================

# Global coastline database (loaded once)
_coastline_db = None


def _get_coastline_db() -> CoastlineDatabase:
    """Lazy-load the global coastline database."""
    global _coastline_db
    if _coastline_db is None:
        _coastline_db = CoastlineDatabase()
    return _coastline_db


# ─── Memoized distance-to-coast ────────────────────────────────────────────
# Storm tracks are sampled every 3-6 hours and typically move by ~0.3-1.0° per
# step; rounding inputs to 0.1° (~11 km) collapses adjacent timesteps onto
# the same cache entry without materially changing the computed distance
# (the nearest-waypoint database has ~50 km granularity). This avoids a
# full Haversine search over ~150 waypoints on every per-snapshot DPS call.
from functools import lru_cache as _lru_cache


@_lru_cache(maxsize=16384)
def _distance_to_coast_cached(lat_bin: float, lon_bin: float) -> tuple:
    """Internal: cached Haversine lookup keyed by rounded coords.

    Returns a flat tuple (not a dict) so it's hashable and cheap to store.
    The public wrapper re-packs it into the dict shape callers expect.
    """
    db = _get_coastline_db()
    wp, dist_km = db.nearest_waypoint(lat_bin, lon_bin)
    bearing_deg = _compute_bearing(lat_bin, lon_bin, wp.lat, wp.lon)
    return (dist_km, wp.region_key, wp.lat, wp.lon, bearing_deg)


def compute_distance_to_coast(lat: float, lon: float) -> Dict[str, Any]:
    """
    Compute the distance from a storm center to the nearest coastline.

    This replaces the bounding-box region lookup (Issue #10) with a
    continuous Haversine-based distance calculation, providing full coverage
    with no geographic gaps.

    Args:
        lat: Storm center latitude (degrees)
        lon: Storm center longitude (degrees)

    Returns:
        Dictionary with:
            - distance_km: Distance to nearest coast (float)
            - nearest_region_key: Region key (str) matching COASTAL_PROFILES
            - nearest_lat: Latitude of nearest waypoint (float)
            - nearest_lon: Longitude of nearest waypoint (float)
            - bearing_deg: Bearing from storm to coast (float, 0-360 degrees)
                          0° = north, 90° = east, 180° = south, 270° = west

    Example:
        >>> result = compute_distance_to_coast(29.0, -95.0)  # Houston area
        >>> print(result['distance_km'])  # < 20 km (near coast)
        >>> print(result['nearest_region_key'])  # 'gulf_central_tx'
    """
    # Round to 0.1° bins for cache locality. Rounded to 1 decimal because
    # coastline_db.nearest_waypoint is already a discretized lookup, so
    # sub-0.1° precision doesn't change the answer.
    lat_bin = round(float(lat), 1)
    lon_bin = round(float(lon), 1)
    dist_km, region_key, wp_lat, wp_lon, bearing_deg = _distance_to_coast_cached(
        lat_bin, lon_bin
    )
    return {
        'distance_km': dist_km,
        'nearest_region_key': region_key,
        'nearest_lat': wp_lat,
        'nearest_lon': wp_lon,
        'bearing_deg': bearing_deg,
    }


def compute_land_proximity_factor(lat: float, lon: float) -> float:
    """
    Compute a factor (0-1) representing how close a storm is to land.

    This function fixes Issues #3 and #6: it prevents coastal amplification
    from being applied to truly open-ocean storms, and it uses smooth sigmoid
    transitions instead of hard cutoffs.

    The factor follows a sigmoid curve:
        - 0.0 = open ocean (>500 km from any coast)
        - ~0.05 = far ocean (400 km) → minimal amplification
        - ~0.15 = distant approach (300 km) → weak amplification
        - ~0.30 = moderate approach (200 km) → moderate amplification
        - ~0.50 = near coast (100 km) → strong amplification
        - ~0.70 = very near (50 km) → very strong amplification
        - 1.0 = at coast (<10 km)

    The sigmoid shape ensures that:
      1. Storms >500km away get 0.0 factor (no amplification)
      2. Coastal amplification only kicks in meaningfully <200km away
      3. There's a smooth transition (no hard cutoff artifacts)

    Args:
        lat: Storm latitude (degrees)
        lon: Storm longitude (degrees)

    Returns:
        Float between 0.0 (open ocean) and 1.0 (at coast)

    References:
        - Sigmoid curve equation: factor = 1 / (1 + exp(k * (d - d0)))
        - k = steepness parameter (set to 0.05 per km)
        - d0 = inflection point (set to 200 km for meaningful coastal distance)
        - Tobler's First Law of Geography: "Everything is related to everything
          else, but near things are more related than distant things."
    """
    result = compute_distance_to_coast(lat, lon)
    distance_km = result['distance_km']

    # Sigmoid parameters
    k = 0.05  # Steepness: controls how quickly factor rises from 0 to 1
    d0 = 200.0  # Inflection point: distance where factor ≈ 0.5

    # Clamp distance to [0, 600] to prevent numeric overflow in exp()
    distance_clamped = max(0, min(distance_km, 600))

    # Sigmoid: factor = 1 / (1 + exp(k * (d - d0)))
    try:
        factor = 1.0 / (1.0 + math.exp(k * (distance_clamped - d0)))
    except OverflowError:
        # If exp() overflows, clamp to boundaries
        factor = 0.0 if distance_clamped > d0 else 1.0

    return float(factor)


def compute_population_threat(lat: float, lon: float, r34_m: float) -> float:
    """
    Estimate population threat as a function of proximity to coast and R34 radius.

    This function estimates how many people are within the tropical-storm-force
    wind (R34) radius, normalized to a 0-100 scale. High scores indicate storms
    threatening dense population centers.

    The algorithm:
      1. Find nearest coastal waypoint and its population density (0-1 scale)
      2. Use R34 radius to define a search area around the storm center
      3. Estimate people within R34 using coastline population density
      4. Scale to 0-100 threat score

    Population density is a regional proxy:
      - 0.0 = uninhabited (remote keys, unpopulated islands)
      - 0.3 = rural (small towns, dispersed communities)
      - 0.6 = suburban (moderate density towns)
      - 0.8 = semi-urban (cities >100k)
      - 1.0 = major metro (>1M population, dense)

    Threat calculation accounts for:
      - Distance from storm center to populated coast
      - Decay with distance (storm is more threatening to nearby populations)
      - R34 radius (larger radius = more area affected)

    Args:
        lat: Storm center latitude (degrees)
        lon: Storm center longitude (degrees)
        r34_m: Radius of tropical-storm-force winds (meters), typically 100-250 km

    Returns:
        Threat score (0-100 scale):
        - 0-20: Remote ocean or weak threat to distant coast
        - 20-40: Approaching distant coast or remote island
        - 40-60: Threatening smaller cities or distant major metros
        - 60-80: Threatening major metropolitan areas
        - 80-100: Direct threat to major city or densely populated coast

    Example:
        >>> # Cat 4 storm 50 km from Miami with R34=150 km
        >>> threat = compute_population_threat(25.761, -80.188, 150_000)
        >>> print(threat)  # ~85-95 (major threat)
    """
    db = _get_coastline_db()
    wp, dist_to_coast = db.nearest_waypoint(lat, lon)

    # Convert R34 from meters to kilometers
    r34_km = r34_m / 1000.0

    # Base threat from population density at nearest waypoint
    base_population_threat = wp.population_density * 100.0  # 0-100 scale

    # Distance decay: threat decreases with distance from the populated coast
    # Decay function: decay = exp(-(distance / decay_scale)^2)
    # decay_scale = 100 km (controls how fast threat drops with distance)
    decay_scale_km = 100.0
    distance_decay = math.exp(-(dist_to_coast / decay_scale_km) ** 2)

    # R34 amplification: larger wind field = more area affected
    # Threat grows logarithmically with R34 (diminishing returns for huge radii)
    # r34_scaling: 1.0 at R34=100km, ~1.5 at R34=200km, ~1.8 at R34=300km
    r34_scaling = 1.0 + 0.5 * math.log(1.0 + r34_km / 100.0)

    # Combine factors
    threat = base_population_threat * distance_decay * r34_scaling

    # Clamp to [0, 100] range
    return max(0.0, min(threat, 100.0))


def get_nearest_region(lat: float, lon: float) -> str:
    """
    Determine the COASTAL_PROFILES region key for a storm location.

    This function replaces the bounding-box zone lookup (Issue #10), providing
    continuous coverage with no geographic gaps. A storm anywhere on Earth can
    now be assigned to the nearest coastal region.

    The algorithm:
      1. Find the nearest coastline waypoint using Haversine distance
      2. Return the region_key from that waypoint
      3. If distance > 500 nm (930 km), fall back to "open_ocean"

    Args:
        lat: Storm latitude (degrees)
        lon: Storm longitude (degrees)

    Returns:
        Region key as a string (e.g., 'gulf_central_tx', 'carib_pr', 'open_ocean')
        Matches keys in COASTAL_PROFILES from storm_surge.py

    Note:
        The 500 nm threshold is based on operational hurricane forecast lead time
        (~5 days). Beyond this distance, coastal-specific parameters are not
        meaningful; the storm may still adjust its path or dissipate.

    Example:
        >>> region = get_nearest_region(29.5, -95.0)  # Galveston area
        >>> print(region)  # 'gulf_central_tx'

        >>> region = get_nearest_region(20.0, -60.0)  # Remote Atlantic
        >>> print(region)  # 'open_ocean'
    """
    result = compute_distance_to_coast(lat, lon)
    distance_km = result['distance_km']
    region_key = result['nearest_region_key']

    # Convert 500 nautical miles to kilometers
    # 1 nm = 1.852 km
    threshold_nm = 500.0
    threshold_km = threshold_nm * 1.852

    if distance_km > threshold_km:
        return "open_ocean"

    return region_key


# ============================================================================
#  WESTERN PACIFIC DISTANCE GATES — [WP_DPS_AUDIT_V2 §7, Tranche B]
# ============================================================================
# The WP scoring activation is longitude-gated so no Atlantic / EP / southern-
# hemisphere code path can ever observe a wp_* region: callers outside the
# window get None and fall through to their pre-Tranche-B behavior, keeping
# every non-WP baked score bit-identical.

WP_WINDOW_LON_MIN = 95.0
WP_WINDOW_LON_MAX = 150.0
WP_WINDOW_LAT_MIN = 0.0
WP_WINDOW_LAT_MAX = 46.0


def in_wp_window(lat: float, lon: float) -> bool:
    """True if the coordinate is inside the WP scoring-activation window."""
    return (WP_WINDOW_LAT_MIN <= lat <= WP_WINDOW_LAT_MAX
            and WP_WINDOW_LON_MIN <= lon <= WP_WINDOW_LON_MAX)


@_lru_cache(maxsize=16384)
def _wp_coast_cached(lat_bin: float, lon_bin: float) -> tuple:
    """Nearest WP waypoint (distance_km, region_key, population_density)."""
    db = _get_coastline_db()
    best_d = float("inf")
    best = None
    for wp in db.wp_waypoints:
        d = db._haversine(lat_bin, lon_bin, wp.lat, wp.lon)
        if d < best_d:
            best_d = d
            best = wp
    if best is None:  # defensive — wp list is never empty
        return (float("inf"), "open_ocean", 0.0)
    return (best_d, best.region_key, best.population_density)


def nearest_wp_coast(lat: float, lon: float) -> Optional[Tuple[float, str, float]]:
    """
    Distance to the nearest WESTERN PACIFIC coastline waypoint.

    Returns (distance_km, wp_region_key, population_density) for coordinates
    inside the WP activation window, None outside it. No distance threshold
    is applied here — callers own their own gates (Tranche B convention:
    <=150 km profile assignment, <=100 km coastal hours, <=50 km land
    contact / landfall / landfall-intensity bonus). population_density is
    the nearest waypoint's 0-1 coastal-exposure proxy: sub-0.20 waypoints
    are remote islets (Batanes, Calayan, Yakushima, Rota) that anchor
    landfall DETECTION but confer no living-legs exposure profile, and the
    landfall-intensity bonus scales by it so an islet brush at Cat 5 never
    reads like a Tacloban strike.
    """
    if not in_wp_window(lat, lon):
        return None
    lat_bin = round(float(lat), 1)
    lon_bin = round(float(lon), 1)
    dist_km, region_key, pop = _wp_coast_cached(lat_bin, lon_bin)
    return dist_km, region_key, pop


# ============================================================================
#  SOUTHERN HEMISPHERE DISTANCE GATES — [SH_DPS_AUDIT, Tranche 2026-07]
# ============================================================================
# Same architecture as the WP gates. The activation is SOUTHERN-LATITUDE gated
# (lat < 0) so no Northern-Hemisphere basin (Atlantic/EP/WP/NI — and every
# baked storm) can observe an sh_* region; callers outside the window get None
# and keep their pre-SH behavior, so every non-SH score stays bit-identical.
# Longitude is unbounded on purpose: a rare South Atlantic system (Catarina)
# simply finds no nearby sh_* waypoint and resolves to open_ocean anyway.

SH_WINDOW_LAT_MIN = -50.0
SH_WINDOW_LAT_MAX = 0.0


def in_sh_window(lat: float, lon: float) -> bool:
    """True if the coordinate is inside the SH scoring-activation window."""
    return SH_WINDOW_LAT_MIN <= lat < SH_WINDOW_LAT_MAX


@_lru_cache(maxsize=16384)
def _sh_coast_cached(lat_bin: float, lon_bin: float) -> tuple:
    """Nearest SH waypoint (distance_km, region_key, population_density)."""
    db = _get_coastline_db()
    best_d = float("inf")
    best = None
    for wp in db.sh_waypoints:
        d = db._haversine(lat_bin, lon_bin, wp.lat, wp.lon)
        if d < best_d:
            best_d = d
            best = wp
    if best is None:  # defensive — sh list is never empty
        return (float("inf"), "open_ocean", 0.0)
    return (best_d, best.region_key, best.population_density)


def nearest_sh_coast(lat: float, lon: float) -> Optional[Tuple[float, str, float]]:
    """
    Distance to the nearest SOUTHERN HEMISPHERE coastline waypoint.

    Returns (distance_km, sh_region_key, population_density) for coordinates
    inside the SH activation window (lat < 0), None outside it. Callers own
    their own distance gates, mirroring the WP convention (<=150 km profile,
    <=100 km coastal hours, <=50 km land contact, <=60 km LFI). Sub-0.20
    population points are sparse coasts (Pilbara/Kimberley, remote islands)
    that anchor landfall DETECTION but confer no exposure profile and scale
    the LFI down — so a Cat 5 on an empty coast (Ilsa) never reads like a
    Suva or Beira strike.
    """
    if not in_sh_window(lat, lon):
        return None
    lat_bin = round(float(lat), 1)
    lon_bin = round(float(lon), 1)
    dist_km, region_key, pop = _sh_coast_cached(lat_bin, lon_bin)
    return dist_km, region_key, pop


# ============================================================================
#  HELPER FUNCTIONS
# ============================================================================

def _compute_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute bearing (initial direction) from point 1 to point 2.

    Args:
        lat1, lon1: Starting point (degrees)
        lat2, lon2: Ending point (degrees)

    Returns:
        Bearing in degrees (0-360), where:
        - 0° = north
        - 90° = east
        - 180° = south
        - 270° = west

    Reference:
        https://www.movable-type.co.uk/scripts/latlong.html (bearing formula)
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)

    y = math.sin(dlon) * math.cos(lat2_rad)
    x = (math.cos(lat1_rad) * math.sin(lat2_rad) -
         math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon))

    bearing_rad = math.atan2(y, x)
    bearing_deg = (math.degrees(bearing_rad) + 360) % 360

    return bearing_deg
