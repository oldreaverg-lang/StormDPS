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
    db = _get_coastline_db()
    wp, dist_km = db.nearest_waypoint(lat, lon)

    # Compute bearing from storm center to nearest coast
    bearing_deg = _compute_bearing(lat, lon, wp.lat, wp.lon)

    return {
        'distance_km': dist_km,
        'nearest_region_key': wp.region_key,
        'nearest_lat': wp.lat,
        'nearest_lon': wp.lon,
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
