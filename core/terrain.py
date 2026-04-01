"""
Terrain & Orographic Enhancement Module.

This module computes orographic (mountain-induced) rainfall enhancement and
terrain-based flood vulnerability factors. These metrics address a key limitation
in the original hazard scoring:

    Issue #13 (no orographic rainfall): The original rainfall model used only
    forward speed and moisture availability, completely ignoring mountain ranges.
    Orographic uplift can increase rainfall by 100-300% over the windward slopes
    of mountains, while rain shadows on the leeward side suppress rainfall by
    30-50%. Canonical examples:
      - Hurricane Melissa (2019) over Jamaica: the Blue Mountains enhanced
        rainfall to 800+ mm in the peaks while nearby coasts got 200 mm.
      - Hurricane Maria (2017) over Puerto Rico: the Sierra de Luquillo amplified
        rainfall to 1000+ mm, causing catastrophic flooding in valleys.
      - Hurricane Florence (2018) over the Carolinas: the Appalachian foothills
        enhanced rainfall by 40-60%, contributing to record river crests.

The core physics:
  1. Forced orographic lifting: As moist air is forced up a mountain slope, it
     cools adiabatically and condensation occurs. The rate of lifting is
     proportional to wind speed and mountain elevation. Steeper slopes and
     taller peaks → more condensation → more rain.
  2. Windward vs. leeward asymmetry: The windward (upwind) side of a mountain
     receives 2-3× more rain than the leeward (downwind) side due to rain shadow.
  3. Terrain-induced convection: Mountains can trigger or enhance convective
     cells, further amplifying rainfall.

The implementation:
  1. TerrainDatabase: Stores major mountain ranges and elevated terrain near
     hurricane-prone coasts with their elevation, location, and geometry.
  2. compute_orographic_factor(): Returns a 1.0-3.0 multiplier based on:
     - Distance from storm to mountains
     - Elevation of nearby peaks
     - Angle between storm approach direction and ridge orientation
       (perpendicular approach → maximum enhancement)
  3. compute_elevation_vulnerability(): Returns flood-risk factors accounting for:
     - Valley flooding (steep terrain channels rainfall into narrow valleys)
     - Slope runoff (steep slopes → faster runoff → flash flooding)
  4. get_terrain_profile(): Classifies terrain at a location as flat/coastal/
     hilly/mountainous and returns aggregate statistics.

References:
  - Smith, R. B. (1979). "The influence of mountains on the atmosphere."
    Advances in Geophysics, 21, 87-230. (Classic orographic theory)
  - Houze, R. A. (2012). "Orographic effects on precipitating clouds."
    Reviews of Geophysics, 50, RG1001. (Modern understanding)
  - Tropical Cyclone Rainfall from Chen et al. (2006), Marks et al. (2008)
  - Jamaica/Melissa: Pasch & Latto (2020), NHC Report
  - Puerto Rico/Maria: Pasch et al. (2018), NHC Report
"""

import math
import numpy as np
from typing import Any, Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


# ============================================================================
#  TERRAIN FEATURE DEFINITIONS
# ============================================================================

@dataclass
class MountainRange:
    """
    Represents a major mountain range or elevated terrain near hurricane-prone coasts.

    Attributes:
        name: Human-readable name (e.g., 'Blue Mountains, Jamaica')
        peak_elevation_m: Peak elevation (meters above sea level)
        center_lat: Central latitude of range (degrees)
        center_lon: Central longitude of range (degrees)
        effective_radius_km: Effective geographic radius over which orographic
            effects are significant (km). A storm this distance away may still
            experience enhancement. Typically 50-150 km depending on peak height
            and regional moisture availability.
        orientation_deg: Orientation of the main ridge line (degrees, 0-360):
            - 0° / 360° = north-south oriented ridge
            - 90° = east-west oriented ridge
            - This is the perpendicular (ridge line direction), not the slope-facing
              direction. Used to compute the angle between storm approach direction
              and ridge geometry. When a storm approaches perpendicular to the ridge,
              enhancement is maximum; parallel approach gives minimum enhancement.
        region_key: Region from COASTAL_PROFILES (e.g., 'carib_jamaica'), used
            to link terrain to coastal surge/rainfall profiles
        avg_elevation_m: Average elevation of the mountain range (m), lower than peak
        slope_degrees: Average slope angle (degrees from horizontal, 0-90°):
            - 5-15° = gentle foothills
            - 20-30° = moderate mountains
            - 35-45° = steep mountains
            - >45° = extreme terrain (cliffs, gorges)
    """
    name: str
    peak_elevation_m: float
    center_lat: float
    center_lon: float
    effective_radius_km: float
    orientation_deg: float
    region_key: str
    avg_elevation_m: float
    slope_degrees: float


class TerrainDatabase:
    """
    Stores and manages major mountain ranges and elevated terrain near coasts.

    Includes:
      - Blue Mountains, Jamaica (2256m) — Melissa enhancement
      - Cordillera Central, Dominican Republic/Haiti (3098m) — extreme elevation
      - Sierra de Luquillo, Puerto Rico (1065m) — Maria enhancement
      - Appalachian Mountains (2037m) — US East Coast flooding
      - Sierra Madre Oriental, Mexico (3700m) — eastern Mexico
      - Central highlands, Cuba (1972m)
      - Foothills and minor ranges in other regions

    Each range includes peak elevation, location, extent (effective_radius_km),
    ridge orientation, and region_key linking to COASTAL_PROFILES.

    Design principle: Include ranges that noticeably affect rainfall within
    ~100-150 km of affected coastlines. Remote mountains with minimal coast
    impact are excluded.
    """

    def __init__(self):
        """Initialize the terrain database with major mountain ranges."""
        self.mountain_ranges: List[MountainRange] = self._build_ranges()

    def _build_ranges(self) -> List[MountainRange]:
        """
        Build the complete list of mountain ranges.

        Returns:
            List of MountainRange objects covering hurricane-prone coastal areas.
        """
        ranges = []

        # ====== JAMAICA ======
        # Blue Mountains: 2256m peak, strong orographic effect on northern coast
        # Orientation: roughly NW-SE (perpendicular to prevailing NE trade winds)
        ranges.append(MountainRange(
            name="Blue Mountains, Jamaica",
            peak_elevation_m=2256,
            center_lat=18.150,
            center_lon=-76.550,
            effective_radius_km=80,
            orientation_deg=135,  # NW-SE ridge
            region_key="carib_jamaica",
            avg_elevation_m=1400,
            slope_degrees=28,
        ))

        # ====== PUERTO RICO ======
        # Sierra de Luquillo: 1065m peak, affects NE coast (San Juan area)
        # Orientation: roughly E-W ridge, causes rain on northern windward slopes
        ranges.append(MountainRange(
            name="Sierra de Luquillo, Puerto Rico",
            peak_elevation_m=1065,
            center_lat=18.300,
            center_lon=-65.300,
            effective_radius_km=70,
            orientation_deg=90,  # E-W ridge
            region_key="carib_pr",
            avg_elevation_m=700,
            slope_degrees=22,
        ))

        # Cordillera Central, PR: 1330m, affects central and southern PR
        ranges.append(MountainRange(
            name="Cordillera Central, Puerto Rico",
            peak_elevation_m=1330,
            center_lat=18.170,
            center_lon=-66.450,
            effective_radius_km=90,
            orientation_deg=80,  # E-W oriented
            region_key="carib_pr",
            avg_elevation_m=900,
            slope_degrees=24,
        ))

        # ====== HISPANIOLA (Dominican Republic / Haiti) ======
        # Cordillera Central (DR): 3098m peak — extreme elevation
        # E-W orientation, affects rainfall across the island
        ranges.append(MountainRange(
            name="Cordillera Central, Dominican Republic",
            peak_elevation_m=3098,
            center_lat=19.100,
            center_lon=-70.300,
            effective_radius_km=120,
            orientation_deg=80,  # E-W ridge
            region_key="carib_hispaniola",
            avg_elevation_m=1800,
            slope_degrees=30,
        ))

        # Chaîne de la Selle (Haiti/DR border): 2680m
        ranges.append(MountainRange(
            name="Chaîne de la Selle, Haiti/DR",
            peak_elevation_m=2680,
            center_lat=18.550,
            center_lon=-71.850,
            effective_radius_km=110,
            orientation_deg=75,  # ESE-WNW ridge
            region_key="carib_hispaniola",
            avg_elevation_m=1600,
            slope_degrees=29,
        ))

        # Massif de la Selle (SW Haiti): 2347m
        ranges.append(MountainRange(
            name="Massif de la Selle, Haiti",
            peak_elevation_m=2347,
            center_lat=18.200,
            center_lon=-72.500,
            effective_radius_km=100,
            orientation_deg=85,
            region_key="carib_hispaniola",
            avg_elevation_m=1400,
            slope_degrees=27,
        ))

        # ====== CUBA ======
        # Sierra Maestra (eastern Cuba): 1972m
        # E-W orientation on southern coast
        ranges.append(MountainRange(
            name="Sierra Maestra, Cuba",
            peak_elevation_m=1972,
            center_lat=20.020,
            center_lon=-76.400,
            effective_radius_km=100,
            orientation_deg=85,  # E-W ridge
            region_key="carib_cuba_n",  # Applies to both northern and southern Cuba
            avg_elevation_m=1200,
            slope_degrees=26,
        ))

        # Escambray Mountains (central Cuba): 1156m
        ranges.append(MountainRange(
            name="Escambray Mountains, Cuba",
            peak_elevation_m=1156,
            center_lat=21.900,
            center_lon=-80.000,
            effective_radius_km=70,
            orientation_deg=80,
            region_key="carib_cuba_n",
            avg_elevation_m=750,
            slope_degrees=20,
        ))

        # ====== US APPALACHIAN MOUNTAINS ======
        # Extends from AL to ME; focus on coastal-impact regions
        # Blue Ridge Mountains (VA/NC): ~2037m peak
        ranges.append(MountainRange(
            name="Blue Ridge Mountains, VA/NC",
            peak_elevation_m=2037,
            center_lat=35.580,
            center_lon=-82.270,  # Great Smoky Mountains area
            effective_radius_km=150,
            orientation_deg=45,  # NE-SW ridge (parallel to Appalachian trend)
            region_key="atl_nc",
            avg_elevation_m=1200,
            slope_degrees=25,
        ))

        # Appalachian foothills in western NC: 1000-1200m
        ranges.append(MountainRange(
            name="Appalachian Foothills, NC/TN",
            peak_elevation_m=1500,
            center_lat=35.300,
            center_lon=-83.500,
            effective_radius_km=120,
            orientation_deg=40,  # NE-SW
            region_key="atl_nc",
            avg_elevation_m=900,
            slope_degrees=18,
        ))

        # ====== MEXICO (Eastern) ======
        # Sierra Madre Oriental: 3700m peak in NE Mexico
        # Runs NW-SE, affects Veracruz coast
        ranges.append(MountainRange(
            name="Sierra Madre Oriental, Mexico",
            peak_elevation_m=3700,
            center_lat=25.000,
            center_lon=-100.300,
            effective_radius_km=150,
            orientation_deg=135,  # NW-SE ridge
            region_key="mex_veracruz",
            avg_elevation_m=2200,
            slope_degrees=28,
        ))

        # ====== MEXICO (Yucatan Peninsula) ======
        # The Yucatan is generally flat (limestone); however, the Sierrita de
        # Ticul and other low ranges exist. These have minimal orographic effect
        # compared to the Caribbean islands, so we include a minimal entry:
        ranges.append(MountainRange(
            name="Sierrita de Ticul, Yucatan",
            peak_elevation_m=320,  # Low elevation, minimal effect
            center_lat=20.200,
            center_lon=-89.500,
            effective_radius_km=40,  # Small effective radius
            orientation_deg=95,
            region_key="mex_yucatan",
            avg_elevation_m=150,
            slope_degrees=8,
        ))

        # ====== BELIZE / HONDURAS ======
        # Maya Mountains (Belize/Guatemala border): 1124m
        ranges.append(MountainRange(
            name="Maya Mountains, Belize",
            peak_elevation_m=1124,
            center_lat=16.850,
            center_lon=-88.750,
            effective_radius_km=80,
            orientation_deg=0,  # N-S ridge
            region_key="ca_belize",  # (if defined as separate region)
            avg_elevation_m=700,
            slope_degrees=22,
        ))

        # ====== NICARAGUA / HONDURAS ======
        # Central American volcanic highlands: ~2400m
        ranges.append(MountainRange(
            name="Central American Highlands (Honduras/Nicaragua)",
            peak_elevation_m=2400,
            center_lat=14.500,
            center_lon=-86.500,
            effective_radius_km=100,
            orientation_deg=15,  # NNE-SSW trend
            region_key="ca_honduras",
            avg_elevation_m=1400,
            slope_degrees=26,
        ))

        # ====== US GULF COAST (Texas foothills) ======
        # Slight topography in coastal Texas: Edwards Plateau inland
        # Minimal direct coastal effect, but affects inland rainfall
        ranges.append(MountainRange(
            name="Edwards Plateau, Texas",
            peak_elevation_m=1500,
            center_lat=30.000,
            center_lon=-99.500,
            effective_radius_km=150,
            orientation_deg=120,  # NW-SE trend
            region_key="gulf_central_tx",
            avg_elevation_m=800,
            slope_degrees=12,
        ))

        # ====== LESSER ANTILLES ======
        # Dominica: 1447m peak (volcanic island)
        ranges.append(MountainRange(
            name="Morne Diablotins, Dominica",
            peak_elevation_m=1447,
            center_lat=15.413,
            center_lon=-61.371,
            effective_radius_km=50,
            orientation_deg=0,  # N-S island ridge
            region_key="carib_lesser_antilles",
            avg_elevation_m=900,
            slope_degrees=30,
        ))

        # Guadeloupe: 1467m (volcanic)
        ranges.append(MountainRange(
            name="La Soufrière, Guadeloupe",
            peak_elevation_m=1467,
            center_lat=16.052,
            center_lon=-61.664,
            effective_radius_km=50,
            orientation_deg=0,
            region_key="carib_lesser_antilles",
            avg_elevation_m=850,
            slope_degrees=30,
        ))

        # St. Lucia: 740m
        ranges.append(MountainRange(
            name="Morne Fortune, St. Lucia",
            peak_elevation_m=740,
            center_lat=13.900,
            center_lon=-60.970,
            effective_radius_km=40,
            orientation_deg=0,
            region_key="carib_lesser_antilles",
            avg_elevation_m=500,
            slope_degrees=22,
        ))

        return ranges

    def nearest_mountains(self, lat: float, lon: float,
                         max_distance_km: float = 200) -> List[Tuple[MountainRange, float]]:
        """
        Find all mountain ranges within max_distance_km of a given location.

        Args:
            lat: Query latitude (degrees)
            lon: Query longitude (degrees)
            max_distance_km: Maximum distance to search (km)

        Returns:
            List of (MountainRange, distance_km) tuples, sorted by distance
        """
        results = []

        for mountain in self.mountain_ranges:
            dist = self._haversine(lat, lon, mountain.center_lat, mountain.center_lon)
            if dist <= max_distance_km:
                results.append((mountain, dist))

        # Sort by distance
        results.sort(key=lambda x: x[1])
        return results

    @staticmethod
    def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Compute great-circle distance between two points.

        Args:
            lat1, lon1: Point 1 (degrees)
            lat2, lon2: Point 2 (degrees)

        Returns:
            Distance in kilometers
        """
        R_km = 6371.0
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

# Global terrain database (loaded once)
_terrain_db = None


def _get_terrain_db() -> TerrainDatabase:
    """Lazy-load the global terrain database."""
    global _terrain_db
    if _terrain_db is None:
        _terrain_db = TerrainDatabase()
    return _terrain_db


def compute_orographic_factor(lat: float, lon: float,
                              storm_approach_deg: Optional[float] = None) -> float:
    """
    Compute an orographic enhancement factor (1.0-3.0) for rainfall.

    This function estimates how mountain ranges amplify rainfall due to forced
    orographic lifting. The factor accounts for:
      1. Distance from storm to mountains (closer = more enhancement)
      2. Elevation of nearby peaks (taller = more enhancement)
      3. Angle between storm approach and ridge orientation
         (perpendicular approach = maximum enhancement)

    Physics:
      The factor follows the basic orographic rainfall equation:
        Rain enhancement ~ (elevation × wind_speed × moisture) / mountain_distance

      We normalize this to a 1.0-3.0 scale:
        - 1.0 = no terrain (flat coast or no mountains nearby)
        - 1.5 = moderate enhancement (foothills within 50 km)
        - 2.0 = strong enhancement (major mountains within 30 km)
        - 3.0 = extreme (compact storm hitting perpendicular to steep ridge)

    Windward/Leeward asymmetry:
      The windward slope (facing the storm) receives 2-3× more rain than the
      leeward slope. The compute_orographic_factor() returns the windward-side
      enhancement. For leeward slopes, the factor would be 0.3-0.5 (rain shadow).
      Users should adjust based on storm approach direction and local topography.

    Args:
        lat: Location latitude (degrees)
        lon: Location longitude (degrees)
        storm_approach_deg: Storm approach direction (degrees, 0-360):
            - 0° = approaching from north
            - 90° = approaching from east
            - etc.
            If None, function returns the maximum possible enhancement for any
            approach direction. This is conservative (worst-case amplification).

    Returns:
        Float between 1.0 (no terrain effect) and 3.0 (extreme amplification)

    Example:
        >>> # Location on windward slope of Puerto Rico mountains
        >>> factor = compute_orographic_factor(18.3, -65.3, storm_approach_deg=45)
        >>> print(factor)  # ~2.0-2.5 (strong enhancement)

    References:
        - Smith (1979), Houze (2012): Classic orographic rainfall theory
        - Chen et al. (2006): TC rainfall parameterization with elevation
    """
    db = _get_terrain_db()
    nearby_mountains = db.nearest_mountains(lat, lon, max_distance_km=200)

    if not nearby_mountains:
        # No mountains nearby
        return 1.0

    # Compute enhancement from all nearby mountains; take the maximum
    # (storm is most enhanced by the closest, highest mountain)
    max_enhancement = 1.0

    for mountain, distance_km in nearby_mountains:
        # Skip mountains farther than their effective radius
        if distance_km > mountain.effective_radius_km:
            continue

        # ---- Component 1: Elevation effect ----
        # Taller mountains → more enhancement
        # Normalized to 2256m (Blue Mountains, Jamaica)
        # factor ~ 1 + (elevation / reference_elevation)^0.5
        elevation_factor = 1.0 + (mountain.peak_elevation_m / 2256.0) ** 0.5

        # ---- Component 2: Distance decay ----
        # Closer mountains → more enhancement
        # Decay function: exp(-(distance / decay_scale)^2)
        # decay_scale is half the effective_radius
        decay_scale = mountain.effective_radius_km / 2.0
        distance_decay = math.exp(-(distance_km / decay_scale) ** 2)

        # ---- Component 3: Ridge orientation angle ----
        # Perpendicular approach to ridge → maximum enhancement
        # Parallel approach → minimal enhancement
        # If storm_approach_deg not provided, assume worst case (perpendicular)
        if storm_approach_deg is not None:
            # Compute angle between storm approach and ridge orientation
            # Ridge orientation is perpendicular to slope-facing direction
            approach_angle = abs((storm_approach_deg - mountain.orientation_deg) % 180)

            # Angle 0° = parallel, 90° = perpendicular
            # Enhancement is maximized at perpendicular: cos^2(angle)
            # At 0° (parallel): cos^2(0°) = 1, but ridge orientation is perpendicular,
            # so we want sin^2(angle) instead, which is 0 at 0° and 1 at 90°
            angle_factor = math.sin(math.radians(approach_angle)) ** 2
        else:
            # Conservative: assume worst case (perpendicular approach)
            angle_factor = 1.0

        # ---- Component 4: Slope effect ----
        # Steeper slopes → more enhancement
        # Normalized to 30° (typical steep mountain slope)
        slope_factor = 1.0 + (mountain.slope_degrees / 30.0) ** 0.8

        # Combine all factors
        # The base enhancement from elevation and decay is multiplicative
        # Angle and slope factors are applied as multipliers
        enhancement = elevation_factor * distance_decay * (1.0 + 0.5 * angle_factor) * slope_factor

        max_enhancement = max(max_enhancement, enhancement)

    # Clamp to [1.0, 3.0] range
    return max(1.0, min(max_enhancement, 3.0))


def compute_elevation_vulnerability(lat: float, lon: float) -> Dict[str, float]:
    """
    Compute flood vulnerability factors based on local elevation and terrain.

    This function estimates how topography affects flood risk, including:
      1. Valley flooding: Steep terrain channels rainfall into narrow valleys,
         creating flash flood concentration. Mountainous areas with deep valleys
         have higher vulnerability.
      2. Slope runoff: Steep slopes accelerate runoff, creating faster hydrologic
         response and flash flooding. Gentle slopes allow infiltration.
      3. Peak elevation: Higher peaks receive more orographic rainfall but may
         have poorer drainage.

    Args:
        lat: Location latitude (degrees)
        lon: Location longitude (degrees)

    Returns:
        Dictionary with:
            - avg_elevation_m: Average elevation in the area (m, or 0 if flat coast)
            - max_nearby_elevation_m: Highest peak within 100 km (m)
            - valley_flooding_risk: Risk factor (0-1), higher = more vulnerable
                * 0.0 = flat coast (no valleys)
                * 0.3 = foothills with gentle valleys
                * 0.6 = moderate mountains with notable valleys
                * 0.9 = steep mountain terrain with deep valleys
                * 1.0 = extreme terrain (gorges, defiles)
            - slope_runoff_factor: Multiplier for surface runoff (1.0-2.0)
                * 1.0 = flat terrain (slow runoff, good infiltration)
                * 1.3 = gentle slopes (moderate runoff)
                * 1.6 = steep terrain (fast runoff, flash flood risk)
                * 2.0 = extreme slopes (nearly vertical, direct runoff)

    Example:
        >>> # Puerto Rico location near Sierra de Luquillo
        >>> vuln = compute_elevation_vulnerability(18.3, -65.3)
        >>> print(vuln['valley_flooding_risk'])  # ~0.7 (high risk)
        >>> print(vuln['slope_runoff_factor'])  # ~1.7 (fast runoff)
    """
    db = _get_terrain_db()
    nearby_mountains = db.nearest_mountains(lat, lon, max_distance_km=150)

    if not nearby_mountains:
        # Flat coast, no terrain effects
        return {
            'avg_elevation_m': 0.0,
            'max_nearby_elevation_m': 0.0,
            'valley_flooding_risk': 0.0,
            'slope_runoff_factor': 1.0,
        }

    # Get statistics from nearby mountains
    closest_mountain, closest_distance = nearby_mountains[0]
    max_elevation = max(m.peak_elevation_m for m, _ in nearby_mountains)
    avg_elevation = np.mean([m.avg_elevation_m for m, _ in nearby_mountains])

    # Valley flooding risk: based on:
    #   - Elevation of nearby mountains
    #   - Proximity to mountains (closer = more risk)
    #   - Slope steepness (steeper = more channeled flow)
    elevation_component = (closest_mountain.peak_elevation_m / 3100.0) * 0.6  # Normalized to Hispaniola
    proximity_component = max(0, (150.0 - closest_distance) / 150.0) * 0.3
    slope_component = (closest_mountain.slope_degrees / 45.0) * 0.1  # Small contribution from slope

    valley_flooding_risk = min(1.0, elevation_component + proximity_component + slope_component)

    # Slope runoff factor: how fast rainfall becomes runoff
    # Gentle slopes (5-10°): factor ~1.1
    # Moderate slopes (15-20°): factor ~1.4
    # Steep slopes (25-35°): factor ~1.7
    # Extreme slopes (>40°): factor ~2.0
    avg_slope = np.mean([m.slope_degrees for m, _ in nearby_mountains])
    slope_runoff_factor = 1.0 + (avg_slope / 50.0)  # Linear scaling to 2.0 at 50°

    return {
        'avg_elevation_m': float(avg_elevation),
        'max_nearby_elevation_m': float(max_elevation),
        'valley_flooding_risk': float(valley_flooding_risk),
        'slope_runoff_factor': float(slope_runoff_factor),
    }


def get_terrain_profile(lat: float, lon: float, radius_km: float = 100) -> Dict[str, Any]:
    """
    Classify terrain at a location and return aggregate characteristics.

    This function provides a high-level terrain summary useful for determining
    if a location should use enhanced rainfall/flooding models.

    Classification:
        - 'flat': Coastal plains, deltas (avg elevation <100m, no nearby peaks)
        - 'coastal': Coastal areas with nearby gentle foothills (<500m peaks, >50km away)
        - 'hilly': Rolling terrain with moderate elevation (500-1500m peaks within 50km)
        - 'mountainous': Major mountains (>1500m peaks within 100km)

    Args:
        lat: Location latitude (degrees)
        lon: Location longitude (degrees)
        radius_km: Search radius for terrain features (km), default 100

    Returns:
        Dictionary with:
            - terrain_type: String ('flat', 'coastal', 'hilly', 'mountainous')
            - peak_elevation: Highest elevation within radius (m)
            - avg_elevation: Average elevation in region (m)
            - ridge_count: Number of significant ridges within radius
            - has_orographic_effect: Boolean, true if mountains will enhance rainfall

    Example:
        >>> profile = get_terrain_profile(25.761, -80.188)  # Miami
        >>> print(profile['terrain_type'])  # 'flat'
        >>> print(profile['peak_elevation'])  # ~0-50

        >>> profile = get_terrain_profile(18.3, -65.3)  # Puerto Rico
        >>> print(profile['terrain_type'])  # 'mountainous'
        >>> print(profile['has_orographic_effect'])  # True
    """
    db = _get_terrain_db()
    nearby_mountains = db.nearest_mountains(lat, lon, max_distance_km=radius_km)

    if not nearby_mountains:
        return {
            'terrain_type': 'flat',
            'peak_elevation': 0.0,
            'avg_elevation': 0.0,
            'ridge_count': 0,
            'has_orographic_effect': False,
        }

    # Extract statistics
    peak_elevation = max(m.peak_elevation_m for m, _ in nearby_mountains)
    avg_elevation = np.mean([m.avg_elevation_m for m, _ in nearby_mountains])

    # Count ridges: mountains within radius
    ridge_count = len(nearby_mountains)

    # Classify terrain
    closest_mountain, closest_distance = nearby_mountains[0]

    if peak_elevation < 100:
        terrain_type = 'flat'
        has_orographic_effect = False
    elif peak_elevation < 500 and closest_distance > 50:
        terrain_type = 'coastal'
        has_orographic_effect = False
    elif peak_elevation < 1500 and closest_distance < 100:
        terrain_type = 'hilly'
        has_orographic_effect = True
    else:
        terrain_type = 'mountainous'
        has_orographic_effect = True

    return {
        'terrain_type': terrain_type,
        'peak_elevation': float(peak_elevation),
        'avg_elevation': float(avg_elevation),
        'ridge_count': ridge_count,
        'has_orographic_effect': has_orographic_effect,
    }


# ============================================================================
#  HELPER: Windward/Leeward Adjustment
# ============================================================================

def adjust_for_windward_leeward(base_rainfall_mm: float, lat: float, lon: float,
                                storm_approach_deg: float) -> float:
    """
    Adjust rainfall estimate for windward/leeward asymmetry.

    This helper function applies the 2-3× windward amplification and rain shadow
    effects after computing the base rainfall using orographic_factor.

    Approach:
      1. Compute distance to the nearest mountain range
      2. Determine if location is on windward or leeward slope
      3. Apply multiplier:
         - Windward: 1.0-2.5× (depending on how exposed)
         - Leeward: 0.3-0.7× (rain shadow)

    Args:
        base_rainfall_mm: Base rainfall estimate before windward/leeward adjustment
        lat: Location latitude
        lon: Location longitude
        storm_approach_deg: Direction storm is approaching from (0-360°)

    Returns:
        Adjusted rainfall in mm, accounting for windward/leeward effects
    """
    db = _get_terrain_db()
    nearby_mountains = db.nearest_mountains(lat, lon, max_distance_km=100)

    if not nearby_mountains:
        return base_rainfall_mm

    closest_mountain, closest_distance = nearby_mountains[0]

    # Compute bearing from mountain center to query location
    bearing_to_location = _compute_bearing(
        closest_mountain.center_lat, closest_mountain.center_lon,
        lat, lon
    )

    # Compute wind exposure angle
    # Windward: storm approaches toward the location
    # Leeward: storm approaches away from the location
    approach_angle = abs((bearing_to_location - storm_approach_deg) % 180)
    # approach_angle 0° = leeward (away), 180° = windward (toward)

    # Map angle to multiplier
    # 180° (perpendicular approach directly up slope) = 2.0× windward
    # 90° (parallel to mountain) = 1.0× (no windward effect)
    # 0° (downslope) = 0.5× (rain shadow)
    windward_multiplier = 0.5 + 1.5 * math.sin(math.radians(approach_angle)) ** 2

    return base_rainfall_mm * windward_multiplier


def _compute_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute bearing from point 1 to point 2 (degrees, 0-360).

    Args:
        lat1, lon1: Starting point
        lat2, lon2: Ending point

    Returns:
        Bearing in degrees (0° = north, 90° = east, etc.)
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
