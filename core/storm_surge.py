"""
Formula 2: Rainfall Impact & Storm Surge Nullification Model.

This module computes the compound flooding/surge threat from a hurricane,
accounting for regional factors that either amplify or nullify storm surge.

The core insight: identical storms produce vastly different surge outcomes
depending on where they make landfall. A shallow continental shelf amplifies
surge (e.g., Gulf Coast), while a steep dropoff limits it (e.g., Pacific islands).
Similarly, rainfall accumulation depends on forward speed, moisture availability,
and orographic effects.

The formula integrates:
  1. Pressure-driven surge estimate (inverse barometer effect)
  2. Wind-driven surge component (Ekman transport)
  3. Forward speed modulation (slow storms = more rain, less surge; fast = more surge, less rain)
  4. Continental shelf amplification factor (regional)
  5. Rainfall accumulation estimate (TCR - Tropical Cyclone Rainfall)
  6. Compound flooding interaction (surge + rain > sum of parts)

Reference:
  - Irish et al. (2008): "The effect of hurricane forward speed on storm surge"
  - Weisberg & Zheng (2006): "Hurricane storm surge in Tampa Bay"
  - Lonfat et al. (2004): "Tropical cyclone rainfall climatology"
  - Emanuel (2005): "Increasing destructiveness of tropical cyclones"
"""

import math
import numpy as np
from typing import Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Lazy imports for new modules — these are imported at function call time
# to avoid circular imports and allow the module to load without them.
_land_proximity_module = None
_terrain_module = None


def _get_land_proximity():
    """Lazy-load the land proximity module."""
    global _land_proximity_module
    if _land_proximity_module is None:
        try:
            from core import land_proximity as lp
            _land_proximity_module = lp
        except ImportError:
            logger.warning("land_proximity module not available — using legacy region detection")
    return _land_proximity_module


def _get_terrain():
    """Lazy-load the terrain module."""
    global _terrain_module
    if _terrain_module is None:
        try:
            from core import terrain as tr
            _terrain_module = tr
        except ImportError:
            logger.warning("terrain module not available — using profile-based rain enhancement")
    return _terrain_module


# ============================================================================
#  REGIONAL COASTAL PROFILES
# ============================================================================

@dataclass
class CoastalProfile:
    """
    Characterizes a coastal region's vulnerability to storm surge and rainfall.

    Attributes:
        name: Human-readable region name
        shelf_width_km: Continental shelf width (km) - wider = more surge amplification
        avg_slope: Average nearshore slope (dimensionless, rise/run)
        surge_amplification: Multiplier for surge height (1.0 = neutral)
        rain_enhancement: Orographic/moisture enhancement factor for rainfall
        tidal_range_m: Mean tidal range (meters) - affects compound flooding
        wetland_buffer: Fraction of surge absorbed by coastal wetlands (0-1)
        bay_funneling: Amplification from bay/estuary funneling (1.0 = none)
        coastal_defense: Fraction of surge mitigated by engineered defenses and
            barrier islands (0-1). Captures seawalls, barrier island chains, and
            engineered coastal protection that reduce effective surge height.
            Distinct from wetland_buffer (natural attenuation) — this is human-built
            or geomorphological barriers. Default 0.0 (no defense).
            Reference: USACE Coastal Engineering Manual; Wamsley et al. (2010)
        river_basin_factor: Amplification of inland flooding from river discharge
            when storms stall over major river basins (1.0 = neutral, >1 = amplified).
            Captures how upstream rainfall drains into coastal/lowland areas days
            after landfall, creating compound flooding beyond what the local rainfall
            model predicts. Harvey (2017) over Houston's Buffalo Bayou/San Jacinto
            watershed and Florence (2018) over NC's Cape Fear River basin are
            canonical examples.
            Reference: Wing et al. (2019); Villarini & Goska (2020)
        antecedent_moisture: Seasonal soil moisture proxy (0-1 scale, 0 = dry, 1 = saturated).
            Represents the typical soil moisture state during peak hurricane season
            (Aug-Oct) for a region. Saturated soils dramatically reduce infiltration
            capacity, causing more surface runoff and faster river response to rainfall.
            NC's coastal plain (Florence Sept 2018) was already saturated from a wet
            summer; TX Gulf coast is typically drier. Caribbean highlands have high
            moisture from year-round rainfall. This modifies the effective rainfall
            damage by reducing drainage capacity in already-wet regions.
            Reference: Villarini et al. (2014) "Rainfall and flooding associated
            with TCs"; Brauer et al. (2020) "Antecedent soil moisture conditions"
        bathymetric_concavity: Coastal embayment concavity factor (0-1 scale, 0 = straight
            coast, 1 = extreme concavity). Concave bathymetry focuses and amplifies
            storm surge through constructive interference of shelf waves converging
            toward the embayment apex. The Mississippi Bight (MS/AL coast) is the
            canonical example: the concave coastline from Mobile Bay to the MS barrier
            islands funnels surge energy toward the central coast, amplifying Katrina's
            (2005) peak surge 15-25% beyond what a straight-coast model predicts.
            Tampa Bay's funnel geometry also creates focused surge amplification.
            This is geometrically distinct from bay_funneling (which captures local
            harbor/estuary effects) — bathymetric concavity operates at the 100-200km
            scale of the continental shelf geometry itself.
            Reference: Weisberg & Zheng (2006); Irish et al. (2008) "The effect of
            hurricane forward speed on storm surge"; Dietrich et al. (2011)
            "Modeling hurricane waves and storm surge using integrally-coupled,
            scalable computations"
    """
    name: str
    shelf_width_km: float
    avg_slope: float
    surge_amplification: float
    rain_enhancement: float
    tidal_range_m: float
    wetland_buffer: float
    bay_funneling: float
    coastal_defense: float = 0.0
    river_basin_factor: float = 1.0
    antecedent_moisture: float = 0.5  # Default moderate moisture
    bathymetric_concavity: float = 0.0  # Default straight coast


# Calibrated from NOAA tide gauge data, SLOSH model outputs, and historical records
COASTAL_PROFILES = {
    # === US Gulf Coast (highest surge vulnerability) ===
    # === US Gulf Coast (highest surge vulnerability) ===
    "gulf_west_tx": CoastalProfile(
        name="Western Texas Coast (Corpus Christi to Brownsville)",
        shelf_width_km=120, avg_slope=0.0005, surge_amplification=1.35,
        rain_enhancement=0.85, tidal_range_m=0.5, wetland_buffer=0.10,
        bay_funneling=1.15,
        coastal_defense=0.03,   # Minimal: some seawalls at Corpus Christi
        river_basin_factor=1.05,  # Nueces River basin — small
        antecedent_moisture=0.35,  # Semi-arid south TX, low soil moisture
        bathymetric_concavity=0.10,  # Mostly straight coast, slight Corpus Christi Bay curve
    ),
    "gulf_central_tx": CoastalProfile(
        name="Central Texas Coast (Galveston to Corpus Christi)",
        shelf_width_km=130, avg_slope=0.0004, surge_amplification=1.40,
        rain_enhancement=0.90, tidal_range_m=0.5, wetland_buffer=0.08,
        bay_funneling=1.15,
        coastal_defense=0.05,   # Galveston Seawall provides some protection
        river_basin_factor=1.20,  # Buffalo Bayou/San Jacinto/Brazos River basins
        # Harvey (2017) demonstrated severe river discharge amplification:
        # 5 days of stalling dumped upstream rain that funneled into Houston's
        # bayou system, creating catastrophic compound flooding well inland.
        # Reference: Sebastian et al. (2017), Juan et al. (2020)
        antecedent_moisture=0.45,  # Houston area moderately humid but clay soils
        # drain poorly. Houston's black gumbo clay becomes impervious when wet.
        bathymetric_concavity=0.10,  # Galveston Bay entrance has some concavity
        # but bay_funneling=1.15 already captures the local harbor effect
    ),
    "gulf_la": CoastalProfile(
        name="Louisiana Coast",
        shelf_width_km=180, avg_slope=0.0002, surge_amplification=1.70,
        rain_enhancement=1.10, tidal_range_m=0.4, wetland_buffer=0.12,
        bay_funneling=1.25,
        coastal_defense=0.04,   # Post-Katrina HSDRRS levee system helps New Orleans
        # but doesn't protect the broader coast; 0.04 reflects limited coverage
        river_basin_factor=1.15,  # Mississippi River basin — massive but well-channeled
        antecedent_moisture=0.70,  # LA's coastal marshes are perpetually saturated
        bathymetric_concavity=0.30,  # LA/MS coast has moderate concavity from
        # the Mississippi Delta's protruding shelf creating a broad embayment to the east.
        # Katrina's surge was amplified by the shelf geometry funneling toward the MS coast.
    ),
    "gulf_ms_al": CoastalProfile(
        name="Mississippi/Alabama Coast",
        shelf_width_km=100, avg_slope=0.0006, surge_amplification=1.40,
        rain_enhancement=1.00, tidal_range_m=0.5, wetland_buffer=0.12,
        bay_funneling=1.25,
        coastal_defense=0.02,
        river_basin_factor=1.10,  # Mobile River/Tombigbee basin
        antecedent_moisture=0.60,  # Humid subtropical, moderate saturation
        bathymetric_concavity=0.55,  # Mississippi Bight — the most concave
        # bathymetry on the US Gulf Coast. The curved coastline from Mobile Bay to
        # the MS barrier islands creates a bowl-shaped shelf that focuses surge
        # energy toward the central MS coast. Katrina (2005) produced 8.5m surge
        # partly because of this focusing effect. Dietrich et al. (2011) showed
        # 15-25% surge amplification from the Bight's geometry.
        # Reference: Dietrich et al. (2011); Westerink et al. (2008)
    ),
    "gulf_fl_panhandle": CoastalProfile(
        name="Florida Panhandle",
        shelf_width_km=80, avg_slope=0.001, surge_amplification=1.25,
        rain_enhancement=1.05, tidal_range_m=0.5, wetland_buffer=0.05,
        bay_funneling=1.10,
        coastal_defense=0.02,
        river_basin_factor=1.05,  # Apalachicola River — moderate
        antecedent_moisture=0.55,  # Sandy soils drain well despite humid climate
        bathymetric_concavity=0.15,  # Mexico Beach/Panama City area has some
        # concavity from the Apalachicola Bight but less pronounced than MS Bight
    ),
    "gulf_fl_west": CoastalProfile(
        name="Florida West Coast (Tampa to Naples)",
        shelf_width_km=160, avg_slope=0.0003, surge_amplification=1.35,
        rain_enhancement=1.15, tidal_range_m=0.6, wetland_buffer=0.10,
        bay_funneling=1.20,
        coastal_defense=0.04,   # Tampa Bay seawall segments, barrier islands (Pinellas)
        river_basin_factor=1.08,  # Peace/Myakka/Hillsborough rivers — moderate
        antecedent_moisture=0.55,  # FL wet season coincides with hurricane season
        bathymetric_concavity=0.20,  # Charlotte Harbor to Tampa Bay has moderate
        # concave geometry, but bay_funneling=1.20 already captures local effects.
        # The broader shelf concavity is real but less pronounced than MS Bight.
    ),

    # === US Atlantic Coast ===
    "atl_fl_east": CoastalProfile(
        name="Florida East Coast (Miami to Jacksonville)",
        shelf_width_km=15, avg_slope=0.01, surge_amplification=1.05,
        rain_enhancement=1.10, tidal_range_m=0.8, wetland_buffer=0.05,
        bay_funneling=1.20,
        coastal_defense=0.08,   # Barrier islands (Miami Beach, Palm Beach, Amelia
        # Island) plus extensive seawall/revetment infrastructure along the
        # Intracoastal Waterway. The barrier island chain absorbs wave energy
        # and reduces effective surge penetration into the mainland lagoon system.
        # Reference: USACE Jacksonville District coastal protection assessment
        river_basin_factor=1.03,  # FL east rivers are short — minimal basin effect
        antecedent_moisture=0.50,  # Limestone bedrock drains well despite wet season
        bathymetric_concavity=0.05,  # FL east coast is essentially straight
        # with narrow shelf. Minimal concavity effects. Biscayne Bay provides
        # some local focusing but at harbor scale (captured by bay_funneling).
    ),
    "atl_ga_sc": CoastalProfile(
        name="Georgia/South Carolina Coast",
        shelf_width_km=90, avg_slope=0.001, surge_amplification=1.20,
        rain_enhancement=1.05, tidal_range_m=2.0, wetland_buffer=0.20,
        bay_funneling=1.15,
        coastal_defense=0.03,   # Barrier islands (Sea Island, Hilton Head) — natural
        river_basin_factor=1.10,  # Savannah/Cooper/Santee rivers
        antecedent_moisture=0.60,  # Humid, clay soils retain water
        bathymetric_concavity=0.15,  # GA/SC has moderate embayment from
        # the South Atlantic Bight curvature between Cape Fear and FL
    ),
    "atl_nc": CoastalProfile(
        name="North Carolina Coast",
        shelf_width_km=70, avg_slope=0.002, surge_amplification=1.15,
        rain_enhancement=1.10, tidal_range_m=1.0, wetland_buffer=0.15,
        bay_funneling=1.20,
        coastal_defense=0.06,   # Outer Banks barrier islands provide significant
        # surge attenuation for the mainland. Pamlico/Albemarle Sounds act as
        # buffer zones. However, overwash and breaching during major storms
        # limits effectiveness. Reference: Sallenger et al. (2006)
        river_basin_factor=1.18,  # Cape Fear, Neuse, Tar-Pamlico rivers
        # Florence (2018) demonstrated severe river compound flooding:
        # upstream rainfall drained into NC's coastal plain over 3-5 days,
        # producing record river crests well after the storm passed.
        # Reference: Stewart & Berg (2019) "NHC TC Report: Florence"
        antecedent_moisture=0.65,  # NC coastal plain has high water table and
        # poor drainage. Florence (Sept 2018) arrived after a wet summer —
        # soil was already near saturation, amplifying surface runoff.
        # Reference: Brauer et al. (2020)
        bathymetric_concavity=0.20,  # Pamlico Sound embayment creates moderate
        # concavity. The Outer Banks act as both barrier and focusing geometry.
    ),
    "atl_mid": CoastalProfile(
        name="Mid-Atlantic (VA to NJ)",
        shelf_width_km=100, avg_slope=0.001, surge_amplification=1.30,
        rain_enhancement=1.00, tidal_range_m=1.2, wetland_buffer=0.08,
        bay_funneling=1.25,
        coastal_defense=0.05,   # NJ barrier islands, Chesapeake Bay mouth protection
        river_basin_factor=1.12,  # Potomac, James, Delaware rivers
        antecedent_moisture=0.55,  # Moderate — mixed soils, seasonal variation
        bathymetric_concavity=0.25,  # Chesapeake Bay entrance and Delaware Bay
        # create significant embayment effects along the Mid-Atlantic
    ),
    "atl_ne": CoastalProfile(
        name="Northeast (NY to New England)",
        shelf_width_km=120, avg_slope=0.001, surge_amplification=1.35,
        rain_enhancement=1.05, tidal_range_m=1.5, wetland_buffer=0.05,
        bay_funneling=1.35,
        coastal_defense=0.06,   # NY/NJ Harbor barriers, Long Island barrier beaches
        # Post-Sandy investment in flood barriers and seagates.
        # Reference: USACE North Atlantic Coast Comprehensive Study (2015)
        river_basin_factor=1.05,  # Hudson, Connecticut rivers — moderate
        antecedent_moisture=0.50,  # Variable — depends on prior nor'easters
        bathymetric_concavity=0.15,  # NY Bight has some shelf concavity
        # but bay_funneling=1.35 already captures the NY Harbor focusing effect.
        # The broader scale concavity adds modest additional amplification.
    ),

    # === Caribbean ===
    "carib_pr": CoastalProfile(
        name="Puerto Rico",
        shelf_width_km=8, avg_slope=0.05, surge_amplification=0.70,
        rain_enhancement=1.40, tidal_range_m=0.3, wetland_buffer=0.02,
        bay_funneling=1.05,
        coastal_defense=0.02,   # Minimal engineered coastal defense
        river_basin_factor=1.15,  # Short but steep river basins amplify flash
        # flooding. PR's Cordillera Central creates rapid runoff into coastal
        # communities. Maria (2017) caused catastrophic river flooding in
        # Toa Baja and Guaynabo from overtopped rivers.
        # Reference: Santos-Hernandez & Morrow (2013)
        antecedent_moisture=0.75,  # Tropical highland soils, year-round rain.
        # PR's Cordillera Central has perpetually moist soils with thin laterite
        # layers that saturate rapidly, producing flash floods even from moderate rain.
        bathymetric_concavity=0.05,  # PR has minimal shelf concavity — steep dropoff
    ),
    "carib_usvi": CoastalProfile(
        name="US Virgin Islands",
        shelf_width_km=5, avg_slope=0.08, surge_amplification=0.60,
        rain_enhancement=1.20, tidal_range_m=0.3, wetland_buffer=0.01,
        bay_funneling=1.00,
        coastal_defense=0.01,
        river_basin_factor=1.0,  # No significant river basins
        antecedent_moisture=0.55,  # Tropical but small island drains quickly
        bathymetric_concavity=0.0,  # Small islands, no concave shelf geometry
    ),
    "carib_bahamas": CoastalProfile(
        name="Bahamas",
        shelf_width_km=200, avg_slope=0.0001, surge_amplification=1.30,
        rain_enhancement=0.80, tidal_range_m=0.8, wetland_buffer=0.02,
        bay_funneling=1.05,
        coastal_defense=0.03,   # Inter-island dissipation provides some natural defense
        river_basin_factor=1.0,  # No river basins on low-lying islands
        antecedent_moisture=0.40,  # Porous limestone, rain drains into aquifer
        bathymetric_concavity=0.10,  # Bahamas bank is flat, some inter-island
        # geometry creates mild focusing between Grand Bahama and Abaco
    ),
    "carib_jamaica": CoastalProfile(
        name="Jamaica",
        shelf_width_km=12, avg_slope=0.03, surge_amplification=0.75,
        rain_enhancement=1.50, tidal_range_m=0.3, wetland_buffer=0.03,
        bay_funneling=1.05,
        coastal_defense=0.01,
        river_basin_factor=1.20,  # Blue Mountain rivers produce flash floods
        antecedent_moisture=0.70,  # Tropical highlands, high year-round rainfall
        bathymetric_concavity=0.05,  # Mountainous island, steep shelf
    ),
    "carib_cuba_n": CoastalProfile(
        name="Northern Cuba",
        shelf_width_km=30, avg_slope=0.01, surge_amplification=1.00,
        rain_enhancement=1.20, tidal_range_m=0.5, wetland_buffer=0.10,
        bay_funneling=1.10,
        coastal_defense=0.03,
        river_basin_factor=1.08,
        antecedent_moisture=0.60,  # Tropical, moderate soil moisture
        bathymetric_concavity=0.10,  # Northern Cuba has some coastal embayments
    ),
    "carib_hispaniola": CoastalProfile(
        name="Hispaniola (DR / Haiti)",
        shelf_width_km=10, avg_slope=0.04, surge_amplification=0.72,
        rain_enhancement=1.60, tidal_range_m=0.3, wetland_buffer=0.02,
        bay_funneling=1.05,
        coastal_defense=0.01,
        river_basin_factor=1.25,  # Deforested watersheds amplify flooding
        antecedent_moisture=0.80,  # Deforested slopes, compacted soils, high runoff
        bathymetric_concavity=0.05,  # Steep mountainous island, minimal shelf
    ),
    "carib_lesser_antilles": CoastalProfile(
        name="Lesser Antilles",
        shelf_width_km=4, avg_slope=0.10, surge_amplification=0.55,
        rain_enhancement=1.30, tidal_range_m=0.3, wetland_buffer=0.01,
        bay_funneling=1.00,
        coastal_defense=0.01,
        river_basin_factor=1.05,
        antecedent_moisture=0.65,  # Volcanic soils, tropical moisture
        bathymetric_concavity=0.0,  # Small islands, no concave shelf
    ),
    "carib_cayman": CoastalProfile(
        name="Cayman Islands",
        shelf_width_km=3, avg_slope=0.15, surge_amplification=0.50,
        rain_enhancement=0.90, tidal_range_m=0.3, wetland_buffer=0.01,
        bay_funneling=1.00,
        coastal_defense=0.02,
        river_basin_factor=1.0,
        antecedent_moisture=0.35,  # Limestone karst, excellent drainage
        bathymetric_concavity=0.0,  # Small, low-lying island
    ),

    # === Central America / Mexico ===
    "mex_yucatan": CoastalProfile(
        name="Yucatan Peninsula",
        shelf_width_km=180, avg_slope=0.0002, surge_amplification=1.55,
        rain_enhancement=1.30, tidal_range_m=0.3, wetland_buffer=0.15,
        bay_funneling=1.05,
        coastal_defense=0.02,
        river_basin_factor=1.10,  # Cenote/karst system affects drainage patterns
        antecedent_moisture=0.35,  # Karst drainage, low soil moisture
        bathymetric_concavity=0.15,  # Yucatan has moderate shelf curvature
    ),
    "mex_gulf": CoastalProfile(
        name="Mexico Gulf Coast (Tamaulipas to Tabasco)",
        shelf_width_km=60, avg_slope=0.002, surge_amplification=1.10,
        rain_enhancement=1.40, tidal_range_m=0.4, wetland_buffer=0.10,
        bay_funneling=1.05,
        coastal_defense=0.02,
        river_basin_factor=1.15,  # Grijalva/Usumacinta rivers
        antecedent_moisture=0.65,  # Tabasco lowlands are swampy/saturated
        bathymetric_concavity=0.20,  # Bay of Campeche creates concavity
    ),
    "central_am": CoastalProfile(
        name="Central America (Belize to Panama)",
        shelf_width_km=20, avg_slope=0.02, surge_amplification=0.80,
        rain_enhancement=1.70, tidal_range_m=0.3, wetland_buffer=0.08,
        bay_funneling=1.05,
        coastal_defense=0.01,
        river_basin_factor=1.20,  # Steep tropical rivers amplify flash flooding
        antecedent_moisture=0.75,  # Tropical forests, high year-round moisture
        bathymetric_concavity=0.10,  # Varied coastline, some bays (Honduras)
    ),
    "open_ocean": CoastalProfile(
        name="Open Ocean (No Coastal Effects)",
        shelf_width_km=0, avg_slope=0, surge_amplification=0.0,
        rain_enhancement=0.0, tidal_range_m=0, wetland_buffer=0.0,
        bay_funneling=1.0,
        coastal_defense=0.0,
        river_basin_factor=1.0,
        antecedent_moisture=0.0,
        bathymetric_concavity=0.0,
    ),
}


def get_coastal_profile(region_key: str) -> Optional[CoastalProfile]:
    """Look up a coastal profile by region key."""
    return COASTAL_PROFILES.get(region_key)


def estimate_region_from_coordinates(lat: float, lon: float) -> str:
    """
    Estimate the most appropriate coastal region from lat/lon coordinates.

    Uses the land_proximity module's Haversine-based nearest-coast
    lookup when available, falling back to the legacy bounding-box approach.
    """
    # Try the new land_proximity module first (distance-based, no gaps)
    lp = _get_land_proximity()
    if lp is not None:
        try:
            result = lp.get_nearest_region(lat, lon)
            if result and result != "open_ocean":
                return result
            # If open_ocean, check if we're genuinely far from land
            dist_info = lp.compute_distance_to_coast(lat, lon)
            if dist_info and dist_info.get("distance_km", 999) > 500:
                return "open_ocean"
            # Near-ish to land but no matching region — fall through to legacy
            if dist_info and dist_info.get("nearest_region_key"):
                return dist_info["nearest_region_key"]
        except Exception as e:
            logger.debug(f"land_proximity lookup failed, using legacy: {e}")

    # Legacy bounding-box approach (kept as fallback)
    # Caribbean
    if lat < 20 and lon > -88 and lon < -59:
        if lon > -68 and lat > 17.5 and lat < 18.6:
            return "carib_pr"
        if lon > -65.5 and lon < -64 and lat > 17 and lat < 19:
            return "carib_usvi"
        if lon > -80 and lon < -73 and lat > 21 and lat < 27:
            return "carib_bahamas"
        if lon > -79 and lon < -76 and lat > 17.5 and lat < 18.6:
            return "carib_jamaica"
        if lon > -85 and lon < -74 and lat > 19 and lat < 24:
            return "carib_cuba_n"
        if lon > -75 and lon < -68 and lat > 17.5 and lat < 20.5:
            return "carib_hispaniola"
        if lon > -63 and lon < -59 and lat > 12 and lat < 19:
            return "carib_lesser_antilles"
        return "carib_lesser_antilles"

    # Yucatan
    if lat > 18 and lat < 22 and lon > -92 and lon < -86:
        return "mex_yucatan"

    # Mexico Gulf
    if lat > 18 and lat < 25 and lon > -98 and lon < -92:
        return "mex_gulf"

    # Central America
    if lat > 8 and lat < 18 and lon > -92 and lon < -77:
        return "central_am"

    # US Gulf Coast
    if lat > 25 and lat < 31 and lon > -98 and lon < -94:
        return "gulf_central_tx"
    if lat > 25 and lat < 28 and lon > -98 and lon < -96.5:
        return "gulf_west_tx"
    if lat > 28 and lat < 31 and lon > -94 and lon < -88.5:
        return "gulf_la"
    if lat > 29 and lat < 31 and lon > -88.5 and lon < -87:
        return "gulf_ms_al"
    if lat > 29 and lat < 31 and lon > -87 and lon < -85:
        return "gulf_fl_panhandle"
    if lat > 25 and lat < 29 and lon > -85 and lon < -81:
        return "gulf_fl_west"

    # US Atlantic Coast
    if lat > 24 and lat < 31 and lon > -82 and lon < -79.5:
        return "atl_fl_east"
    if lat > 31 and lat < 34 and lon > -82 and lon < -78:
        return "atl_ga_sc"
    if lat > 33 and lat < 36.5 and lon > -79 and lon < -75:
        return "atl_nc"
    if lat > 36 and lat < 40 and lon > -77 and lon < -73:
        return "atl_mid"
    if lat > 40 and lon > -74 and lon < -69:
        return "atl_ne"

    # Default: open ocean (minimal surge)
    return "open_ocean"


# ============================================================================
#  FORMULA 2: STORM SURGE & RAINFALL IMPACT
# ============================================================================

@dataclass
class SurgeRainfallResult:
    """
    Result of the storm surge and rainfall impact computation.

    Attributes:
        surge_height_m: Estimated peak storm surge height (meters above normal tide)
        surge_score: Normalized surge threat (0-100)
        rainfall_total_mm: Estimated total rainfall accumulation (mm)
        rainfall_score: Normalized rainfall threat (0-100)
        compound_flood_score: Combined surge + rain with interaction term (0-100)
        surge_nullification: How much the coast nullifies surge (0 = none, 1 = full)
        region_key: Which coastal profile was used
        components: Dict of intermediate values for debugging
    """
    surge_height_m: float
    surge_score: float
    rainfall_total_mm: float
    rainfall_score: float
    compound_flood_score: float
    surge_nullification: float
    region_key: str
    components: dict


def compute_surge_rainfall(
    vmax_ms: float,
    min_pressure_hpa: Optional[float],
    forward_speed_ms: Optional[float],
    r34_m: Optional[float],
    rmw_m: Optional[float],
    lat: float,
    lon: float,
    region_key: Optional[str] = None,
    ike_total_tj: Optional[float] = None,
    approach_angle_deg: Optional[float] = None,
    # Parameters from API integrations when available
    real_soil_moisture: Optional[float] = None,  # 0-1 from Open-Meteo API
    real_sst_c: Optional[float] = None,          # °C from Open-Meteo marine/Google Weather
    storm_approach_heading_deg: Optional[float] = None,  # For terrain windward/leeward
) -> SurgeRainfallResult:
    """
    Compute storm surge height and rainfall impact using regional coastal profiles.

    This is Formula 2 of the hurricane DPI system. It estimates:

    1. **Storm Surge Height** (meters):
       Surge = (IB_surge + Wind_surge) × shelf_amplification × bay_funneling × (1 - wetland_buffer)

       Where:
       - IB_surge: Inverse barometer effect (1 cm per hPa pressure deficit)
       - Wind_surge: Wind-driven Ekman transport, proportional to V² × shelf_width / (g × depth)
       - shelf_amplification: Regional continental shelf geometry factor
       - bay_funneling: Local amplification from coastal geometry
       - wetland_buffer: Natural attenuation from coastal wetlands

    2. **Rainfall Accumulation** (mm):
       TCR = R_base × speed_factor × moisture_factor × orographic_factor

       Where:
       - R_base: Empirical base rainfall rate from Lonfat et al. (2004)
       - speed_factor: Slower storms produce more rain (inversely proportional)
       - moisture_factor: SST and latitude-dependent moisture availability
       - orographic_factor: Regional terrain enhancement

    3. **Compound Flooding Score**:
       Combines surge and rainfall with a super-linear interaction term,
       because simultaneous surge + rain overwhelms drainage systems.

    Args:
        vmax_ms: Maximum sustained winds (m/s)
        min_pressure_hpa: Central pressure (hPa), or None
        forward_speed_ms: Forward translational speed (m/s), or None
        r34_m: Radius of 34-kt winds (meters), for rain extent
        rmw_m: Radius of maximum winds (meters)
        lat: Storm center latitude
        lon: Storm center longitude
        region_key: Override automatic region detection
        ike_total_tj: IKE from Formula 1, for cross-formula coupling
        approach_angle_deg: Storm approach angle relative to coastline normal
            in degrees. 0° = perpendicular landfall (standard), positive = storm
            approaching from the right (amplified right-front quadrant surge).
            None = use default perpendicular assumption.
            Range: -90 to 90. Positive values typically produce higher surge.

    Returns:
        SurgeRainfallResult with all components
    """
    # Determine coastal profile
    if region_key is None:
        region_key = estimate_region_from_coordinates(lat, lon)

    profile = COASTAL_PROFILES.get(region_key)
    if profile is None:
        # Fallback to a neutral profile
        profile = CoastalProfile(
            name="Default Open Coast", shelf_width_km=50, avg_slope=0.005,
            surge_amplification=1.0, rain_enhancement=1.0, tidal_range_m=0.8,
            wetland_buffer=0.05, bay_funneling=1.0,
            coastal_defense=0.0, river_basin_factor=1.0,
            antecedent_moisture=0.5, bathymetric_concavity=0.0,
        )

    # Default forward speed if missing (use climatological average ~5 m/s)
    fwd_speed = forward_speed_ms if forward_speed_ms and forward_speed_ms > 0 else 5.0

    # Default pressure if missing (estimate from wind-pressure relationship)
    if min_pressure_hpa is not None:
        pressure = min_pressure_hpa
    else:
        # Knaff & Zehr (2007) wind-pressure relationship
        vmax_kt = vmax_ms / 0.514444
        pressure = 1013.25 - (vmax_kt / 1.12) ** 1.5  # Approximate

    # Default r34 if missing
    if r34_m is None:
        r34_m = max(100_000, vmax_ms * 5000)  # Rough estimate

    # Default RMW if missing
    if rmw_m is None:
        vmax_kt = vmax_ms / 0.514444
        rmw_m = 46.4 * math.exp(-0.0155 * vmax_kt + 0.0169 * abs(lat)) * 1000

    components = {}

    # ===================================================================
    # PART 1: STORM SURGE ESTIMATION
    # ===================================================================

    # 1a. Inverse barometer effect
    # 1 hPa pressure deficit ≈ 1 cm water rise
    pressure_deficit = 1013.25 - pressure
    ib_surge_m = pressure_deficit * 0.01  # Convert cm to m
    components["ib_surge_m"] = ib_surge_m

    # 1b. Wind-driven surge (Ekman transport)
    # Uses empirical Jelesnianski (1972) / SLOSH-lite approach rather than
    # raw V²L/gd which overestimates for wide shelves.
    #
    # Empirical surge formula (fitted to SLOSH model outputs):
    #   Wind_surge ≈ C × (Vmax/V_ref)^2 × shelf_factor
    # where C ≈ 2.5m for reference conditions (V_ref = 50 m/s, moderate shelf)
    g = 9.81
    shelf_length_m = profile.shelf_width_km * 1000.0

    # Reference wind speed for scaling
    V_ref = 50.0  # m/s (strong Cat 2 / weak Cat 3)

    # Pressure-dependent coastal wind preservation:
    # Stronger storms (deeper pressure) maintain their wind structure better
    # as they approach the coast. Weaker storms experience more friction loss.
    # At extreme pressure deficits (>90 hPa), storms retain ~87% of wind;
    # at modest deficits (<40 hPa), only ~83%.
    coast_factor = 0.82 + 0.05 * min(1.0, pressure_deficit / 100.0)
    vmax_at_coast = vmax_ms * coast_factor

    # Base surge from empirical fit to SLOSH outputs
    # At V_ref on a moderate shelf, surge ≈ 2.0m (reduced from 2.5 to match
    # SLOSH model outputs more accurately — prior value overestimated by ~25%)
    base_wind_surge = 2.0 * (vmax_at_coast / V_ref) ** 2.0

    # Shelf width amplification (logarithmic, not linear)
    # Wider shelves amplify surge but with strongly diminishing returns.
    # Reference shelf: 100 km (moderate). Capped more aggressively to prevent
    # overestimation on very wide shelves (Gulf Coast).
    shelf_factor = 0.7 + 0.25 * math.log(max(5, profile.shelf_width_km) / 100.0 + 1.0)
    shelf_factor = max(0.5, min(1.5, shelf_factor))

    wind_surge_m = base_wind_surge * shelf_factor

    # Clamp wind surge to physically reasonable values
    wind_surge_m = min(wind_surge_m, 8.0)  # Wind surge alone rarely exceeds 8m
    components["wind_surge_m"] = wind_surge_m

    # 1c. Forward speed modulation of surge
    # Faster storms push more water (resonant coupling with shelf waves)
    # But very fast storms reduce residence time
    # Optimal surge speed is ~5-8 m/s (Irish et al., 2008)
    fwd_kt = fwd_speed / 0.514444
    if fwd_kt < 5:
        speed_surge_factor = 0.70 + 0.06 * fwd_kt  # Slow storms: less surge
    elif fwd_kt < 15:
        speed_surge_factor = 1.0  # Optimal range
    elif fwd_kt < 25:
        speed_surge_factor = 1.0 + 0.02 * (fwd_kt - 15)  # Fast storms: more surge
    else:
        speed_surge_factor = 1.20  # Very fast: plateau
    components["speed_surge_factor"] = speed_surge_factor

    # 1d. Size effect on surge
    # Larger storms (larger r34) push surge over wider area, amplifying it
    # Normalized to a "typical" r34 of ~200 km
    size_factor = min(1.5, max(0.7, (r34_m / 200_000) ** 0.3))
    components["size_factor"] = size_factor

    # 1d2. Compact storm surge focus factor
    # Compact Cat 4-5 storms (Andrew, Michael) concentrate extreme wind energy
    # into a very small area, generating localized surge peaks that exceed what
    # the standard parametric model predicts. The tight RMW creates an intense
    # wind-driven setup over a short coastal segment that isn't captured by the
    # broad-area size_factor. When RMW is small (<25km) and winds are extreme
    # (>60 m/s), the focused Ekman transport can amplify peak surge by 10-20%.
    # Reference: Weisberg & Zheng (2006) showed compact vortex surge focusing.
    #
    # Refinement: Extended the RMW threshold from 25km to 35km with a graduated
    # scale. Andrew (RMW ~22km) and Michael (RMW ~22km) were barely triggering the
    # original threshold. The physical effect — concentrated wind stress over a
    # narrow coastal segment — begins at ~35km RMW and intensifies as RMW shrinks.
    # Also added bay/funneling interaction: compact vortex surge is amplified further
    # when it enters constricted coastal geometry (bays, harbors) because the focused
    # surge jet interacts constructively with the funneling. Andrew's surge into
    # Biscayne Bay and Michael's surge into Mexico Beach's concave coastline both
    # showed this effect.
    compact_surge_bonus = 0.0
    if rmw_m and rmw_m < 35_000 and vmax_ms > 55.0:
        compactness = 1.0 - (rmw_m / 35_000)  # 0 at 35km, 1 at 0km
        intensity_excess = (vmax_ms - 55.0) / 30.0  # 0 at 55 m/s, 1 at 85 m/s
        intensity_excess = min(1.0, intensity_excess)
        # Base compact bonus: up to 20%
        compact_surge_bonus = compactness * intensity_excess * 0.20
        # Bay funneling interaction: when compact surge enters a constricted bay,
        # the focused jet amplifies further. Up to 8% additional.
        if profile.bay_funneling > 1.0:
            funnel_excess = (profile.bay_funneling - 1.0) / 0.35  # Norm: 0 at 1.0, 1 at 1.35
            funnel_excess = min(1.0, funnel_excess)
            compact_surge_bonus += compactness * intensity_excess * funnel_excess * 0.08
        compact_surge_bonus = min(0.28, compact_surge_bonus)  # Hard cap at 28%
    components["compact_surge_bonus"] = compact_surge_bonus

    # 1e. Combine surge components with regional modifiers
    raw_surge = (ib_surge_m + wind_surge_m) * speed_surge_factor * size_factor

    # Apply compact storm surge focus
    raw_surge *= (1.0 + compact_surge_bonus)

    # Apply regional factors
    amplified_surge = raw_surge * profile.surge_amplification * profile.bay_funneling

    # Subtract wetland buffer
    final_surge_m = amplified_surge * (1.0 - profile.wetland_buffer)

    # Add tidal contribution (assume worst case: near high tide)
    # Use 50% of tidal range as expected contribution
    final_surge_m += profile.tidal_range_m * 0.5

    # Size-pressure surge coupling for high-amplification shelves:
    # Very large storms (r34 > 200km) with deep pressure deficits (> 60 hPa)
    # making landfall on wide, shallow shelves (surge_amplification > 1.5)
    # generate enhanced surge from the sustained wind fetch across the broad
    # shallow bathymetry. This effect is specific to Gulf Coast geometry
    # (Louisiana, Yucatan) where the shelf+storm size resonance amplifies
    # surge beyond what the standard parametric model captures.
    # Reference: Irish et al. (2008) showed 15-25% surge enhancement for
    # large storms on wide shelves due to Ekman transport coupling.
    if (r34_m and r34_m > 200_000
            and pressure_deficit > 60
            and profile.surge_amplification > 1.5):
        size_norm = (r34_m / 200_000) - 1.0  # How much larger than reference
        pressure_norm = pressure_deficit / 100.0
        size_pressure_bonus = min(0.25, size_norm * 0.15 * pressure_norm)
        final_surge_m *= (1.0 + size_pressure_bonus)
        components["size_pressure_surge_bonus"] = size_pressure_bonus

    # Bathymetric concavity surge amplification
    # Concave coastal bathymetry (embayments, bights) focuses and amplifies
    # storm surge through constructive interference of continental shelf waves
    # converging toward the embayment apex. This operates at the 100-200km
    # scale of the shelf geometry itself, distinct from local bay funneling
    # (which operates at 1-10km harbor/estuary scale).
    #
    # The effect is most significant on wide, shallow shelves where the
    # concavity can direct wave energy over long distances. Steep shelves
    # (Caribbean islands) have minimal concavity effect because the shelf
    # is too narrow for wave focusing to develop.
    #
    # The amplification scales with:
    #   1. Concavity magnitude (bathymetric_concavity, 0-1)
    #   2. Shelf width (wider shelf = more focusing distance)
    #   3. Storm size (larger storms interact more with shelf geometry)
    #
    # Mississippi Bight (concavity=0.55): Katrina's surge was amplified 15-25%
    # by the bowl-shaped shelf geometry funneling wave energy toward the MS coast.
    # Tampa Bay area (concavity=0.35): Ian's surge was focused into Charlotte Harbor.
    # NY Bight (concavity=0.30): Sandy's surge was amplified by the coastal bend.
    #
    # Reference: Dietrich et al. (2011) "Modeling hurricane waves and storm surge";
    # Weisberg & Zheng (2006); Irish et al. (2008)
    concavity_bonus = 0.0
    if profile.bathymetric_concavity > 0.10:
        concavity_strength = profile.bathymetric_concavity
        # Shelf width interaction: wider shelves allow more focusing
        shelf_norm = min(1.0, profile.shelf_width_km / 150.0)  # Normalize to 150km
        # Storm size interaction: larger storms interact more with shelf geometry
        size_norm = min(1.0, (r34_m / 200_000) ** 0.4) if r34_m else 0.7
        # Reduce concavity when bay_funneling is already high to avoid
        # double-counting the surge focusing effect at different scales
        funnel_reduction = 1.0
        if profile.bay_funneling > 1.15:
            funnel_excess = (profile.bay_funneling - 1.15) / 0.20  # 0 at 1.15, 1 at 1.35
            funnel_reduction = max(0.3, 1.0 - funnel_excess * 0.5)  # Reduce by up to 50%
        concavity_bonus = concavity_strength * shelf_norm * size_norm * funnel_reduction * 0.15  # Up to ~15%
        concavity_bonus = min(0.15, concavity_bonus)
        final_surge_m *= (1.0 + concavity_bonus)
    components["concavity_bonus"] = concavity_bonus

    # Right-front quadrant surge bias
    # In the Northern Hemisphere, the right-front quadrant of a hurricane
    # produces the highest surge because:
    #   1. Cyclonic winds (counterclockwise) add to the forward translational
    #      speed on the right side, creating higher effective wind speeds
    #   2. The onshore-directed wind component is maximized in the right-front
    #      quadrant for storms approaching land from the east or south
    #
    # This effect is captured by the approach_angle_deg parameter:
    #   - Perpendicular approach (0°): standard surge (landfall point gets
    #     right-front quadrant surge if coast faces east)
    #   - Positive angle (coast to the right): amplified surge (right-front
    #     quadrant directly onshore)
    #
    # When approach_angle is not provided (None), we estimate it from the
    # storm's forward speed and latitude. Fast-moving storms approaching at
    # oblique angles to the coast tend to have amplified right-quadrant surge.
    # Katrina's approach to the MS coast was nearly perpendicular, maximizing
    # right-front quadrant interaction with the Mississippi Bight.
    # Reference: Jelesnianski et al. (1992); Weisberg & Zheng (2006)
    quadrant_surge_bias = 0.0
    if approach_angle_deg is not None and abs(approach_angle_deg) > 10:
        # Positive angle = right-front quadrant facing shore = amplified
        if approach_angle_deg > 0:
            angle_norm = min(1.0, approach_angle_deg / 45.0)
            quadrant_surge_bias = angle_norm * 0.08  # Up to 8% more surge
        else:
            # Negative = left-front facing shore = reduced surge
            angle_norm = min(1.0, abs(approach_angle_deg) / 45.0)
            quadrant_surge_bias = -angle_norm * 0.05  # Up to 5% less surge
        final_surge_m *= (1.0 + quadrant_surge_bias)
    components["quadrant_surge_bias"] = quadrant_surge_bias

    # Coastal defense attenuation
    # Engineered defenses (seawalls, barrier islands, revetments) reduce the
    # effective surge that reaches the mainland. This is distinct from wetland
    # buffering — coastal defenses are either geomorphological (barrier islands)
    # or engineered (seawalls, floodgates). The attenuation is applied after
    # all amplification factors because defenses operate on the final surge
    # at the coast, not on the offshore surge components.
    # Important: defenses have diminishing effectiveness as surge increases —
    # a 1m seawall helps at 2m surge but is irrelevant at 8m surge.
    # We model this as: effective_defense = coastal_defense × (1 - surge/12)
    # So at 12m surge, defenses provide zero benefit.
    # Reference: USACE Coastal Engineering Manual Ch. 5; Wamsley et al. (2010)
    if profile.coastal_defense > 0:
        surge_ratio = min(1.0, final_surge_m / 12.0)
        effective_defense = profile.coastal_defense * (1.0 - surge_ratio)

        # Compact vortex barrier island penetration
        # Compact Cat 4-5 storms (RMW < 25km, Vmax > 60 m/s) generate an
        # intensely focused surge jet that punches through barrier islands
        # rather than being attenuated by them. Andrew (1992, RMW ~22km,
        # 145 kt) drove its concentrated surge directly through Miami Beach's
        # barrier islands into Biscayne Bay with minimal attenuation.
        # The standard coastal_defense model assumes the surge is spread over
        # a wide front where barrier islands can dissipate energy; compact
        # vortex surge is instead a narrow, high-momentum jet that overwashes
        # or breaches barrier islands at a point, bypassing the attenuation.
        # When this condition is met, reduce the defense effectiveness by up
        # to 80%, effectively modeling barrier island penetration.
        # Reference: Powell & Houston (1996) "Hurricane Andrew's landfall in
        # South Florida"; Sallenger et al. (2006) "Storm impact scale for
        # barrier islands"
        compact_barrier_penetration = 0.0
        if rmw_m and rmw_m < 25_000 and vmax_ms > 60.0:
            compactness = 1.0 - (rmw_m / 25_000)  # 0 at 25km, 1 at 0km
            intensity_excess = min(1.0, (vmax_ms - 60.0) / 25.0)  # 0 at 60, 1 at 85
            compact_barrier_penetration = min(0.80, compactness * intensity_excess * 1.0)
            effective_defense *= (1.0 - compact_barrier_penetration)
        components["compact_barrier_penetration"] = compact_barrier_penetration

        final_surge_m *= (1.0 - effective_defense)
        components["coastal_defense_reduction"] = effective_defense
    else:
        components["coastal_defense_reduction"] = 0.0
        components["compact_barrier_penetration"] = 0.0

    # Compact vortex direct wind setup on narrow shelves
    # For compact Cat 5 storms (RMW < 25km, Vmax > 65 m/s) making landfall on
    # narrow continental shelves (< 40km), the standard parametric surge model
    # underestimates surge because it relies on shelf_factor amplification (which
    # is minimal for narrow shelves). However, the intense eyewall vortex generates
    # significant localized wind setup independent of shelf geometry — the extreme
    # wind speeds (~150+ kt) in a tight core push water directly ahead like a
    # piston through any constricted coastal geometry (bays, inlets, channels).
    #
    # Andrew (1992) is the canonical example: Cat 5 with 12nm RMW on FL East Coast's
    # 15km shelf. The narrow shelf gave minimal parametric amplification, but the
    # intense compact vortex drove a 5.2m surge directly through Biscayne Bay's
    # narrow entrance — a localized vortex-scale wind setup that overwhelmed the
    # shelf geometry limitations.
    #
    # The effect is suppressed on wide shelves (>40km) where the standard
    # shelf_factor amplification already captures wind-driven surge adequately.
    # It also requires fast-moving storms (>5 m/s) because the vortex needs
    # translational speed to push the water pile ahead of it — stalling storms
    # on narrow shelves don't generate this piston effect.
    #
    # Reference: Powell & Houston (1996) "Hurricane Andrew's landfall in South
    # Florida"; Weisberg & Zheng (2006) "Vortex-driven surge focusing"
    compact_vortex_setup = 0.0
    if (rmw_m and rmw_m < 35_000 and vmax_ms > 60.0
            and profile.shelf_width_km < 50 and fwd_speed > 4.0):
        narrow_shelf_norm = max(0.0, 1.0 - profile.shelf_width_km / 50.0)
        intensity_norm = min(1.0, (vmax_ms - 60.0) / 25.0)  # 0 at 60, 1 at 85 m/s
        compactness_norm = min(1.0, 1.0 - rmw_m / 35_000)   # 0 at 35km, 1 at 0
        # Bay interaction: compact vortex surge amplifies further when it enters
        # constricted coastal geometry (bays, inlets)
        bay_boost = 1.0
        if profile.bay_funneling > 1.0:
            bay_boost = 1.0 + (profile.bay_funneling - 1.0) * 0.5
        compact_vortex_setup = (
            narrow_shelf_norm * intensity_norm * compactness_norm * bay_boost * 0.80
        )
        compact_vortex_setup = min(1.0, compact_vortex_setup)  # Cap at 1.0m additional
        final_surge_m += compact_vortex_setup
    components["compact_vortex_setup"] = compact_vortex_setup

    # Clamp to physical limits (highest recorded: ~8.5m Katrina, 7.3m Michael)
    final_surge_m = max(0.0, min(final_surge_m, 12.0))
    components["raw_surge_m"] = raw_surge
    components["amplified_surge_m"] = amplified_surge

    # Compute surge nullification factor
    # How much does this coast reduce surge vs. worst-case geography?
    # max_amplification possible is ~1.60 * 1.40 = 2.24 (Louisiana + Tampa Bay)
    max_possible_amp = 1.60 * 1.40
    actual_amp = profile.surge_amplification * profile.bay_funneling * (1 - profile.wetland_buffer)
    surge_nullification = 1.0 - (actual_amp / max_possible_amp)
    surge_nullification = max(0.0, min(1.0, surge_nullification))

    # Normalize surge to 0-100 score
    # Reference: 8m surge = score of 100 (extreme, Katrina-level)
    surge_score = min(100.0, (final_surge_m / 8.0) * 100.0)

    # ===================================================================
    # PART 2: RAINFALL ACCUMULATION
    # ===================================================================

    # 2a. Base rainfall rate from Lonfat et al. (2004) climatology
    # Average TC rainfall rate near RMW: ~10-15 mm/hr for Cat 1, ~20-30 for Cat 4-5
    vmax_kt = vmax_ms / 0.514444
    if vmax_kt < 50:
        rain_rate_mmhr = 5.0 + 0.10 * vmax_kt  # TS to weak Cat 1
    elif vmax_kt < 100:
        rain_rate_mmhr = 10.0 + 0.15 * (vmax_kt - 50)  # Cat 1-3
    else:
        rain_rate_mmhr = 17.5 + 0.10 * (vmax_kt - 100)  # Cat 4-5
    components["rain_rate_mmhr"] = rain_rate_mmhr

    # 2b. Residence time (how long the storm affects a point)
    # Slower storms = longer residence time = more total rainfall
    # Typical rain shield extends ~1.5x r34
    rain_shield_diameter_km = (r34_m * 1.5 * 2) / 1000.0  # km
    residence_hours = rain_shield_diameter_km / (fwd_speed * 3.6)  # km / km/hr
    # Clamp: storms don't usually stall for more than 72h or pass in less than 3h
    residence_hours = max(3.0, min(72.0, residence_hours))
    components["residence_hours"] = residence_hours

    # 2c. SST / moisture factor
    # Warmer SSTs (lower latitudes) provide more atmospheric moisture
    # Clausius-Clapeyron: ~7% more moisture per degree C
    #
    # Use real SST from Open-Meteo marine API or Google Weather API
    # when available. Fall back to latitude-based climatological estimate.
    # Real SST is critical because:
    #   1. Gulf Loop Current creates localized SST anomalies (+2-3°C)
    #   2. Cold-core eddies can suppress SST by 2-4°C
    #   3. Post-storm cooling reduces SST along the track
    #   4. Seasonal variation not captured by latitude alone
    # Reference: Mainelli et al. (2008) "TC intensity prediction using SST"
    if real_sst_c is not None:
        sst_approx = real_sst_c
        components["sst_source"] = "api"
        logger.debug(f"Using real SST: {real_sst_c:.1f}°C at ({lat:.1f}, {lon:.1f})")
    else:
        sst_approx = 30.0 - 0.3 * abs(lat - 15)  # Peaks at 15°N
        sst_approx = max(24, min(31, sst_approx))
        components["sst_source"] = "climatology"
    moisture_factor = 1.0 + 0.07 * (sst_approx - 27.0)  # Normalized to 27°C
    components["sst_c"] = sst_approx
    components["moisture_factor"] = moisture_factor

    # 2d. Total rainfall estimate
    # Rain = rate × residence_time × moisture × orographic enhancement
    # But apply a decay factor since not all of residence time gets peak rates
    # Peak rates occur within ~2-3 RMW radii, outer bands contribute less
    peak_fraction = 0.4  # ~40% of total rain comes at peak rate
    outer_fraction = 0.6  # ~60% at reduced rate (~40% of peak)

    effective_rate = rain_rate_mmhr * (peak_fraction + outer_fraction * 0.4)
    rainfall_total_mm = effective_rate * residence_hours * moisture_factor * profile.rain_enhancement

    # Terrain-intensity rainfall coupling:
    # Mountainous islands (Puerto Rico, Jamaica, Hispaniola) produce
    # disproportionately more rainfall when hit by intense storms (>50 m/s)
    # because the forced orographic ascent of the hurricane's deep moisture
    # column triggers embedded mesoscale convective systems on the windward
    # slopes. The standard rain_enhancement captures the average orographic
    # effect, but the interaction between extreme wind-driven moisture flux
    # and steep terrain amplifies rainfall non-linearly.
    # Maria (2017) dumped extreme rainfall on PR not just from stalling
    # but from the intense interaction with the Cordillera Central.
    # Reference: Smith et al. (2009) "Orographic precipitation and climate"
    #
    # Enhanced with terrain module when available.
    # The terrain module provides actual mountain range data with elevation,
    # orientation, and windward/leeward asymmetry — a major improvement over
    # the single rain_enhancement parameter per region. This captures:
    #   1. Jamaica Blue Mountains (2256m) — amplified Melissa's 18-24" rainfall
    #   2. Cordillera Central, DR/Haiti (3098m) — flash flood risk
    #   3. Sierra de Luquillo, PR (1065m) — Maria's extreme rainfall
    #   4. Appalachians — Florence/Helene inland flooding amplification
    terrain_rain_bonus = 0.0
    orographic_factor = 1.0
    tr = _get_terrain()
    if tr is not None:
        try:
            orographic_factor = tr.compute_orographic_factor(
                lat, lon, storm_approach_deg=storm_approach_heading_deg
            )
            components["orographic_factor"] = orographic_factor
            components["orographic_source"] = "terrain_module"
            # Apply orographic enhancement from terrain module
            # Subtract 1.0 because 1.0 is neutral (no enhancement)
            if orographic_factor > 1.05:
                # Scale with intensity — stronger storms drive more moisture upslope
                intensity_norm = min(1.0, vmax_ms / 60.0)  # Full effect at Cat 3+
                effective_orographic = 1.0 + (orographic_factor - 1.0) * intensity_norm
                rainfall_total_mm *= effective_orographic
                terrain_rain_bonus = effective_orographic - 1.0
                # Get elevation vulnerability for flash flood risk assessment
                elev_vuln = tr.compute_elevation_vulnerability(lat, lon)
                if elev_vuln:
                    components["valley_flood_risk"] = elev_vuln.get("valley_flooding_risk", 0)
                    components["slope_runoff_factor"] = elev_vuln.get("slope_runoff_factor", 1.0)
        except Exception as e:
            logger.debug(f"Terrain module error: {e}, falling back to profile-based")
            orographic_factor = 1.0

    # Fallback: legacy profile-based terrain enhancement (when terrain module unavailable)
    if orographic_factor <= 1.05:
        components["orographic_source"] = "profile"
        if profile.rain_enhancement > 1.25 and vmax_ms > 50.0:
            terrain_factor = (profile.rain_enhancement - 1.25) / 0.45  # 0 at 1.25, 1 at 1.70
            terrain_factor = min(1.0, terrain_factor)
            intensity_norm = min(1.0, (vmax_ms - 50.0) / 35.0)  # 0 at 50 m/s, 1 at 85 m/s
            terrain_rain_bonus = terrain_factor * intensity_norm * 0.12  # Up to 12%
            rainfall_total_mm *= (1.0 + terrain_rain_bonus)
    components["terrain_rain_bonus"] = terrain_rain_bonus

    # Stalling storm convective cycling amplification
    # When storms move very slowly (<3 m/s), mesoscale convective bands
    # repeatedly cycle over the same location. This creates a non-linear
    # amplification beyond what residence_time alone captures — the repeated
    # passage of intense convective cells produces rainfall totals that exceed
    # simple rate × time predictions. Harvey (2017, 1.5 m/s fwd speed) and
    # Dorian (2019, 1.5 m/s) are canonical examples.
    # Reference: Trenberth et al. (2018), "Harvey, Irma, and Maria rainfall"
    stall_rain_bonus = 0.0
    if fwd_speed < 3.0:
        stall_factor = (3.0 - fwd_speed) / 3.0  # 0 at 3 m/s, 1 at 0 m/s
        stall_rain_bonus = stall_factor * 0.15  # Up to 15% more rainfall
        rainfall_total_mm *= (1.0 + stall_rain_bonus)
    components["stall_rain_bonus"] = stall_rain_bonus

    # River basin flood amplification
    # When storms stall or slow over major river basins, upstream rainfall
    # drains into coastal and lowland areas over the following 2-5 days,
    # producing compound flooding that exceeds what the local rain accumulation
    # model predicts. This effect is most severe when:
    #   1. Forward speed is slow (< 5 m/s) — more time for upstream accumulation
    #   2. The region has significant river basin catchment (river_basin_factor > 1.0)
    #   3. Total rainfall is already heavy (> 300mm) — saturated watersheds
    #
    # The amplification scales with stalling severity: at fwd=5 m/s the full
    # river_basin_factor applies; at faster speeds it's reduced proportionally
    # because fast-moving storms don't dump enough upstream rain to create
    # significant delayed riverine flooding.
    #
    # Harvey (2017, fwd=2.6 m/s, Central TX, river_basin_factor=1.20):
    #   Upstream Brazos/San Jacinto rain drained into Houston bayou system,
    #   creating week-long flooding that exceeded direct rainfall accumulation.
    # Florence (2018, fwd=2.6 m/s, NC, river_basin_factor=1.18):
    #   Cape Fear River crested 3 days after Florence stalled, flooding
    #   communities that were not in the direct storm path.
    #
    # Reference: Villarini & Goska (2020) "River flooding from tropical cyclones"
    river_flood_bonus = 0.0
    if profile.river_basin_factor > 1.0 and fwd_speed < 5.0 and rainfall_total_mm > 300:
        basin_strength = profile.river_basin_factor - 1.0  # Excess above neutral
        stall_scaling = (5.0 - fwd_speed) / 5.0  # 0 at 5 m/s, 1 at 0 m/s
        rain_saturation = min(1.0, (rainfall_total_mm - 300) / 500.0)  # 0 at 300, 1 at 800
        river_flood_bonus = basin_strength * stall_scaling * rain_saturation
        rainfall_total_mm *= (1.0 + river_flood_bonus)
    components["river_flood_bonus"] = river_flood_bonus

    # Clamp to physical limits (Harvey 2017 was ~1500mm over 5 days, extreme outlier)
    rainfall_total_mm = max(0.0, min(1500.0, rainfall_total_mm))
    components["effective_rate"] = effective_rate

    # Normalize rainfall to 0-100 score
    # Reference: 500mm = score of 100 (extreme, Harvey-level without the 5-day stall)
    rainfall_score = min(100.0, (rainfall_total_mm / 500.0) * 100.0)

    # ===================================================================
    # PART 3: COMPOUND FLOODING INTERACTION
    # ===================================================================

    # When surge and rain happen simultaneously, drainage systems back up.
    # The interaction is super-linear: combined impact > sum of parts.
    #
    # Compound score = w_surge * surge_score + w_rain * rain_score
    #                  + interaction * sqrt(surge_score * rain_score)
    #
    # The interaction term captures the non-linear amplification.

    # Antecedent soil moisture amplification
    # When soils are already saturated (antecedent_moisture > 0.5), rainfall
    # produces more surface runoff because infiltration capacity is reduced.
    # This amplifies the compound flooding interaction: surge backs up drainage
    # while rain falls on ground that can't absorb it. The amplification only
    # affects the interaction term (not base surge or base rain scores) because
    # antecedent moisture specifically amplifies the compounding effect.
    #
    # Florence (2018) hit NC (antecedent_moisture=0.65) after a wet summer,
    # and the saturated coastal plain caused rivers to crest at record levels
    # from rainfall that would have been manageable on dry soil.
    # Conversely, Harvey (2017) hit central TX (antecedent_moisture=0.45)
    # where the severity was driven by sheer volume, not soil saturation.
    #
    # When real soil moisture from Open-Meteo API is available,
    # it captures:
    #   1. Actual pre-storm rainfall saturation (not seasonal climatology)
    #   2. Prior storm passage effects (back-to-back storms compound flooding)
    #   3. Drought conditions that reduce flooding risk
    #   4. Week-to-week variability from actual weather patterns
    # When real data is unavailable, falls back to profile-based estimates.
    #
    # The effect scales: moisture above 0.5 amplifies, below 0.5 slightly reduces.
    # Maximum amplification: 15% boost to the interaction term.
    # Reference: Brauer et al. (2020); Villarini et al. (2014)
    if real_soil_moisture is not None:
        antecedent_moisture = real_soil_moisture
        components["soil_moisture_source"] = "api"
        logger.debug(f"Using real soil moisture: {real_soil_moisture:.2f} at ({lat:.1f}, {lon:.1f})")
    else:
        antecedent_moisture = profile.antecedent_moisture
        components["soil_moisture_source"] = "profile"
    components["antecedent_moisture"] = antecedent_moisture

    moisture_interaction_factor = 1.0
    if antecedent_moisture > 0.5:
        moisture_excess = (antecedent_moisture - 0.5) / 0.5  # 0 at 0.5, 1 at 1.0
        moisture_interaction_factor = 1.0 + moisture_excess * 0.15  # Up to 15% boost
    elif antecedent_moisture < 0.4:
        moisture_deficit = (0.4 - antecedent_moisture) / 0.4  # 0 at 0.4, 1 at 0
        moisture_interaction_factor = 1.0 - moisture_deficit * 0.08  # Up to 8% reduction
    components["moisture_interaction_factor"] = moisture_interaction_factor

    w_surge = 0.50
    w_rain = 0.30
    w_interaction = 0.20

    interaction = math.sqrt(max(0, surge_score) * max(0, rainfall_score))
    interaction *= moisture_interaction_factor  # Apply antecedent moisture interaction
    compound_flood_score = (
        w_surge * surge_score
        + w_rain * rainfall_score
        + w_interaction * interaction
    )
    compound_flood_score = min(100.0, compound_flood_score)

    return SurgeRainfallResult(
        surge_height_m=final_surge_m,
        surge_score=surge_score,
        rainfall_total_mm=rainfall_total_mm,
        rainfall_score=rainfall_score,
        compound_flood_score=compound_flood_score,
        surge_nullification=surge_nullification,
        region_key=region_key,
        components=components,
    )
