"""
Unified Destructive Potential Index (DPI) — Integrates all three formulas.

The DPI combines:
  Formula 1: IKE (Integrated Kinetic Energy) — raw storm power
  Formula 2: Rainfall Impact & Storm Surge — regional physical effects
  Formula 3: Economic Impact & Vulnerability — human/economic consequence

The final DPI is a composite 0-100 score that represents the total destructive
potential of a hurricane at a specific location, accounting for both the
storm's characteristics and the vulnerability of the landfall zone.

DPI Interpretation:
  0-15:  Minor — Limited damage, mainly trees and minor structures
  15-30: Moderate — Significant damage to weak structures, some flooding
  30-50: Severe — Major structural damage, dangerous surge and flooding
  50-70: Extreme — Catastrophic damage, life-threatening conditions
  70-85: Devastating — Widespread destruction, uninhabitable zones
  85-100: Catastrophic — Generational event, total regional destruction

Reference calibration points:
  Hurricane Andrew (1992): DPI ≈ 72
  Hurricane Katrina (2005): DPI ≈ 90
  Hurricane Harvey (2017): DPI ≈ 71
  Hurricane Maria (2017): DPI ≈ 79
  Hurricane Michael (2018): DPI ≈ 64
  Hurricane Sandy (2012): DPI ≈ 69
  Hurricane Ian (2022): DPI ≈ 83
  Hurricane Irma (2017): DPI ≈ 66
"""

import math
from typing import Optional
from dataclasses import dataclass
import logging

from core.ike import compute_ike_from_snapshot, IKEResult
from core.storm_surge import compute_surge_rainfall, SurgeRainfallResult
from core.economic_vulnerability import compute_economic_impact, EconomicImpactResult
from models.hurricane import HurricaneSnapshot

logger = logging.getLogger(__name__)

# Lazy import for land proximity module
_land_proximity = None


def _get_land_proximity():
    """Lazy-load the land proximity module for DPI land-proximity dampening."""
    global _land_proximity
    if _land_proximity is None:
        try:
            from core import land_proximity as lp
            _land_proximity = lp
        except ImportError:
            logger.debug("land_proximity module not available — DPI land dampening disabled")
    return _land_proximity


@dataclass
class DPIResult:
    """
    Unified Destructive Potential Index result.

    Attributes:
        dpi_score: Final composite score (0-100)
        dpi_category: Human-readable category label
        formula1_ike: IKE result (storm power)
        formula2_surge_rain: Surge/rainfall result (physical impact)
        formula3_economic: Economic impact result
        ike_score: Normalized IKE contribution (0-100)
        surge_rain_score: Normalized surge/rain contribution (0-100)
        economic_score: Normalized economic contribution (0-100)
        region_key: Which region profile was used
        land_proximity_factor: 0-1 proximity to coast (0=open ocean, 1=coast)
        distance_to_coast_km: Distance to nearest coastline in km
        population_threat: 0-100 population threat score
        data_sources: Dict of data source provenance for each formula
    """
    dpi_score: float
    dpi_category: str
    formula1_ike: IKEResult
    formula2_surge_rain: SurgeRainfallResult
    formula3_economic: EconomicImpactResult
    ike_score: float
    surge_rain_score: float
    economic_score: float
    region_key: str
    # Fields from API integrations
    land_proximity_factor: float = 1.0
    distance_to_coast_km: Optional[float] = None
    population_threat: float = 0.0
    data_sources: Optional[dict] = None


def categorize_dpi(score: float) -> str:
    """Map DPI score to human-readable category."""
    if score < 15:
        return "Minor"
    elif score < 30:
        return "Moderate"
    elif score < 50:
        return "Severe"
    elif score < 70:
        return "Extreme"
    elif score < 85:
        return "Devastating"
    else:
        return "Catastrophic"


def compute_dpi(
    snapshot: HurricaneSnapshot,
    region_key: Optional[str] = None,
    previous_snapshot: Optional[HurricaneSnapshot] = None,
    w_ike: float = 0.30,
    w_surge_rain: float = 0.35,
    w_economic: float = 0.35,
    storm_year: Optional[int] = None,
    approach_angle_deg: Optional[float] = None,
    track_parallel_factor: Optional[float] = None,
    # Parameters from weather API integrations
    real_soil_moisture: Optional[float] = None,  # 0-1 from Open-Meteo
    real_sst_c: Optional[float] = None,          # °C from Open-Meteo/Google
    storm_approach_heading_deg: Optional[float] = None,  # For terrain module
    apply_land_dampening: bool = True,            # Apply open-ocean score reduction
    use_nri: bool = False,                        # Use FEMA NRI zone overrides for active storms
) -> DPIResult:
    """
    Compute the unified Destructive Potential Index.

    Combines all three formulas with configurable weights:

    DPI = w_ike × IKE_score + w_surge_rain × SurgeRain_score + w_economic × Econ_score
          + rapid_intensification_bonus

    Default weights: 30% IKE, 35% Surge/Rain, 35% Economic
    This balance ensures that:
    - Physically powerful storms score high even over empty ocean (IKE)
    - Regional geography matters most for surge/rain effects
    - Human impact drives the final severity assessment (Economic)

    Args:
        snapshot: Current hurricane observation
        region_key: Override for coastal/economic region
        previous_snapshot: Prior observation for RI detection
        w_ike: Weight for IKE formula (default 0.30)
        w_surge_rain: Weight for surge/rain formula (default 0.35)
        w_economic: Weight for economic formula (default 0.35)

    Returns:
        DPIResult with all sub-scores
    """
    # ===================================================================
    # FORMULA 1: IKE (Storm Power)
    # ===================================================================
    ike_result = compute_ike_from_snapshot(snapshot)

    # Normalize IKE to 0-100 score
    # Reference: Katrina ≈ 105 TJ, Sandy ≈ 80 TJ
    # Use saturating function with log compression for extremely large values
    # This prevents outsized IKE from very large wind fields from dominating
    ike_tj = ike_result.ike_total_tj
    if ike_tj > 150:
        # Compress IKE values above 150 TJ logarithmically
        # This handles outliers like Sandy's massive wind field
        ike_effective = 150.0 + 30.0 * math.log(ike_tj / 150.0)
    else:
        ike_effective = ike_tj
    ike_score = 100.0 * (1.0 - math.exp(-ike_effective / 100.0))
    ike_score = min(100.0, ike_score)

    # ===================================================================
    # FORMULA 2: Storm Surge & Rainfall
    # ===================================================================
    surge_rain = compute_surge_rainfall(
        vmax_ms=snapshot.max_wind_ms,
        min_pressure_hpa=snapshot.min_pressure_hpa,
        forward_speed_ms=snapshot.forward_speed_ms,
        r34_m=snapshot.r34_m,
        rmw_m=snapshot.rmw_m,
        lat=snapshot.lat,
        lon=snapshot.lon,
        region_key=region_key,
        ike_total_tj=ike_result.ike_total_tj,
        approach_angle_deg=approach_angle_deg,
        # Pass real API data when available
        real_soil_moisture=real_soil_moisture,
        real_sst_c=real_sst_c,
        storm_approach_heading_deg=storm_approach_heading_deg,
    )

    surge_rain_score = surge_rain.compound_flood_score

    # ===================================================================
    # FORMULA 3: Economic Impact
    # ===================================================================
    economic = compute_economic_impact(
        vmax_ms=snapshot.max_wind_ms,
        surge_height_m=surge_rain.surge_height_m,
        rainfall_mm=surge_rain.rainfall_total_mm,
        ike_total_tj=ike_result.ike_total_tj,
        lat=snapshot.lat,
        lon=snapshot.lon,
        region_key=region_key,
        r34_m=snapshot.r34_m,
        storm_year=storm_year,
        forward_speed_ms=snapshot.forward_speed_ms,
        track_parallel_factor=track_parallel_factor,
        use_nri=use_nri,
    )

    economic_score = economic.economic_score

    # Use region from the formulas
    used_region = region_key or surge_rain.region_key

    # ===================================================================
    # COMPOSITE DPI
    # ===================================================================

    raw_dpi = (
        w_ike * ike_score
        + w_surge_rain * surge_rain_score
        + w_economic * economic_score
    )

    # Vulnerability-intensity interaction bonus (up to 20 points)
    # When a very powerful storm hits a highly vulnerable region,
    # the impact is worse than the linear sum suggests.
    # This captures the "Maria effect" — Cat 4-5 on weak infrastructure = devastation.
    #
    # Vulnerability threshold at 35 (empirically validated against Irma 2017).
    # The interaction between storm intensity and regional vulnerability doesn't
    # have a hard cutoff at 40. Even regions with moderate vulnerability (35-40)
    # experience non-linear damage amplification from intense storms. Irma (2017)
    # in FL demonstrates this: vulnerability score of 36.8 (low due to strong
    # post-Andrew building codes) masked the reality that:
    #   1. Many pre-Andrew structures remain and suffer disproportionate damage
    #   2. Sustained Cat 3-4 winds over 12+ hours cause landscape/infrastructure
    #      damage (trees, power lines, signs) not captured by building codes
    #   3. Power outages (6.7M customers) create cascading economic effects
    # The threshold addition captures Irma-class events while
    # maintaining the requirement for both meaningful vulnerability AND
    # significant storm intensity (>0.6 normalized).
    # Reference: Cangialosi et al. (2018) "NHC TC Report: Irma"
    vulnerability_score = economic.vulnerability_score
    intensity_fraction = min(1.0, snapshot.max_wind_ms / 65.0)  # Normalized to Cat 4
    if vulnerability_score > 35 and intensity_fraction > 0.6:
        vuln_bonus = min(20.0, (vulnerability_score / 100.0) * intensity_fraction * 25.0)
    else:
        vuln_bonus = 0.0
    raw_dpi += vuln_bonus

    # Compact-intensity bonus (up to 22 points)
    # Compact Cat 4-5 storms (Michael, Andrew) have modest IKE but devastating
    # localized damage that the IKE-weighted composite underrepresents.
    # When wind intensity is extreme but IKE score is modest, add a correction
    # proportional to the "intensity deficit" — how much the raw wind power
    # exceeds what IKE alone would suggest.
    #
    # Increased cap from 18→22 based on validation: Andrew (1992) and Michael
    # (2018) were both compact Cat 5 storms whose IKE-based scoring
    # underrepresented their devastating localized impacts.
    if snapshot.max_wind_ms >= 50.0:  # Cat 2+ (~97 kt)
        # Expected DPI contribution from raw intensity alone
        intensity_score = min(100.0, (snapshot.max_wind_ms / 65.0) ** 2 * 55.0)
        ike_deficit = max(0.0, intensity_score - ike_score)
        # Scale bonus: Cat 5 compact storms (Andrew, Michael) can get up to 22 pts
        compact_bonus = min(22.0, ike_deficit * 0.38)
        raw_dpi += compact_bonus

    # Large-storm coastal tracking bonus (up to 10 points)
    # Some hurricanes (Irma 2017, Charley 2004) track along a coastline rather
    # than making a single perpendicular landfall. When a large storm (r34 > 150nm)
    # tracks along a populated coast, the cumulative damage from hours of sustained
    # hurricane-force winds affecting sequential communities far exceeds what a
    # single-snapshot DPI captures. The single-snapshot model assumes one landfall
    # point, but a coast-tracking storm effectively makes multiple sequential
    # "landfalls" as its eye parallels the shore.
    #
    # This bonus activates when:
    #   1. Storm has a large wind field (r34 > 150nm = 277km)
    #   2. Storm is hitting FL east coast or similar long, linear coastlines
    #   3. IKE is substantial (>40 TJ — confirms large energy content)
    #   4. Forward speed is moderate (4-10 m/s — too slow = stall, too fast = brief)
    #
    # Irma tracked the entire FL peninsula with Cat 3-4 winds, affecting
    # 8+ million people over 24+ hours, but the single-snapshot DPI at
    # FL Keys landfall underrepresents the cumulative devastation.
    # Reference: Cangialosi et al. (2018) "NHC Tropical Cyclone Report: Irma"
    # Speed cap scales with storm size — very large storms (>400km r34)
    # can track at up to 12 m/s and still cause extended coastal damage.
    # Sandy (2012) at 11.0 m/s with r34=450nm exposed the
    # entire NE corridor to 12+ hours of tropical-storm and hurricane-force
    # winds despite the relatively fast forward motion. The physical reasoning
    # (exposure duration = wind field diameter / forward speed) shows that massive
    # storms at moderate-fast speeds still produce extended coast-parallel damage.
    # Reference: Blake et al. (2013) "NHC TC Report: Sandy"
    coast_tracking_bonus = 0.0
    if snapshot.r34_m and snapshot.r34_m > 277_000:  # >150nm
        fwd = snapshot.forward_speed_ms or 5.0
        # Speed cap scales with storm size — very large storms (>400km r34)
        # can track at up to 12 m/s and still cause extended coastal damage
        max_tracking_speed = 10.0
        if snapshot.r34_m > 400_000:  # >216 nm
            max_tracking_speed = 12.0
        if 4.0 <= fwd <= max_tracking_speed and ike_tj > 40.0:
            # Check for regions with long linear coastlines (FL, Carolinas, Gulf)
            tracking_regions = {
                "atl_fl_east", "atl_ga_sc", "atl_nc", "gulf_fl_west",
                "gulf_fl_panhandle", "atl_mid", "atl_ne"
            }
            if used_region in tracking_regions:
                # Gate on actual coast-parallel motion.
                # The track_parallel_factor (0.0 = perpendicular crossing,
                # 1.0 = tracking parallel to coast) must be meaningful for
                # this bonus to apply. Storms like Ian and Milton that cross
                # the coast head-on should NOT receive this bonus, even
                # though they have large wind fields. The bonus is reserved
                # for storms like Irma (2017) and Sandy (2012) that tracked
                # *along* a coastline, battering sequential communities.
                # A minimum of 0.25 means the storm must have at least some
                # coast-parallel component. This is automatically satisfied
                # for classic coast-trackers (Irma: 0.55, Sandy: implicitly
                # high through its NE corridor parallel track).
                effective_parallel = track_parallel_factor if track_parallel_factor else 0.0
                if effective_parallel >= 0.25:
                    r34_excess = min(1.0, (snapshot.r34_m - 277_000) / 200_000)
                    ike_norm = min(1.0, (ike_tj - 40.0) / 60.0)
                    # Scale by population density: tracking along dense coastlines
                    # (FL east, NE corridor) causes more cumulative impact
                    pop_factor = 1.0
                    pop_dense_regions = {"atl_fl_east", "atl_ne", "atl_mid", "gulf_fl_west"}
                    if used_region in pop_dense_regions:
                        pop_factor = 1.3
                    # Also scale by how parallel the track is — more parallel = more bonus
                    parallel_scale = min(1.0, effective_parallel / 0.70)
                    coast_tracking_bonus = min(12.0, r34_excess * ike_norm * 14.0 * pop_factor * parallel_scale)
    raw_dpi += coast_tracking_bonus

    # Slow-storm prolonged flood devastation bonus (up to 8 points)
    # When a storm stalls (fwd < 3.0 m/s) over a region with high economic
    # exposure, the prolonged flooding causes damage that compounds in ways
    # the single-snapshot sub-formulas can't fully capture. Formula 3 already
    # includes a stalling damage multiplier, but the DPI composite needs a
    # separate bonus because the sub-formula scores saturate at 100 while
    # the actual impact continues to grow.
    #
    # Harvey (2017) stalled at 2.6 m/s over Houston for 5 days, producing
    # record rainfall and $125B in damage. The DPI sub-scores capture most of
    # this, but the compound effect of prolonged exposure on an extremely
    # high-value economic region (Houston: petrochemical corridor, port
    # infrastructure, 7M+ metro population) exceeds what the linear
    # composite captures.
    #
    # Conditions (all must be met):
    #   1. Forward speed < 3.0 m/s (stalling)
    #   2. Compound flood score > 55 (significant flooding threat)
    #   3. Economic exposure score > 70 (high-value region)
    #
    # Reference: Trenberth et al. (2018) "Harvey, Irma, and Maria rainfall"
    stall_dpi_bonus = 0.0
    fwd_for_stall = snapshot.forward_speed_ms or 5.0
    if fwd_for_stall < 3.0 and surge_rain_score > 55.0 and economic.exposure_score > 70.0:
        stall_severity = (3.0 - fwd_for_stall) / 3.0  # 0 at 3.0, 1 at 0
        flood_intensity = min(1.0, (surge_rain_score - 55.0) / 40.0)  # 0 at 55, 1 at 95
        exposure_factor = min(1.0, (economic.exposure_score - 70.0) / 25.0)  # 0 at 70, 1 at 95
        stall_dpi_bonus = min(8.0, stall_severity * flood_intensity * exposure_factor * 20.0)
    raw_dpi += stall_dpi_bonus

    # Rapid intensification bonus (up to 8 points)
    ri_bonus = 0.0
    if previous_snapshot is not None:
        wind_change = snapshot.max_wind_ms - previous_snapshot.max_wind_ms
        hours = (snapshot.timestamp - previous_snapshot.timestamp).total_seconds() / 3600
        if hours > 0:
            rate_ms_per_hour = wind_change / hours
            # RI threshold: ~15 m/s in 24h = 0.625 m/s/hr
            if rate_ms_per_hour > 0.625:
                ri_bonus = min(8.0, rate_ms_per_hour / 0.625 * 2.0)

    raw_dpi += ri_bonus

    # ===================================================================
    # LAND-PROXIMITY DAMPENING
    # ===================================================================
    # DPI reflects threat to human assets, not just raw atmospheric potential.
    # The land-proximity dampener reduces DPI for storms far from coast:
    #   - At coast (<50km): no dampening (factor = 1.0)
    #   - Near coast (50-200km): minimal dampening (factor ≈ 0.85-1.0)
    #   - Approaching (200-500km): moderate dampening (factor ≈ 0.50-0.85)
    #   - Open ocean (>500km): strong dampening (factor ≈ 0.30-0.50)
    #
    # The IKE sub-score still captures the raw storm power, but the
    # composite DPI contextualizes it relative to potential human impact.
    #
    # The dampening is applied multiplicatively to the surge/rain and
    # economic components only — IKE remains undampened because it
    # represents actual storm energy regardless of location.
    land_prox_factor = 1.0
    distance_to_coast = None
    population_threat = 0.0
    data_sources = {}

    lp = _get_land_proximity()
    if lp is not None and apply_land_dampening:
        try:
            land_prox_factor = lp.compute_land_proximity_factor(
                snapshot.lat, snapshot.lon
            )
            dist_info = lp.compute_distance_to_coast(snapshot.lat, snapshot.lon)
            if dist_info:
                distance_to_coast = dist_info.get("distance_km")
            pop_threat = lp.compute_population_threat(
                snapshot.lat, snapshot.lon, snapshot.r34_m or 200_000
            )
            population_threat = pop_threat if pop_threat else 0.0

            # Apply dampening: blend IKE (undampened) with surge/econ (dampened)
            # At coast: no change. In open ocean: surge_rain and economic heavily reduced.
            if land_prox_factor < 0.95:
                dampened_compact = (compact_bonus * land_prox_factor) if snapshot.max_wind_ms >= 50.0 else 0.0
                dampened_dpi = (
                    w_ike * ike_score  # IKE: always full (raw storm power)
                    + w_surge_rain * surge_rain_score * land_prox_factor
                    + w_economic * economic_score * land_prox_factor
                    + vuln_bonus * land_prox_factor
                    + dampened_compact
                    + coast_tracking_bonus  # Already requires coastal region
                    + stall_dpi_bonus * land_prox_factor
                    + ri_bonus
                )
                # Don't let dampening drop below 60% of undampened value
                # (storm is still physically powerful even over ocean)
                raw_dpi = max(dampened_dpi, raw_dpi * 0.60)
                logger.debug(
                    f"Land proximity dampening: factor={land_prox_factor:.2f}, "
                    f"dist={distance_to_coast:.0f}km, "
                    f"DPI before={raw_dpi:.1f}"
                )
            data_sources["land_proximity"] = "land_proximity_module"
        except Exception as e:
            logger.debug(f"Land proximity dampening failed: {e}")
            land_prox_factor = 1.0

    dpi_score = min(100.0, raw_dpi)
    dpi_category = categorize_dpi(dpi_score)

    return DPIResult(
        dpi_score=dpi_score,
        dpi_category=dpi_category,
        formula1_ike=ike_result,
        formula2_surge_rain=surge_rain,
        formula3_economic=economic,
        ike_score=ike_score,
        surge_rain_score=surge_rain_score,
        economic_score=economic_score,
        region_key=used_region,
        land_proximity_factor=land_prox_factor,
        distance_to_coast_km=distance_to_coast,
        population_threat=population_threat,
        data_sources=data_sources,
    )


def compute_dpi_simple(
    vmax_ms: float,
    min_pressure_hpa: Optional[float],
    lat: float,
    lon: float,
    r34_m: Optional[float] = None,
    rmw_m: Optional[float] = None,
    forward_speed_ms: Optional[float] = None,
    r34_quadrants_m: Optional[dict] = None,
    r50_quadrants_m: Optional[dict] = None,
    r64_quadrants_m: Optional[dict] = None,
    region_key: Optional[str] = None,
    storm_id: str = "UNKNOWN",
    name: str = "Unknown",
    storm_year: Optional[int] = None,
    approach_angle_deg: Optional[float] = None,
    track_parallel_factor: Optional[float] = None,
    # API-sourced parameters
    real_soil_moisture: Optional[float] = None,
    real_sst_c: Optional[float] = None,
    storm_approach_heading_deg: Optional[float] = None,
    apply_land_dampening: bool = True,
    use_nri: bool = False,
) -> DPIResult:
    """
    Convenience function to compute DPI from raw parameters (no snapshot needed).

    Useful for quick calculations, historical validation, and what-if scenarios.

    Parameters:
        real_soil_moisture: Live soil moisture (0-1) from Open-Meteo API
        real_sst_c: Live sea surface temperature (°C) from weather APIs
        storm_approach_heading_deg: Storm heading for terrain windward/leeward calc
        apply_land_dampening: Whether to apply open-ocean score reduction
        use_nri: If True, use FEMA NRI zone overrides for exposure/vulnerability
    """
    from datetime import datetime

    snapshot = HurricaneSnapshot(
        storm_id=storm_id,
        name=name,
        timestamp=datetime.utcnow(),
        lat=lat,
        lon=lon,
        max_wind_ms=vmax_ms,
        min_pressure_hpa=min_pressure_hpa,
        rmw_m=rmw_m,
        r34_m=r34_m,
        r34_quadrants_m=r34_quadrants_m,
        r50_quadrants_m=r50_quadrants_m,
        r64_quadrants_m=r64_quadrants_m,
        forward_speed_ms=forward_speed_ms,
    )

    return compute_dpi(
        snapshot, region_key=region_key, storm_year=storm_year,
        approach_angle_deg=approach_angle_deg,
        track_parallel_factor=track_parallel_factor,
        real_soil_moisture=real_soil_moisture,
        real_sst_c=real_sst_c,
        storm_approach_heading_deg=storm_approach_heading_deg,
        apply_land_dampening=apply_land_dampening,
        use_nri=use_nri,
    )
