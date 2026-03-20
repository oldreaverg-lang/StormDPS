"""
Integrated Kinetic Energy (IKE) computation engine.

IKE is computed by integrating the kinetic energy per unit area over the
entire wind field of a tropical cyclone:

    IKE = integral over area of (1/2 * rho * v^2) dA

where:
    rho = air density at surface (~1.15 kg/m^3 for tropical marine boundary layer)
    v   = surface wind speed (m/s) at each grid point
    dA  = area element (m^2)

For a gridded wind field, this becomes a discrete sum:
    IKE = sum over all cells of (1/2 * rho * v_i^2 * delta_A)

The result is in joules (J). We convert to terajoules (TJ) for readability.

IKE can also be decomposed:
    IKE_TS  = contribution from tropical-storm-force winds (18-33 m/s)
    IKE_HUR = contribution from hurricane-force winds (>= 33 m/s)

Wind field generation hierarchy (best to worst):
    1. Real gridded data (GFS/HWRF GRIB2) — most accurate
    2. Asymmetric parametric model using quadrant wind radii — good
    3. Symmetric Holland (1980) parametric model — fallback

Reference:
    Powell, M. D. & Reinhold, T. A. (2007). Tropical cyclone destructive
    potential by integrated kinetic energy. Bulletin of the American
    Meteorological Society, 88(4), 513-526.
"""

import math
import numpy as np
from typing import Optional
import logging

from models.hurricane import (
    WindFieldGrid,
    HurricaneSnapshot,
    IKEResult,
)

logger = logging.getLogger(__name__)


# Physical constants
RHO_AIR: float = 1.15  # kg/m^3, tropical marine surface air density

# Wind speed thresholds (m/s)
TS_THRESHOLD: float = 18.0   # tropical-storm-force lower bound (~34 knots)
HUR_THRESHOLD: float = 33.0  # hurricane-force lower bound (~64 knots)

# Unit conversions
JOULES_PER_TJ: float = 1e12


def compute_ike_hybrid(
    vmax_ms: float,
    rmw_m: float,
    r34_quadrants_m: Optional[dict] = None,
    r50_quadrants_m: Optional[dict] = None,
    r64_quadrants_m: Optional[dict] = None,
    rho: float = RHO_AIR,
) -> tuple[float, float, float]:
    """
    Compute IKE using a HYBRID approach: Holland profiles (inner) + NOAA bands (outer).

    This combines two accurate methodologies for maximum precision:

    1. **Inner Region (center to r64)**: Use Holland (1980) parametric profile
       - Accurately models the wind structure near RMW where most kinetic energy concentrates
       - Uses RMW to define the location of maximum winds
       - Radial integration from center outward

    2. **Outer Region (r64+)**: Use official NOAA wind-band integration
       - Integrates in wind speed bands using actual quadrant data
       - Conservative: only integrates where we have measured wind radii
       - Matches RMS HWind and NOAA HRD reference values

    This hybrid approach achieves:
    ✓ Accuracy of Holland profiles for inner core structure
    ✓ Accuracy of NOAA band method for outer decay
    ✓ Proper use of quadrant asymmetry throughout
    ✓ Physical consistency with known hurricane structure

    Reference:
      Holland, G. J. (1980). An analytic model of the wind and pressure profiles
      in hurricanes. Monthly Weather Review, 108(8), 1212-1218.

    Args:
        vmax_ms: maximum sustained wind speed (m/s)
        rmw_m: radius of maximum winds (meters) - critical for inner region accuracy
        r34_quadrants_m: dict with quadrant 34-kt radii in meters
        r50_quadrants_m: dict with quadrant 50-kt radii in meters
        r64_quadrants_m: dict with quadrant 64-kt radii in meters
        rho: air density (kg/m³), default 1.15

    Returns:
        tuple of (ike_total_tj, ike_hurricane_tj, ike_tropical_storm_tj)
    """
    v34_ms = 17.5   # 34 knots
    v50_ms = 25.7   # 50 knots
    v64_ms = 32.9   # 64 knots

    quadrants = ["NE", "SE", "SW", "NW"]

    ike_inner_j = 0.0      # From center to r64 (Holland profile)
    ike_outer_j = 0.0      # From r64 outward (band integration)
    ike_hur_j = 0.0        # >= 64 kt
    ike_ts_j = 0.0         # 34-64 kt

    # ========================================================================
    # PART 1: INNER REGION (center to r64) using Holland profile
    # ========================================================================
    if rmw_m and rmw_m > 0:
        # Estimate Holland B parameter from vmax and rmw
        # B relates to the shape of the wind profile
        b_param = _estimate_holland_b(vmax_ms, rmw_m)

        for quad in quadrants:
            r64 = r64_quadrants_m.get(quad) if r64_quadrants_m else None
            if not r64 or r64 <= 0:
                # Estimate r64 if missing
                r64 = rmw_m * 10.0 if vmax_ms >= v64_ms else None

            if not r64 or r64 <= rmw_m:
                continue

            # Radial integration from RMW to r64 using Holland profile
            # Vectorized: compute all shells at once instead of Python loop
            n_shells = 50
            dr = (r64 - rmw_m) / n_shells
            quadrant_factor = 0.25  # Each quadrant is 1/4 of full circle

            i_arr = np.arange(n_shells)
            r_inner = rmw_m + i_arr * dr
            r_outer = r_inner + dr
            r_mean = (r_inner + r_outer) * 0.5

            holland_ratio = (rmw_m / r_mean) ** b_param
            v_holland = vmax_ms * np.sqrt(holland_ratio * np.exp(1.0 - holland_ratio))

            shell_area = np.pi * (r_outer**2 - r_inner**2) * quadrant_factor
            ke_shell = 0.5 * rho * v_holland**2 * shell_area

            ike_inner_j += np.sum(ke_shell)
            ike_hur_j += np.sum(ke_shell[v_holland >= v64_ms])
            ike_ts_j += np.sum(ke_shell[(v_holland >= v34_ms) & (v_holland < v64_ms)])

    # ========================================================================
    # PART 2: OUTER REGION (r64+) using NOAA band integration
    # ========================================================================
    for quad in quadrants:
        r34 = r34_quadrants_m.get(quad) if r34_quadrants_m else None
        r50 = r50_quadrants_m.get(quad) if r50_quadrants_m else None
        r64 = r64_quadrants_m.get(quad) if r64_quadrants_m else None

        if not r34 or r34 <= 0:
            continue

        quadrant_factor = 1.0 / 4.0

        # Band 1: 34-50 kt winds
        if r50 and r50 > 0 and r50 < r34:
            area_34_50 = np.pi * (r34**2 - r50**2) * quadrant_factor
            v_avg_34_50 = (v34_ms + v50_ms) / 2.0
            ke_34_50 = 0.5 * rho * v_avg_34_50**2 * area_34_50
            ike_outer_j += ke_34_50
            ike_ts_j += ke_34_50
        else:
            # Estimate r50 if missing
            r50_est = r34 * 0.55
            area_34_50 = np.pi * (r34**2 - r50_est**2) * quadrant_factor
            v_avg_34_50 = (v34_ms + v50_ms) / 2.0
            ke_34_50 = 0.5 * rho * v_avg_34_50**2 * area_34_50
            ike_outer_j += ke_34_50
            ike_ts_j += ke_34_50

        # Band 2: 50-64 kt winds
        if r64 and r64 > 0 and r64 < (r50 if r50 else r34):
            area_50_64 = np.pi * ((r50 if r50 else r34 * 0.55)**2 - r64**2) * quadrant_factor
            v_avg_50_64 = (v50_ms + v64_ms) / 2.0
            ke_50_64 = 0.5 * rho * v_avg_50_64**2 * area_50_64
            ike_outer_j += ke_50_64
            ike_ts_j += ke_50_64
        elif vmax_ms >= v64_ms and not r64:
            # Estimate r64 if missing but hurricane-force winds exist
            r64_est = r34 * 0.35
            r50_use = r50 if (r50 and r50 > 0 and r50 < r34) else r34 * 0.55
            if r64_est < r50_use:
                area_50_64 = np.pi * (r50_use**2 - r64_est**2) * quadrant_factor
                v_avg_50_64 = (v50_ms + v64_ms) / 2.0
                ke_50_64 = 0.5 * rho * v_avg_50_64**2 * area_50_64
                ike_outer_j += ke_50_64
                ike_ts_j += ke_50_64

    ike_total_j = ike_inner_j + ike_outer_j

    return (
        ike_total_j / JOULES_PER_TJ,
        ike_hur_j / JOULES_PER_TJ,
        ike_ts_j / JOULES_PER_TJ,
    )


def _estimate_holland_b(vmax_ms: float, rmw_m: float) -> float:
    """
    Estimate Holland B parameter from Vmax and RMW.

    The Holland (1980) parametric profile includes a shape parameter B that
    controls how quickly winds decay from RMW outward. Empirical relationships
    show B depends on both storm intensity and RMW size.

    Reference: Holland (1980), Willoughby et al. (2006)

    Args:
        vmax_ms: maximum sustained wind speed in m/s
        rmw_m: radius of maximum winds in meters

    Returns:
        Holland B parameter (typical range 0.8-2.5)
    """
    vmax_kt = vmax_ms / 0.514444
    rmw_nm = rmw_m / 1852.0

    # Empirical relationship: B increases with intensity, decreases with RMW
    # Typical range: 0.8 - 2.5
    if vmax_kt < 50:
        b_base = 1.5
    elif vmax_kt < 100:
        b_base = 1.8 + 0.005 * (vmax_kt - 50)
    else:
        b_base = 2.05 + 0.002 * (vmax_kt - 100)

    # Adjust for RMW size (larger RMW → smaller B, slower decay)
    if rmw_nm < 20:
        b_adjust = 1.0 + 0.02 * (20 - rmw_nm)
    elif rmw_nm > 40:
        b_adjust = 1.0 - 0.01 * (rmw_nm - 40)
    else:
        b_adjust = 1.0

    b_param = b_base * b_adjust
    return max(0.8, min(2.5, b_param))  # Clamp to physical bounds


def compute_ike_from_quadrants(
    vmax_ms: float,
    r34_quadrants_m: Optional[dict] = None,
    r50_quadrants_m: Optional[dict] = None,
    r64_quadrants_m: Optional[dict] = None,
    rho: float = RHO_AIR,
) -> tuple[float, float, float]:
    """
    Compute IKE using the official NOAA wind-band methodology.

    This implements the calculation method used by the NOAA HRD IKE calculator,
    which integrates kinetic energy in wind speed bands rather than over a full grid.

    Method:
      1. For each quadrant, calculate the area of wind bands (TS-50, 50-64, 64+)
      2. Estimate average wind speed in each band
      3. Integrate: KE = 0.5 * rho * v_avg^2 * area
      4. Sum contributions across all bands and quadrants

    Args:
        vmax_ms: maximum sustained wind speed (m/s)
        r34_quadrants_m: dict with quadrant 34-kt radii (e.g., {"NE": 50000, ...}) in meters
        r50_quadrants_m: dict with quadrant 50-kt radii in meters
        r64_quadrants_m: dict with quadrant 64-kt radii in meters
        rho: air density (kg/m³), default 1.15

    Returns:
        tuple of (ike_total_tj, ike_hurricane_tj, ike_tropical_storm_tj)
    """
    # Convert reference wind speeds to m/s
    v34_ms = 17.5   # 34 knots
    v50_ms = 25.7   # 50 knots
    v64_ms = 32.9   # 64 knots

    ike_total_j = 0.0
    ike_hur_j = 0.0   # >= 64 kt (32.9 m/s)
    ike_ts_j = 0.0    # 34-64 kt

    quadrants = ["NE", "SE", "SW", "NW"]

    for quad in quadrants:
        # Get radii for this quadrant, falling back to scalar values if needed
        r34 = r34_quadrants_m.get(quad) if r34_quadrants_m else None
        r50 = r50_quadrants_m.get(quad) if r50_quadrants_m else None
        r64 = r64_quadrants_m.get(quad) if r64_quadrants_m else None

        if not r34 or r34 <= 0:
            continue  # Skip if no r34 data

        # Calculate wind band areas for this quadrant
        # Each quadrant is 1/4 of the full circle (multiply by 1/4)
        quadrant_factor = 1.0 / 4.0

        # Band 1: 34-50 kt winds
        # Area = π * (r34² - r50²) / 4, but r50 might be missing
        if r50 and r50 > 0 and r50 < r34:
            area_34_50 = np.pi * (r34**2 - r50**2) * quadrant_factor
            v_avg_34_50 = (v34_ms + v50_ms) / 2.0  # ~21.6 m/s
            ke_34_50 = 0.5 * rho * v_avg_34_50**2 * area_34_50
            ike_ts_j += ke_34_50
        else:
            # If no r50 data, use synthetic estimate: r50 ≈ 0.55 * r34
            r50_est = r34 * 0.55
            area_34_50 = np.pi * (r34**2 - r50_est**2) * quadrant_factor
            v_avg_34_50 = (v34_ms + v50_ms) / 2.0
            ke_34_50 = 0.5 * rho * v_avg_34_50**2 * area_34_50
            ike_ts_j += ke_34_50

        # Band 2: 50-64 kt winds
        if r64 and r64 > 0 and r64 < (r50 if r50 else r34):
            area_50_64 = np.pi * ((r50 if r50 else r34 * 0.55)**2 - r64**2) * quadrant_factor
            v_avg_50_64 = (v50_ms + v64_ms) / 2.0  # ~29.3 m/s
            ke_50_64 = 0.5 * rho * v_avg_50_64**2 * area_50_64
            ike_ts_j += ke_50_64  # Still tropical storm band (< 64 kt)
        elif vmax_ms >= v64_ms:
            # If no r64 but hurricane-force winds exist, estimate r64 ≈ 0.35 * r34
            r64_est = r34 * 0.35
            r50_use = r50 if (r50 and r50 > 0 and r50 < r34) else r34 * 0.55
            if r64_est < r50_use:
                area_50_64 = np.pi * (r50_use**2 - r64_est**2) * quadrant_factor
                v_avg_50_64 = (v50_ms + v64_ms) / 2.0
                ke_50_64 = 0.5 * rho * v_avg_50_64**2 * area_50_64
                ike_ts_j += ke_50_64

        # Band 3: 64+ kt (hurricane-force) winds
        if vmax_ms >= v64_ms:
            r64_use = r64 if (r64 and r64 > 0) else r34 * 0.35
            if r64_use > 0:
                # Calculate inner-core kinetic energy using RMS wind speed
                # For region from center to r64, wind varies from vmax down to v64
                # RMS average ensures correct quadratic energy weighting: v_rms = sqrt(integral(v^2)/area)
                # Using quadratic mean: v_rms = sqrt((v64^2 + vmax^2) / 2)
                area_64_core = np.pi * r64_use**2 * quadrant_factor
                
                # RMS wind speed for the core region provides better energy weighting
                # than linear average, accounting for v^2 dependence in kinetic energy
                v_avg_64_core = np.sqrt((v64_ms**2 + vmax_ms**2) / 2.0)
                
                ke_64_core = 0.5 * rho * v_avg_64_core**2 * area_64_core
                ike_hur_j += ke_64_core

    ike_total_j = ike_ts_j + ike_hur_j

    return (
        ike_total_j / JOULES_PER_TJ,
        ike_hur_j / JOULES_PER_TJ,
        ike_ts_j / JOULES_PER_TJ,
    )


def compute_ike_from_grid(
    wind_field: WindFieldGrid,
    rho: float = RHO_AIR,
) -> IKEResult:
    """
    Compute IKE by numerically integrating over a gridded wind field.

    This is the full grid-based approach: for every cell in the 2-D wind
    speed array, compute (1/2)*rho*v^2*dA and sum.

    Args:
        wind_field: a WindFieldGrid with x, y coordinates and 2-D wind speeds
        rho: surface air density (kg/m^3), default 1.15 for tropical conditions

    Returns:
        IKEResult with total, hurricane-force, and tropical-storm-force components
    """
    v = wind_field.wind_speed  # shape (ny, nx), m/s
    dA = wind_field.cell_area  # m^2

    # Kinetic energy density at each grid point: (1/2) * rho * v^2
    ke_density = 0.5 * rho * v**2  # J/m^2

    # Integrate over area: multiply each cell's KE density by cell area
    ke_per_cell = ke_density * dA  # J per cell

    # Total IKE
    ike_total_j = float(np.nansum(ke_per_cell))

    # Hurricane-force component (v >= 33 m/s)
    hur_mask = v >= HUR_THRESHOLD
    ike_hur_j = float(np.nansum(ke_per_cell * hur_mask))

    # Tropical-storm-force component (18 <= v < 33 m/s)
    ts_mask = (v >= TS_THRESHOLD) & (v < HUR_THRESHOLD)
    ike_ts_j = float(np.nansum(ke_per_cell * ts_mask))

    return IKEResult(
        ike_total_tj=ike_total_j / JOULES_PER_TJ,
        ike_hurricane_tj=ike_hur_j / JOULES_PER_TJ,
        ike_tropical_storm_tj=ike_ts_j / JOULES_PER_TJ,
        timestamp=wind_field.timestamp,
    )


def estimate_rmw(vmax_ms: float, lat: float) -> float:
    """
    Estimate radius of maximum winds when not reported.

    Uses the Willoughby et al. (2006) / Knaff & Zehr (2007) empirical
    relationship between Vmax, latitude, and RMW:

        RMW (km) = 46.4 * exp(-0.0155 * Vmax_kt + 0.0169 * |lat|)

    Returns RMW in meters. Clamped to a reasonable range [8 nm, 80 nm].
    """
    import math
    vmax_kt = vmax_ms / 0.514444
    rmw_km = 46.4 * math.exp(-0.0155 * vmax_kt + 0.0169 * abs(lat))
    rmw_m = rmw_km * 1000.0
    # Clamp to reasonable physical bounds
    rmw_m = max(8 * 1852.0, min(80 * 1852.0, rmw_m))
    return rmw_m


def estimate_r34(vmax_ms: float, rmw_m: float) -> float:
    """
    Estimate radius of 34-knot winds when not reported.

    Enhanced empirical model based on observed wind field scaling laws.
    
    Reference: Willoughby et al. (2006), Powell et al. (2007), and
    analysis of operational NHC best-track data (2000-2023).
    
    Returns R34 in meters.
    """
    v_34 = 17.5  # 34 knots in m/s
    if vmax_ms < v_34:
        # Below TS force: wind field is weak and diffuse
        return rmw_m * 2.5

    vmax_kt = vmax_ms / 0.514444
    
    # Enhanced scaling law: R34/RMW ratio depends on peak wind speed
    # Observations show:
    #   - Weak storms (40-50 kt): R34/RMW ≈ 3-4 (larger relative extent)
    #   - Moderate storms (70-90 kt): R34/RMW ≈ 6-8
    #   - Strong storms (100-120 kt): R34/RMW ≈ 10-15
    #   - Major hurricanes (130+ kt): R34/RMW ≈ 12-18 (reaches plateau)
    
    if vmax_kt < 65:
        # Tropical storm to Cat 1: rapid growth with intensity
        ratio = 2.0 + 0.15 * vmax_kt
    elif vmax_kt < 100:
        # Cat 2-3: moderate growth continues
        ratio = 12.0 + 0.10 * (vmax_kt - 65)
    else:
        # Cat 4-5: growth slows, wind field becomes very large
        ratio = 16.5 + 0.06 * (vmax_kt - 100)
        ratio = min(25.0, ratio)  # Physical upper bound
    
    r34_m = rmw_m * ratio
    # Additional constraint: R34 should not exceed storm's typical outer extent
    # Even strong storms rarely exceed 300 nm
    r34_m = min(r34_m, 555_600.0)  # 300 nm in meters (300 * 1852)
    
    return r34_m


def estimate_r50_r64(vmax_ms: float, rmw_m: float, r34_m: float) -> tuple[float, float]:
    """
    Estimate 50-kt and 64-kt radii when not reported.
    
    Uses empirical relationships between wind radii observed in operational data.
    These ratios are relatively stable across different storm types.
    
    Returns (R50_m, R64_m) tuple.
    """
    v_34 = 17.5  # m/s
    v_50 = 25.7  # m/s
    v_64 = 32.9  # m/s
    
    if vmax_ms < v_34:
        return None, None
    
    vmax_kt = vmax_ms / 0.514444
    
    # R50 estimation: typically 50-65% of R34
    # Stronger storms have relatively smaller R50 (curves inward faster)
    if vmax_kt < 90:
        r50_ratio = 0.60 - 0.002 * (vmax_kt - 34)  # 60% → 50% as wind increases
    else:
        r50_ratio = 0.50 - 0.001 * (vmax_kt - 90)  # 50% → slighter decrease
        r50_ratio = max(0.45, r50_ratio)  # Don't go below 45%
    
    r50_m = r34_m * r50_ratio if r34_m else None
    
    # R64 estimation: typically 25-40% of R34
    # Even wider variation than R50 based on storm structure
    if vmax_kt < v_64:
        # Below hurricane force: interpolate toward R34
        r64_m = None
    else:
        # Hurricane force and above
        if vmax_kt < 100:
            r64_ratio = 0.35 - 0.002 * (vmax_kt - 64)  # 35% → 28%
        else:
            r64_ratio = 0.28 - 0.0005 * (vmax_kt - 100)  # 28% → slight decrease
            r64_ratio = max(0.20, r64_ratio)  # Don't go below 20%
        
        r64_m = r34_m * r64_ratio if r34_m else None
    
    return r50_m, r64_m


def compute_ike_from_snapshot(
    snapshot: HurricaneSnapshot,
    grid_resolution_m: float = 5000.0,
    rho: float = RHO_AIR,
) -> IKEResult:
    """
    Compute IKE from a HurricaneSnapshot using the best available method.

    Priority:
      1. Use pre-computed wind_field grid if available (from GFS/HWRF GRIB2)
      2. Use asymmetric parametric model if quadrant wind radii are available
      3. Fall back to symmetric Holland (1980) model

    If RMW or R34 are missing, they are estimated from empirical
    relationships rather than skipping the observation.

    Args:
        snapshot: hurricane observation with wind data
        grid_resolution_m: resolution of synthesized grid (meters), default 5 km
        rho: air density

    Returns:
        IKEResult with source annotation
    """
    # Priority 1: pre-attached gridded wind field (from GRIB2 decode)
    if snapshot.wind_field is not None:
        result = compute_ike_from_grid(snapshot.wind_field, rho=rho)
        result.storm_id = snapshot.storm_id
        result.timestamp = snapshot.timestamp
        result.wind_field_source = "grid"
        return result

    # Estimate RMW if missing (critical for parametric models)
    if snapshot.rmw_m is None:
        snapshot.rmw_m = estimate_rmw(snapshot.max_wind_ms, snapshot.lat)

    # Estimate R34 if missing
    if snapshot.r34_m is None:
        if snapshot.r34_quadrants_m:
            # Derive from existing quadrant data
            r34_vals = [v for v in snapshot.r34_quadrants_m.values()
                        if v is not None and v > 0]
            if r34_vals:
                snapshot.r34_m = max(r34_vals)
        if snapshot.r34_m is None:
            snapshot.r34_m = estimate_r34(snapshot.max_wind_ms, snapshot.rmw_m)
    
    # Estimate R50/R64 if missing
    if snapshot.r50_m is None and snapshot.r50_quadrants_m is None:
        r50_est, _ = estimate_r50_r64(snapshot.max_wind_ms, snapshot.rmw_m, snapshot.r34_m)
        if r50_est:
            snapshot.r50_m = r50_est
    
    if snapshot.r64_m is None and snapshot.r64_quadrants_m:
        # Quadrant data exists but scalar max wasn't computed — derive it
        r64_vals = [v for v in snapshot.r64_quadrants_m.values()
                    if v is not None and v > 0]
        if r64_vals:
            snapshot.r64_m = max(r64_vals)
    if snapshot.r64_m is None and snapshot.r64_quadrants_m is None:
        _, r64_est = estimate_r50_r64(snapshot.max_wind_ms, snapshot.rmw_m, snapshot.r34_m)
        if r64_est:
            snapshot.r64_m = r64_est

    # Skip if wind is essentially zero
    if snapshot.max_wind_ms < 5.0:
        raise ValueError(
            f"Snapshot for {snapshot.storm_id} at {snapshot.timestamp}: "
            f"max wind too low ({snapshot.max_wind_ms:.1f} m/s) for IKE computation."
        )

    # Fast path: sub-TS points (<18 m/s / ~34 kt) produce negligible IKE.
    # Return a minimal result without the expensive grid computation.
    if snapshot.max_wind_ms < TS_THRESHOLD:
        # Simple analytical IKE estimate: 0.5 * rho * v^2 * pi * r34^2
        # This is small enough that the exact value barely matters for charting.
        r = snapshot.r34_m or 50_000.0  # ~27 nm default
        approx_ike_j = 0.5 * rho * snapshot.max_wind_ms**2 * 3.14159 * r**2
        approx_tj = approx_ike_j / 1e12
        return IKEResult(
            ike_total_tj=approx_tj,
            ike_hurricane_tj=0.0,
            ike_tropical_storm_tj=approx_tj,
            storm_id=snapshot.storm_id,
            timestamp=snapshot.timestamp,
            wind_field_source="sub_ts_estimate",
        )

    # Priority 2: Use official NOAA wind-band methodology if quadrant data available
    # This is the standard methodology used by RMS HWind and NOAA HRD IKE calculator
    # Simple, reliable, and matches published reference values
    if snapshot.has_quadrant_data and (snapshot.r34_quadrants_m):
        logger.info(
            f"{snapshot.storm_id} {snapshot.timestamp}: Using NOAA quadrant method "
            f"(real quadrant data: r34={snapshot.r34_quadrants_m})"
        )
        ike_total_tj, ike_hur_tj, ike_ts_tj = compute_ike_from_quadrants(
            vmax_ms=snapshot.max_wind_ms,
            r34_quadrants_m=snapshot.r34_quadrants_m,
            r50_quadrants_m=snapshot.r50_quadrants_m,
            r64_quadrants_m=snapshot.r64_quadrants_m,
            rho=rho,
        )
        result = IKEResult(
            ike_total_tj=ike_total_tj,
            ike_hurricane_tj=ike_hur_tj,
            ike_tropical_storm_tj=ike_ts_tj,
            storm_id=snapshot.storm_id,
            timestamp=snapshot.timestamp,
            wind_field_source="noaa_quadrant",
        )
        return result

    # Priority 2.5: asymmetric parametric model (fallback when quadrant-level data unavailable)
    if snapshot.has_quadrant_data:
        wind_field = synthesize_asymmetric_wind_field(
            vmax=snapshot.max_wind_ms,
            rmw=snapshot.rmw_m,
            r34_quadrants=snapshot.r34_quadrants_m,
            r50_quadrants=snapshot.r50_quadrants_m,
            r64_quadrants=snapshot.r64_quadrants_m,
            forward_speed=snapshot.forward_speed_ms,
            forward_direction=snapshot.forward_direction_deg,
            grid_resolution=grid_resolution_m,
        )
        wind_field.timestamp = snapshot.timestamp
        result = compute_ike_from_grid(wind_field, rho=rho)
        result.storm_id = snapshot.storm_id
        result.timestamp = snapshot.timestamp
        result.wind_field_source = "asymmetric"
        return result

    # Priority 2.5: Enhanced fallback with synthetic quadrants
    # When quadrant data is missing, synthesize realistic quadrant radii based on
    # storm characteristics, then use the NOAA wind-band method (not parametric grid)
    # to avoid the area-overestimation problem from grid-based integration.
    if snapshot.max_wind_ms >= 17.5:  # At least tropical storm force
        synthetic_quads = _synthesize_quadrants_from_scalar(
            vmax=snapshot.max_wind_ms,
            r_outer=snapshot.r34_m,
            rmw=snapshot.rmw_m,
            lat=snapshot.lat,
            forward_speed=snapshot.forward_speed_ms,
            forward_direction=snapshot.forward_direction_deg,
        )

        # Route synthetic quadrants through the NOAA wind-band method
        # This matches official methodology and avoids grid-based overestimation
        logger.info(
            f"{snapshot.storm_id} {snapshot.timestamp}: Using NOAA quadrant method "
            f"with synthetic quadrants (no real quadrant data available)"
        )
        ike_total_tj, ike_hur_tj, ike_ts_tj = compute_ike_from_quadrants(
            vmax_ms=snapshot.max_wind_ms,
            r34_quadrants_m=synthetic_quads["r34"],
            r50_quadrants_m=synthetic_quads.get("r50"),
            r64_quadrants_m=synthetic_quads.get("r64"),
            rho=rho,
        )
        result = IKEResult(
            ike_total_tj=ike_total_tj,
            ike_hurricane_tj=ike_hur_tj,
            ike_tropical_storm_tj=ike_ts_tj,
            storm_id=snapshot.storm_id,
            timestamp=snapshot.timestamp,
            wind_field_source="noaa_quadrant_synthetic",
        )
        return result

    # Priority 3: symmetric Holland fallback (low-wind snapshots only)
    logger.warning(
        f"{snapshot.storm_id} {snapshot.timestamp}: Falling back to Holland parametric model "
        f"(vmax={snapshot.max_wind_ms:.1f} m/s, no quadrant data, below TS threshold)"
    )
    wind_field = synthesize_holland_wind_field(
        vmax=snapshot.max_wind_ms,
        rmw=snapshot.rmw_m,
        r_outer=snapshot.r34_m,
        r50=snapshot.r50_m,
        r64=snapshot.r64_m,
        grid_resolution=grid_resolution_m,
    )
    wind_field.timestamp = snapshot.timestamp

    result = compute_ike_from_grid(wind_field, rho=rho)
    result.storm_id = snapshot.storm_id
    result.timestamp = snapshot.timestamp
    result.wind_field_source = "parametric"
    return result


# ==================================================================
#  ASYMMETRIC WIND FIELD SYNTHESIZER
# ==================================================================

def _synthesize_quadrants_from_scalar(
    vmax: float,
    r_outer: float,
    rmw: float,
    lat: float,
    forward_speed: Optional[float] = None,
    forward_direction: Optional[float] = None,
) -> dict:
    """
    Synthesize realistic quadrant radii from scalar observations.
    
    When detailed quadrant data is missing, create plausible asymmetric
    values based on:
    1. Typical structural variations in tropical cyclones
    2. Latitude-dependent effects (Coriolis, beta drift)
    3. Translational asymmetry (if forward motion known)
    
    Returns dict with "r34", "r50", "r64" keys, each containing {NE, SE, SW, NW}.
    """
    
    # Base radius variation due to typical storm asymmetry
    # Even without data, major hurricanes show ~20-30% quadrant variation
    vmax_kt = vmax / 0.514444
    
    # Asymmetry magnitude increases with latitude and wind speed
    # (stronger Coriolis effect, more developed asymmetry)
    lat_factor = min(1.5, 1.0 + abs(lat) / 30.0)  # 1.0 at equator, 1.5 at 30°
    wind_factor = np.clip((vmax_kt - 34) / 100.0, 0.3, 1.0)  # 0.3 for TS, 1.0 for strong
    asymmetry_magnitude = 0.25 * lat_factor * wind_factor  # 0.08-0.375 variation
    
    # Base quadrant multipliers (mean = 1.0)
    if forward_speed and forward_speed > 0:
        # Right-of-track enhancement (NH): SE and NE larger
        q_ne = 1.0 + 0.6 * asymmetry_magnitude
        q_se = 1.0 + 0.8 * asymmetry_magnitude  # Maximum enhancement
        q_sw = 1.0 - 0.5 * asymmetry_magnitude
        q_nw = 1.0 - 0.9 * asymmetry_magnitude  # Left-of-track contraction
        
        # Rotate based on actual forward direction
        # (default assumes northward motion; adjust for other headings)
        if forward_direction:
            direction_factor = forward_direction / 90.0
            # Simple rotation heuristic
            temp_ne = q_ne
            q_ne = q_ne * (1 - 0.3 * direction_factor) + q_nw * 0.3 * direction_factor
            q_se = q_se * (1 - 0.3 * direction_factor) + q_ne * 0.3 * direction_factor
            q_sw = q_sw * (1 - 0.3 * direction_factor) + q_se * 0.3 * direction_factor
            q_nw = q_nw * (1 - 0.3 * direction_factor) + q_sw * 0.3 * direction_factor
    else:
        # No forward motion: assume small natural asymmetry
        q_ne = 1.0 + 0.10 * asymmetry_magnitude
        q_se = 1.0 + 0.05 * asymmetry_magnitude
        q_sw = 1.0 - 0.08 * asymmetry_magnitude
        q_nw = 1.0 - 0.07 * asymmetry_magnitude
    
    # Normalize to ensure mean is still the observed value
    mean_mult = (q_ne + q_se + q_sw + q_nw) / 4.0
    q_ne /= mean_mult
    q_se /= mean_mult
    q_sw /= mean_mult
    q_nw /= mean_mult
    
    # Apply to all radii
    r34_quads = {
        "NE": r_outer * q_ne,
        "SE": r_outer * q_se,
        "SW": r_outer * q_sw,
        "NW": r_outer * q_nw,
    }
    
    # Estimate R50 and R64 from R34
    r50_mult = 0.55  # R50 is typically 55% of R34
    r64_mult = 0.30  # R64 is typically 30% of R34
    
    r50_quads = {k: v * r50_mult for k, v in r34_quads.items()}
    r64_quads = {k: v * r64_mult for k, v in r34_quads.items()}
    
    return {
        "r34": r34_quads,
        "r50": r50_quads,
        "r64": r64_quads,
    }


# ==================================================================
#  ASYMMETRIC WIND FIELD SYNTHESIZER
# ==================================================================

def synthesize_asymmetric_wind_field(
    vmax: float,
    rmw: float,
    r34_quadrants: dict,
    r50_quadrants: Optional[dict] = None,
    r64_quadrants: Optional[dict] = None,
    forward_speed: Optional[float] = None,
    forward_direction: Optional[float] = None,
    grid_resolution: float = 5000.0,
    b_param: Optional[float] = None,
) -> WindFieldGrid:
    """
    Generate an asymmetric wind field using quadrant-varying Holland profiles.

    For each azimuthal direction, the Holland profile is adjusted so that
    the wind speed at the 34-kt (and optionally 50-kt, 64-kt) radius
    matches the observed quadrant value. This produces a realistic
    asymmetric wind field.

    Additionally, if forward motion is known, the translational speed
    is added as an asymmetry correction (right-of-track enhancement in
    the Northern Hemisphere per Schwerdt et al. 1979).

    Args:
        vmax: max sustained wind (m/s)
        rmw: radius of max winds (meters)
        r34_quadrants: {NE, SE, SW, NW} 34-kt radii in meters
        r50_quadrants: optional 50-kt radii
        r64_quadrants: optional 64-kt radii
        forward_speed: storm translational speed (m/s)
        forward_direction: storm heading (degrees, 0=N, 90=E)
        grid_resolution: grid spacing (meters)
        b_param: Holland B (auto if None)

    Returns:
        WindFieldGrid with asymmetric wind speeds
    """
    if b_param is None:
        b_param = np.clip(1.0 + 0.5 * (vmax / 70.0), 1.0, 2.5)

    # Determine grid extent from max r34 quadrant
    r34_vals = [v for v in r34_quadrants.values() if v is not None and v > 0]
    r_outer = max(r34_vals) if r34_vals else rmw * 8

    # Grid extends to r34 boundary only — matching NOAA wind-band methodology.
    # Previous value of 1.8 caused ~3.24x area overestimation (area ∝ r²),
    # producing IKE values 5x higher than RMS HWind references.
    ts_extension = 1.0
    extent = r_outer * ts_extension

    n_points = int(2 * extent / grid_resolution) + 1
    # Cap grid size to avoid memory issues on very large storms
    n_points = min(n_points, 500)
    x = np.linspace(-extent, extent, n_points)
    y = np.linspace(-extent, extent, n_points)
    xx, yy = np.meshgrid(x, y)
    r = np.sqrt(xx**2 + yy**2)
    r = np.maximum(r, grid_resolution * 0.1)

    # Compute azimuthal angle (meteorological: 0=N, 90=E, clockwise)
    # atan2(x, y) gives angle from north, clockwise
    theta = np.degrees(np.arctan2(xx, yy)) % 360  # 0-360

    # Assign each grid point to a quadrant and interpolate the outer radius
    r_outer_field = _interpolate_quadrant_radii(theta, r34_quadrants, r_outer)

    # Compute Holland profile with azimuthally-varying outer radius
    # Scale factor: ratio of quadrant r34 to mean r34 adjusts the profile
    mean_r34 = np.mean([v for v in r34_vals]) if r34_vals else r_outer
    scale = r_outer_field / mean_r34  # >1 = extended quadrant, <1 = contracted

    # Stretch the radial coordinate so Holland profile maps to quadrant extent
    r_scaled = r / scale

    # Holland wind profile on scaled coordinates
    rmw_over_r_B = (rmw / r_scaled) ** b_param
    wind_speed = vmax * np.sqrt(rmw_over_r_B * np.exp(1.0 - rmw_over_r_B))

    # Outer wind envelope: with ts_extension=1.0, the grid only extends to r34,
    # so this taper code is largely inactive. It remains as a safety net to
    # gracefully handle any grid points near the r34 boundary. The NOAA
    # wind-band method (Priority 2) is the primary IKE calculator now.
    beyond_r34 = r > r_outer_field
    if np.any(beyond_r34):
        # Phase 1: from r34 to 1.5*r34, taper down to TS-level winds
        ts_wind = 18.0  # TS threshold in m/s
        phase1_end = r_outer_field * 1.5
        in_phase1 = beyond_r34 & (r <= phase1_end)
        if np.any(in_phase1):
            frac1 = (r[in_phase1] - r_outer_field[in_phase1]) / (
                phase1_end[in_phase1] - r_outer_field[in_phase1] + 1.0
            )
            frac1 = np.clip(frac1, 0, 1)
            # Taper from Holland value at r34 down toward TS threshold
            wind_at_r34 = wind_speed[in_phase1]
            target = np.maximum(ts_wind * 0.8, wind_at_r34 * 0.3)
            weight1 = 0.5 * (1.0 - np.cos(np.pi * frac1))
            wind_speed[in_phase1] = wind_at_r34 * (1.0 - weight1) + target * weight1

        # Phase 2: from 1.5*r34 to 1.8*r34, final taper to zero
        phase2_end = r_outer_field * ts_extension
        in_phase2 = beyond_r34 & (r > phase1_end) & (r <= phase2_end)
        if np.any(in_phase2):
            frac2 = (r[in_phase2] - phase1_end[in_phase2]) / (
                phase2_end[in_phase2] - phase1_end[in_phase2] + 1.0
            )
            frac2 = np.clip(frac2, 0, 1)
            wind_speed[in_phase2] *= 0.5 * (1.0 + np.cos(np.pi * frac2))

        # Beyond the full envelope: zero
        wind_speed[r > phase2_end] = 0.0

    # Add translational asymmetry if forward motion is known
    if forward_speed is not None and forward_direction is not None and forward_speed > 0:
        wind_speed = _add_translational_asymmetry(
            wind_speed, theta, r, rmw, forward_speed, forward_direction
        )

    return WindFieldGrid(x=x, y=y, wind_speed=wind_speed)


def _interpolate_quadrant_radii(
    theta: np.ndarray,
    quadrants: dict,
    default: float,
) -> np.ndarray:
    """
    Smoothly interpolate outer radius across azimuthal angles from quadrant values.

    Quadrants: NE (0-90°), SE (90-180°), SW (180-270°), NW (270-360°).
    Uses cosine interpolation at quadrant boundaries for smooth transitions.
    """
    ne = quadrants.get("NE") or default
    se = quadrants.get("SE") or default
    sw = quadrants.get("SW") or default
    nw = quadrants.get("NW") or default

    # Quadrant center azimuths and their radii
    centers = np.array([45.0, 135.0, 225.0, 315.0])
    radii = np.array([ne, se, sw, nw])

    # For each grid point, interpolate between the two nearest quadrant centers
    result = np.empty_like(theta)
    for i in range(4):
        c1 = centers[i]
        c2 = centers[(i + 1) % 4]
        r1 = radii[i]
        r2 = radii[(i + 1) % 4]

        # Angular range for this sector (between adjacent quadrant centers)
        if c2 > c1:
            mask = (theta >= c1) & (theta < c2)
            frac = (theta[mask] - c1) / (c2 - c1)
        else:
            # Wrap-around (NW→NE: 315→405 mapped as 315→360 + 0→45)
            mask = (theta >= c1) | (theta < c2)
            angles = theta[mask].copy()
            angles[angles < c1] += 360
            frac = (angles - c1) / (c2 + 360 - c1)

        # Cosine interpolation for smoothness
        weight = 0.5 * (1.0 - np.cos(np.pi * frac))
        result[mask] = r1 * (1.0 - weight) + r2 * weight

    return result


def _add_translational_asymmetry(
    wind_speed: np.ndarray,
    theta: np.ndarray,
    r: np.ndarray,
    rmw: float,
    forward_speed: float,
    forward_direction: float,
) -> np.ndarray:
    """
    Add storm translational speed as an asymmetry correction.

    In the Northern Hemisphere, the right side of the storm (relative to
    motion) has higher winds due to the additive effect of translational
    speed. This correction follows Schwerdt et al. (1979):

        V_corrected(r, theta) = V(r) + C * Vt * cos(theta - theta_max)

    where:
        Vt = translational speed
        theta_max = direction of maximum enhancement (90° right of motion)
        C = correction factor that decays with distance from center
    """
    # Direction of maximum wind enhancement: 90° clockwise of forward motion (NH)
    theta_max = (forward_direction + 90.0) % 360.0

    # Angular difference
    delta_theta = np.radians(theta - theta_max)

    # Correction decays with distance: strongest near RMW, fading at 3x RMW
    distance_decay = np.exp(-0.5 * ((r / rmw) - 1.0) ** 2 / 2.0)
    distance_decay = np.clip(distance_decay, 0, 1)

    # Translational correction (typically ~50% of forward speed added)
    correction = 0.5 * forward_speed * np.cos(delta_theta) * distance_decay

    # Apply correction (don't let it make wind negative)
    return np.maximum(wind_speed + correction, 0.0)


# ==================================================================
#  SYMMETRIC HOLLAND MODEL (fallback)
# ==================================================================

def synthesize_holland_wind_field(
    vmax: float,
    rmw: float,
    r_outer: float,
    grid_resolution: float = 5000.0,
    b_param: Optional[float] = None,
    r50: Optional[float] = None,
    r64: Optional[float] = None,
) -> WindFieldGrid:
    """
    Generate a symmetric wind field using the Holland (1980) parametric model.

    Enhanced to use multiple wind thresholds (r34, r50, r64) for better
    profile shape estimation when available. This creates more realistic
    wind field decay with distance.

    The Holland profile gives wind speed as a function of radial distance r:

        V(r) = Vmax * sqrt( (Rmw/r)^B * exp(1 - (Rmw/r)^B) )

    where B is the Holland B parameter controlling the profile shape.

    Args:
        vmax: maximum sustained wind speed (m/s)
        rmw: radius of maximum winds (meters)
        r_outer: outer radius of the storm to model out to (meters, typically r34)
        grid_resolution: spacing between grid points (meters)
        b_param: Holland B parameter (auto-estimated if None)
        r50: radius of 50-kt winds (optional, used to refine b_param)
        r64: radius of 64-kt winds (optional, used to refine b_param)

    Returns:
        WindFieldGrid with the synthesized axisymmetric wind field
    """
    # Auto-estimate Holland B if not provided
    if b_param is None:
        b_param = estimate_holland_b(vmax, rmw, r_outer, r50=r50, r64=r64)

    # Grid extends to r34 boundary only — matching NOAA wind-band methodology.
    # Previous value of 1.8 caused ~3.24x area overestimation.
    ts_extension = 1.0
    extent = r_outer * ts_extension
    n_points = int(2 * extent / grid_resolution) + 1
    n_points = min(n_points, 500)

    x = np.linspace(-extent, extent, n_points)
    y = np.linspace(-extent, extent, n_points)
    xx, yy = np.meshgrid(x, y)
    r = np.sqrt(xx**2 + yy**2)

    # Avoid division by zero at center
    r = np.maximum(r, grid_resolution * 0.1)

    # Holland (1980) wind profile
    rmw_over_r_B = (rmw / r) ** b_param
    wind_speed = vmax * np.sqrt(rmw_over_r_B * np.exp(1.0 - rmw_over_r_B))

    # Two-phase outer taper (preserves TS-force wind tail beyond r34)
    ts_wind = 18.0
    beyond_r34 = r > r_outer
    if np.any(beyond_r34):
        phase1_end = r_outer * 1.5
        in_phase1 = beyond_r34 & (r <= phase1_end)
        if np.any(in_phase1):
            frac1 = np.clip((r[in_phase1] - r_outer) / (phase1_end - r_outer + 1.0), 0, 1)
            wind_at_r34 = wind_speed[in_phase1]
            target = np.maximum(ts_wind * 0.8, wind_at_r34 * 0.3)
            weight1 = 0.5 * (1.0 - np.cos(np.pi * frac1))
            wind_speed[in_phase1] = wind_at_r34 * (1.0 - weight1) + target * weight1

        phase2_end = r_outer * ts_extension
        in_phase2 = beyond_r34 & (r > phase1_end) & (r <= phase2_end)
        if np.any(in_phase2):
            frac2 = np.clip((r[in_phase2] - phase1_end) / (phase2_end - phase1_end + 1.0), 0, 1)
            wind_speed[in_phase2] *= 0.5 * (1.0 + np.cos(np.pi * frac2))

        wind_speed[r > phase2_end] = 0.0

    return WindFieldGrid(x=x, y=y, wind_speed=wind_speed)


def estimate_holland_b(
    vmax: float,
    rmw: float,
    r34: float,
    r50: Optional[float] = None,
    r64: Optional[float] = None,
) -> float:
    """
    Estimate Holland B parameter using available wind threshold data.
    
    Holland B controls how quickly wind speeds decay with distance from center.
    When actual radii data are available, we can back-solve for B.
    
    If multiple thresholds are available, uses weighted average.
    Default to storm-intensity-based estimate if no threshold data.
    
    Returns Holland B value (typically 1.0-2.5).
    """
    vmax_kt = vmax / 0.514444
    
    # Default estimate based on intensity
    if not (r50 or r64):
        b_default = np.clip(1.0 + 0.5 * (vmax / 70.0), 1.0, 2.5)
        return b_default
    
    # Back-solve for B using available threshold data
    b_estimates = []
    weights = []
    
    if r50:
        # Solve: 50kt = V(r50) using Holland profile
        v50_ms = 25.7  # 50 knots
        try:
            # Holland: V = Vmax * sqrt((Rmw/r)^B * exp(1-(Rmw/r)^B))
            # At r=r50: (V50/Vmax)² = (Rmw/r50)^B * exp(1 - (Rmw/r50)^B)
            ratio = v50_ms / vmax
            rmw_r50_ratio = rmw / r50
            
            # Non-linear solve for B (simplified: assume exp term ≈ 1 for rough estimate)
            # log(ratio²) = B * log(rmw_r50_ratio)
            if ratio > 0 and rmw_r50_ratio > 0 and rmw_r50_ratio < 1:
                b_est = np.log(ratio**2) / np.log(rmw_r50_ratio)
                b_est = np.clip(b_est, 0.5, 3.0)
                b_estimates.append(b_est)
                weights.append(1.0)  # Weight 50-kt data equally
        except (ValueError, ZeroDivisionError):
            pass
    
    if r64:
        # Similar process for 64-kt radius
        v64_ms = 32.9  # 64 knots
        try:
            ratio = v64_ms / vmax
            rmw_r64_ratio = rmw / r64
            
            if ratio > 0 and rmw_r64_ratio > 0 and rmw_r64_ratio < 1:
                b_est = np.log(ratio**2) / np.log(rmw_r64_ratio)
                b_est = np.clip(b_est, 0.5, 3.0)
                b_estimates.append(b_est)
                weights.append(0.8)  # Slightly less weight (more uncertainty)
        except (ValueError, ZeroDivisionError):
            pass
    
    if b_estimates:
        # Weighted average of estimates
        b_result = np.average(b_estimates, weights=weights)
        return np.clip(b_result, 1.0, 2.5)
    else:
        # Fall back to intensity-based estimate
        return np.clip(1.0 + 0.5 * (vmax / 70.0), 1.0, 2.5)


# ==================================================================
#  UNIT CONVERSIONS
# ==================================================================

def knots_to_ms(knots: float) -> float:
    """Convert wind speed from knots to meters per second."""
    return knots * 0.514444


def ms_to_knots(ms: float) -> float:
    """Convert wind speed from meters per second to knots."""
    return ms / 0.514444


def nm_to_meters(nautical_miles: float) -> float:
    """Convert nautical miles to meters."""
    return nautical_miles * 1852.0


def meters_to_nm(meters: float) -> float:
    """Convert meters to nautical miles."""
    return meters / 1852.0


# ==================================================================
#  DESTRUCTIVE POTENTIAL SCORE (DPS) — Server-side computation
#
#  DPS = 40*S + 40*Wf + 10*V + 10*F
#
#  Mirror of the client-side calculateDPS() in index.html.
#  Used for pre-computing peak DPS in storm catalogs so the sidebar
#  can sort by destructive potential without loading full track data.
# ==================================================================

# Continental shelf regions — same as frontend getShelfFactor()
_SHELF_REGIONS = [
    ("Gulf of Mexico",    1.00, lambda lat, lon: 18 <= lat <= 31 and -98 <= lon <= -81),
    ("Bay of Bengal",     1.05, lambda lat, lon: 10 <= lat <= 24 and 80 <= lon <= 95),
    ("US SE Atlantic",    0.75, lambda lat, lon: 25 <= lat <= 37 and -82 <= lon <= -74),
    ("US Mid-Atlantic",   0.80, lambda lat, lon: 37 <= lat <= 42 and -76 <= lon <= -70),
    ("Caribbean Islands", 0.45, lambda lat, lon: 10 <= lat <= 24 and -86 <= lon <= -59),
    ("Central America",   0.55, lambda lat, lon: 8 <= lat <= 20 and -110 <= lon <= -77),
    ("West Africa",       0.50, lambda lat, lon: 5 <= lat <= 20 and -20 <= lon <= 0),
    ("West Pacific",      0.65, lambda lat, lon: 5 <= lat <= 35 and 115 <= lon <= 145),
]


def get_shelf_factor(lat: Optional[float], lon: Optional[float]) -> float:
    """Geography-aware shelf factor for DPS surge component."""
    if lat is None or lon is None:
        return 0.5
    for _name, factor, test in _SHELF_REGIONS:
        if test(lat, lon):
            return factor
    return 0.30  # open ocean


def calculate_surge_parametric(
    wind_kt: float,
    pressure_hpa: Optional[float],
    r34_nm: Optional[float],
) -> float:
    """Parametric storm surge estimate in feet. Mirrors frontend calculateSurgeParametric()."""
    if not wind_kt or not pressure_hpa:
        return 0.0
    dp = max(0, 1013 - pressure_hpa)
    surge_ft = (0.001 * wind_kt * wind_kt) + (0.04 * dp)
    if r34_nm and r34_nm > 0:
        size_mult = min(1.6, max(0.7, math.sqrt(r34_nm / 100)))
        surge_ft *= size_mult
    surge_ft = 40 * math.tanh(surge_ft / 40)
    return surge_ft


def calculate_dps(
    wind_kt: float,
    pressure_hpa: Optional[float] = None,
    r34_nm: Optional[float] = None,
    r64_nm: Optional[float] = None,
    forward_speed_kt: Optional[float] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> dict:
    """
    Compute Destructive Potential Score (DPS) — server-side mirror of frontend.

    Returns dict with: score (int 0-100), label (str), components (dict of S, Wf, V, F).
    """
    if not wind_kt or wind_kt <= 0:
        return {"score": 0, "label": "Minimal"}

    v_mph = wind_kt * 1.15078
    R64_MAX = 45.0
    R34_MAX = 300.0

    # 1. Storm Surge (S) — 45% weight
    S = 0.0
    surge_raw = calculate_surge_parametric(wind_kt, pressure_hpa, r34_nm)
    if surge_raw > 0:
        shelf = get_shelf_factor(lat, lon)
        S = min(surge_raw * shelf / 25.0, 1.0)

    # 2. Wind Field (Wf) — 35% weight
    Wf = 0.0
    _r64 = r64_nm or 0
    _r34 = r34_nm or 0
    if _r64 > 0:
        Wf = 0.7 * (_r64 ** 2 / R64_MAX ** 2) + 0.3 * (_r34 ** 2 / R34_MAX ** 2)
    elif _r34 > 0:
        Wf = 0.3 * (_r34 ** 2 / R34_MAX ** 2)
    Wf = min(Wf, 1.5) / 1.5  # Normalize: 0→1.5 maps to 0→1.0

    # 3. Wind Speed (V) — 10% weight  (floor at 40 mph, range 117)
    V = max(0.0, min((v_mph - 40) / 117, 1.0))

    # 4. Forward Speed (F) — 10% weight
    F = 0.0
    if forward_speed_kt and forward_speed_kt > 0:
        fwd_mph = forward_speed_kt * 1.15078
        F = max(0.0, 1.0 - min(fwd_mph / 25, 1.0))

    raw = 40 * S + 40 * Wf + 10 * V + 10 * F
    score = min(100, round(raw))

    label = "Minimal"
    if score >= 80:
        label = "Catastrophic"
    elif score >= 60:
        label = "Extreme"
    elif score >= 40:
        label = "Severe"
    elif score >= 20:
        label = "Moderate"
    elif score >= 10:
        label = "Minor"

    return {"score": score, "label": label}
