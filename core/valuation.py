"""
Hurricane valuation engine.

Assigns a composite "destructive value" to a hurricane based on:
  1. IKE (Integrated Kinetic Energy) — primary metric
  2. Storm surge threat estimate
  3. Rapid intensification factor
  4. Size factor (large storms carry more energy even at lower max wind)

The result is a normalized 0-100 score with sub-components.
"""

import numpy as np
from typing import Optional

from models.hurricane import (
    HurricaneSnapshot,
    IKEResult,
    HurricaneValuation,
)
from core.ike import compute_ike_from_snapshot


# Scaling constants derived from historical data
# Hurricane Sandy (2012) had IKE ~ 300 TJ, Katrina (2005) ~ 250 TJ at peak
# We use 500 TJ as the "reference maximum" for normalization
IKE_REFERENCE_MAX_TJ: float = 500.0

# Surge threat coefficients
# Surge ~ f(pressure deficit, forward speed, continental shelf width)
# Simplified: surge_index proportional to (1013 - Pc) / Vfwd
AMBIENT_PRESSURE_HPA: float = 1013.25


def compute_valuation(
    snapshot: HurricaneSnapshot,
    previous_snapshot: Optional[HurricaneSnapshot] = None,
    grid_resolution_m: float = 5000.0,
) -> HurricaneValuation:
    """
    Compute a composite hurricane destructive value.

    Args:
        snapshot: current storm observation
        previous_snapshot: prior observation (6h earlier) for intensification rate
        grid_resolution_m: resolution for IKE grid computation

    Returns:
        HurricaneValuation with all sub-scores
    """
    # 1. Compute IKE
    ike_result = compute_ike_from_snapshot(snapshot, grid_resolution_m=grid_resolution_m)

    # 2. Destructive potential from IKE (0-100 scale)
    # Uses a saturating function so extreme storms don't blow out the scale
    dp = 100.0 * (1.0 - np.exp(-ike_result.ike_total_tj / IKE_REFERENCE_MAX_TJ))
    destructive_potential = float(np.clip(dp, 0, 100))

    # 3. Storm surge threat estimate (0-100 scale)
    surge_threat = None
    if snapshot.min_pressure_hpa is not None:
        pressure_deficit = AMBIENT_PRESSURE_HPA - snapshot.min_pressure_hpa
        # Normalize: Cat 5 might have deficit ~110 hPa
        surge_threat = float(np.clip(pressure_deficit / 1.1, 0, 100))

    # 4. Rapid intensification bonus
    ri_factor = 0.0
    if previous_snapshot is not None:
        wind_change = snapshot.max_wind_ms - previous_snapshot.max_wind_ms
        hours = (snapshot.timestamp - previous_snapshot.timestamp).total_seconds() / 3600
        if hours > 0:
            rate_ms_per_hour = wind_change / hours
            # RI threshold is ~15 m/s in 24h = 0.625 m/s/hr
            if rate_ms_per_hour > 0.625:
                ri_factor = min(rate_ms_per_hour / 1.25, 1.0) * 15.0  # up to 15 bonus

    # 5. Composite overall value
    # Weighted combination: 60% IKE-based DP, 25% surge, 15% RI
    overall = destructive_potential * 0.60
    if surge_threat is not None:
        overall += surge_threat * 0.25
    else:
        # Redistribute surge weight to DP when surge data unavailable
        overall += destructive_potential * 0.25
    overall += ri_factor
    overall = float(np.clip(overall, 0, 100))

    return HurricaneValuation(
        storm_id=snapshot.storm_id,
        name=snapshot.name,
        ike_result=ike_result,
        destructive_potential=destructive_potential,
        surge_threat=surge_threat,
        overall_value=overall,
    )
