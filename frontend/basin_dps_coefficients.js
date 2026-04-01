/**
 * Basin-Specific DPS Formula Coefficients
 *
 * Derived from analysis of 24 historical destructive storms across 6 tropical cyclone basins.
 * Each basin has unique meteorological characteristics that require formula adjustments.
 *
 * Component weights:
 * - S (Surge): Storm surge + shelf effects (40% baseline Atlantic)
 * - Wf (Wind Field): Storm size measured by R64 & R34 radii (40% baseline)
 * - V (Velocity): Peak wind speed (10% baseline)
 * - F (Forward speed): Duration factor from movement speed (10% baseline)
 */

const BASIN_COEFFICIENTS = {
    ATLANTIC: {
        name: "Atlantic Basin",
        surge_weight: 0.40,      // 40% baseline
        wind_field_weight: 0.40, // 40% baseline
        wind_speed_weight: 0.10, // 10% baseline
        forward_speed_weight: 0.10, // 10% baseline
        ri_bonus: 0,  // No RI bonus
        description: "Reference baseline - balanced damage distribution"
    },
    EASTERN_PACIFIC: {
        name: "Eastern Pacific",
        surge_weight: 0.25,      // Reduced (narrow shelf, steep bathymetry)
        wind_field_weight: 0.38, // Reduced (compact wind fields typical)
        wind_speed_weight: 0.20, // Increased (extreme intensities)
        forward_speed_weight: 0.17, // Slightly increased
        ri_bonus: 15,  // +15 points for rapid intensification storms
        ri_threshold_mph_per_24h: 80,  // Apply bonus when 24-hour gain exceeds this
        description: "RI-dominant basin - boost intensity, reduce surge/breadth"
    },
    WESTERN_PACIFIC: {
        name: "Western Pacific (Typhoon Region)",
        surge_weight: 0.30,      // Moderate (complex island bathymetry)
        wind_field_weight: 0.45, // Increased (+5% from Atlantic)
        wind_speed_weight: 0.10, // Baseline
        forward_speed_weight: 0.15, // Increased (longer duration storms)
        ri_bonus: 0,
        description: "Breadth-dominant basin - large R34 areas critical"
    },
    NORTH_INDIAN: {
        name: "North Indian",
        surge_weight: 0.50,      // DOMINANT (Bay of Bengal funnel effect)
        wind_field_weight: 0.30, // Reduced
        wind_speed_weight: 0.08, // Reduced
        forward_speed_weight: 0.12, // Increased (duration matters)
        ri_bonus: 0,
        description: "Surge-dominant basin - Bay of Bengal amplifies dramatically"
    },
    SOUTH_INDIAN: {
        name: "South Indian",
        surge_weight: 0.42,      // Slightly increased from Atlantic
        wind_field_weight: 0.38, // Slightly reduced
        wind_speed_weight: 0.10, // Baseline
        forward_speed_weight: 0.10, // Baseline
        ri_bonus: 0,
        description: "Atlantic-like baseline with slight surge boost"
    },
    SOUTH_PACIFIC: {
        name: "South Pacific",
        surge_weight: 0.35,      // Atlantic baseline
        wind_field_weight: 0.40, // Atlantic baseline
        wind_speed_weight: 0.15, // Slightly increased (extreme intensities possible)
        forward_speed_weight: 0.10, // Baseline
        ri_bonus: 0,
        description: "Extreme intensity potential but sparse population"
    }
};

/**
 * Determine basin from latitude/longitude
 *
 * Geographic boundaries:
 * - Atlantic: 10°W to 100°W, 0° to 60°N (includes Gulf of Mexico, Caribbean)
 * - Eastern Pacific: 100°W to 140°W, 0° to 35°N
 * - Western Pacific: 100°E to 180°E, 0° to 60°N
 * - North Indian: 30°E to 100°E, 0° to 35°N
 * - South Indian: 20°E to 120°E, -40° to 0°
 * - South Pacific: 120°E to 120°W, -50° to 0°
 */
function detectBasin(lat, lon) {
    // Normalize longitude to -180 to 180
    let normLon = lon;
    if (lon > 180) normLon = lon - 360;
    if (lon < -180) normLon = lon + 360;

    // Atlantic Basin (includes Gulf/Caribbean)
    if (normLon >= -100 && normLon <= -10 && lat >= 0 && lat <= 60) {
        return "ATLANTIC";
    }

    // Eastern Pacific
    if (normLon >= -140 && normLon <= -100 && lat >= 0 && lat <= 35) {
        return "EASTERN_PACIFIC";
    }

    // Western Pacific (Typhoon region)
    if (normLon >= 100 && normLon <= 180 && lat >= 0 && lat <= 60) {
        return "WESTERN_PACIFIC";
    }

    // North Indian
    if (normLon >= 30 && normLon <= 100 && lat >= 0 && lat <= 35) {
        return "NORTH_INDIAN";
    }

    // South Indian
    if (normLon >= 20 && normLon <= 120 && lat >= -40 && lat < 0) {
        return "SOUTH_INDIAN";
    }

    // South Pacific
    if (normLon >= 120 && normLon <= 180 && lat >= -50 && lat < 0) {
        return "SOUTH_PACIFIC";
    }
    if (normLon >= -180 && normLon < -120 && lat >= -50 && lat < 0) {
        return "SOUTH_PACIFIC";
    }

    // Default to Atlantic if unknown (safest assumption for forecast basins)
    return "ATLANTIC";
}

/**
 * Apply basin-specific coefficients to DPS calculation
 *
 * @param {number} S - Storm Surge component (0-1)
 * @param {number} Wf - Wind Field component (0-1)
 * @param {number} V - Wind Speed component (0-1)
 * @param {number} F - Forward Speed/Duration component (0-1)
 * @param {string} basin - Basin identifier (ATLANTIC, EASTERN_PACIFIC, etc.)
 * @param {object} dataPoint - Full data point (for RI calculation if needed)
 * @returns {object} {rawScore, riBonus, finalScore}
 */
function applyBasinDPS(S, Wf, V, F, basin, dataPoint = null) {
    const coeffs = BASIN_COEFFICIENTS[basin] || BASIN_COEFFICIENTS.ATLANTIC;

    // Calculate weighted score with basin-specific coefficients
    let rawScore = 100 * (
        coeffs.surge_weight * S +
        coeffs.wind_field_weight * Wf +
        coeffs.wind_speed_weight * V +
        coeffs.forward_speed_weight * F
    );

    // Apply rapid intensification bonus if applicable
    let riBonus = 0;
    if (coeffs.ri_bonus > 0 && dataPoint && dataPoint.rapid_intensity_24h_mph) {
        if (dataPoint.rapid_intensity_24h_mph > coeffs.ri_threshold_mph_per_24h) {
            riBonus = coeffs.ri_bonus;
        }
    }

    const finalScore = Math.min(100, rawScore + riBonus);

    return {
        rawScore: rawScore,
        riBonus: riBonus,
        finalScore: finalScore,
        basinName: coeffs.name,
        basinCoefficients: coeffs
    };
}

/**
 * Get basin info for display purposes
 * @param {string} basin - Basin identifier
 * @returns {object} Basin information for UI display
 */
function getBasinInfo(basin) {
    const coeffs = BASIN_COEFFICIENTS[basin] || BASIN_COEFFICIENTS.ATLANTIC;
    return {
        name: coeffs.name,
        description: coeffs.description,
        dominantFactor: basin === "NORTH_INDIAN" ? "Storm Surge (Bay of Bengal funnel)" :
                       basin === "EASTERN_PACIFIC" ? "Rapid Intensification + Intensity" :
                       basin === "WESTERN_PACIFIC" ? "Wind Field Size" :
                       "Balanced (Surge + Wind Field)"
    };
}

// Export for use in index.html
window.BasinDPS = {
    COEFFICIENTS: BASIN_COEFFICIENTS,
    detectBasin,
    applyBasinDPS,
    getBasinInfo
};
