"""
Data models for hurricane representation and IKE computation.

Enhanced with:
  - Per-quadrant wind radii at 34, 50, 64 kt thresholds
  - Forward speed and direction for asymmetric wind modeling
  - Support for IBTrACS and HURDAT2 extended data
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import numpy as np


class SaffirSimpsonCategory(Enum):
    """Saffir-Simpson Hurricane Wind Scale categories."""
    TROPICAL_DEPRESSION = 0
    TROPICAL_STORM = 1
    CATEGORY_1 = 2
    CATEGORY_2 = 3
    CATEGORY_3 = 4
    CATEGORY_4 = 5
    CATEGORY_5 = 6

    @classmethod
    def from_wind_speed(cls, wind_speed_ms: float) -> "SaffirSimpsonCategory":
        """Classify by maximum sustained wind speed in m/s."""
        if wind_speed_ms < 17.5:
            return cls.TROPICAL_DEPRESSION
        elif wind_speed_ms < 33.0:
            return cls.TROPICAL_STORM
        elif wind_speed_ms < 43.0:
            return cls.CATEGORY_1
        elif wind_speed_ms < 50.0:
            return cls.CATEGORY_2
        elif wind_speed_ms < 58.0:
            return cls.CATEGORY_3
        elif wind_speed_ms < 70.0:
            return cls.CATEGORY_4
        else:
            return cls.CATEGORY_5


@dataclass
class WindFieldGrid:
    """
    A gridded wind field representing wind speeds across the storm area.

    The grid is defined in a local coordinate system centered on the storm,
    with distances in meters and wind speeds in m/s.

    Attributes:
        x: 1-D array of x-coordinates (meters, east-positive) relative to storm center
        y: 1-D array of y-coordinates (meters, north-positive) relative to storm center
        wind_speed: 2-D array of wind speed magnitudes (m/s), shape (len(y), len(x))
        timestamp: observation time for this snapshot
    """
    x: np.ndarray          # shape (nx,)
    y: np.ndarray          # shape (ny,)
    wind_speed: np.ndarray  # shape (ny, nx)
    timestamp: Optional[datetime] = None

    @property
    def dx(self) -> float:
        """Grid spacing in x-direction (meters)."""
        return float(self.x[1] - self.x[0]) if len(self.x) > 1 else 0.0

    @property
    def dy(self) -> float:
        """Grid spacing in y-direction (meters)."""
        return float(self.y[1] - self.y[0]) if len(self.y) > 1 else 0.0

    @property
    def cell_area(self) -> float:
        """Area of each grid cell in m^2."""
        return abs(self.dx * self.dy)

    def radial_distances(self) -> np.ndarray:
        """2-D array of distances from storm center (meters)."""
        xx, yy = np.meshgrid(self.x, self.y)
        return np.sqrt(xx**2 + yy**2)


@dataclass
class HurricaneSnapshot:
    """
    A single time-step observation of a hurricane.

    Enhanced with per-quadrant wind radii for asymmetric wind field modeling
    and storm motion parameters for motion-asymmetry correction.

    Quadrant dicts use keys: "NE", "SE", "SW", "NW" with values in meters.

    Attributes:
        storm_id: unique identifier (e.g., NOAA ATCF ID 'AL092024' or IBTrACS SID)
        name: storm name
        timestamp: observation time (UTC)
        lat: latitude of storm center (degrees N)
        lon: longitude of storm center (degrees E, negative for West)
        max_wind_ms: maximum sustained wind speed (m/s)
        min_pressure_hpa: minimum central pressure (hPa)
        rmw_m: radius of maximum winds (meters)
        r34_m: max radius of 34-knot winds across all quadrants (meters)
        r34_quadrants_m: 34-kt wind radius per quadrant {NE, SE, SW, NW} in meters
        r50_quadrants_m: 50-kt wind radius per quadrant in meters
        r64_quadrants_m: 64-kt wind radius per quadrant in meters
        forward_speed_ms: storm translational speed (m/s)
        forward_direction_deg: storm heading (degrees, meteorological convention: 0=N, 90=E)
        wind_field: optional full gridded wind field
    """
    storm_id: str
    name: str
    timestamp: datetime
    lat: float
    lon: float
    max_wind_ms: float
    min_pressure_hpa: Optional[float] = None
    rmw_m: Optional[float] = None
    r34_m: Optional[float] = None
    r50_m: Optional[float] = None
    r64_m: Optional[float] = None
    r34_quadrants_m: Optional[dict] = None
    r50_quadrants_m: Optional[dict] = None
    r64_quadrants_m: Optional[dict] = None
    forward_speed_ms: Optional[float] = None
    forward_direction_deg: Optional[float] = None
    wind_field: Optional[WindFieldGrid] = None

    @property
    def category(self) -> SaffirSimpsonCategory:
        return SaffirSimpsonCategory.from_wind_speed(self.max_wind_ms)

    @property
    def has_quadrant_data(self) -> bool:
        """Whether this snapshot has asymmetric wind radii data."""
        return self.r34_quadrants_m is not None

    @property
    def has_motion_data(self) -> bool:
        """Whether storm motion data is available for asymmetry correction."""
        return (
            self.forward_speed_ms is not None
            and self.forward_direction_deg is not None
        )


@dataclass
class IKEResult:
    """
    Result of an Integrated Kinetic Energy computation.

    Attributes:
        ike_total_tj: total IKE in terajoules (TJ)
        ike_hurricane_tj: IKE from hurricane-force winds (>= 33 m/s)
        ike_tropical_storm_tj: IKE from tropical-storm-force winds (18-33 m/s)
        timestamp: time of the wind field used
        storm_id: which storm this belongs to
        wind_field_source: how the wind field was obtained ('grid', 'parametric', 'asymmetric')
    """
    ike_total_tj: float
    ike_hurricane_tj: float
    ike_tropical_storm_tj: float
    timestamp: Optional[datetime] = None
    storm_id: Optional[str] = None
    wind_field_source: str = "parametric"

    @property
    def ike_total_pretty(self) -> str:
        """Human-readable IKE value."""
        if self.ike_total_tj >= 1000:
            return f"{self.ike_total_tj / 1000:.1f} PJ"
        return f"{self.ike_total_tj:.1f} TJ"


@dataclass
class HurricaneValuation:
    """
    Composite value assigned to a hurricane based on IKE and other factors.

    Attributes:
        storm_id: unique storm identifier
        name: storm name
        ike_result: the IKE computation result
        destructive_potential: normalized 0-100 score
        surge_threat: estimated storm surge threat level
        overall_value: composite value score (higher = more destructive)
    """
    storm_id: str
    name: str
    ike_result: IKEResult
    destructive_potential: float  # 0-100
    surge_threat: Optional[float] = None  # 0-100
    overall_value: Optional[float] = None  # composite score
