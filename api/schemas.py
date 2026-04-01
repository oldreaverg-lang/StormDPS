"""
Pydantic schemas for API request/response serialization.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class StormSummary(BaseModel):
    """Brief info for listing active storms."""
    id: str
    name: str
    classification: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    intensity_knots: Optional[float] = None
    pressure_mb: Optional[float] = None
    movement: Optional[str] = None
    movement_speed_knots: Optional[float] = None
    movement_direction_deg: Optional[float] = None


class QuadrantRadii(BaseModel):
    """Wind radii per quadrant in nautical miles."""
    NE: Optional[float] = None
    SE: Optional[float] = None
    SW: Optional[float] = None
    NW: Optional[float] = None


class IKEResponse(BaseModel):
    """IKE computation result."""
    storm_id: str
    timestamp: Optional[datetime] = None
    ike_total_tj: float = Field(description="Total IKE in terajoules")
    ike_hurricane_tj: float = Field(description="IKE from hurricane-force winds (>=33 m/s)")
    ike_tropical_storm_tj: float = Field(description="IKE from tropical-storm-force winds (18-33 m/s)")
    ike_pretty: str = Field(description="Human-readable IKE string")
    lat: Optional[float] = Field(None, description="Latitude of storm center at this time. Positive for North.")
    lon: Optional[float] = Field(None, description="Longitude of storm center at this time. Positive for East.")
    wind_field_source: str = Field(
        "parametric",
        description="How wind field was obtained: 'grid' (GRIB2), 'asymmetric' (quadrant model), or 'parametric' (symmetric Holland)"
    )
    # Wind field parameters for client-side rendering
    max_wind_ms: Optional[float] = Field(None, description="Max sustained wind speed (m/s)")
    min_pressure_hpa: Optional[float] = Field(None, description="Minimum central pressure (hPa/mb)")
    rmw_nm: Optional[float] = Field(None, description="Radius of max winds (nautical miles)")
    r34_nm: Optional[float] = Field(None, description="Max radius of 34-kt winds (nautical miles)")
    r64_nm: Optional[float] = Field(None, description="Max radius of 64-kt winds (nautical miles)")
    r34_quadrants: Optional[QuadrantRadii] = Field(None, description="34-kt wind radii per quadrant (nm)")
    forward_speed_knots: Optional[float] = Field(None, description="Storm forward speed (knots)")
    forward_direction_deg: Optional[float] = Field(None, description="Storm heading (degrees, 0=N)")
    radii_confidence: Optional[float] = Field(
        None,
        description="Wind radii data confidence (0.0-1.0) from cross-source audit. "
                    "None if audit has not run for this advisory cycle.",
    )


class ValuationResponse(BaseModel):
    """Full hurricane valuation result."""
    storm_id: str
    name: str
    timestamp: Optional[datetime] = None
    ike: IKEResponse
    destructive_potential: float = Field(description="IKE-based score 0-100")
    surge_threat: Optional[float] = Field(None, description="Surge threat score 0-100")
    overall_value: Optional[float] = Field(None, description="Composite value 0-100")
    category: str = Field(description="Saffir-Simpson category label")


class SnapshotInput(BaseModel):
    """Manual storm data input for custom IKE computation."""
    storm_id: str = "CUSTOM01"
    name: str = "Custom Storm"
    lat: float = 25.0
    lon: float = -80.0
    max_wind_knots: float = Field(description="Max sustained wind in knots")
    min_pressure_hpa: Optional[float] = None
    rmw_nm: Optional[float] = Field(None, description="Radius of max winds in nautical miles")
    r34_nm: Optional[float] = Field(None, description="Radius of 34-kt winds in nautical miles")
    r34_ne_nm: Optional[float] = Field(None, description="34-kt radius NE quadrant (nm)")
    r34_se_nm: Optional[float] = Field(None, description="34-kt radius SE quadrant (nm)")
    r34_sw_nm: Optional[float] = Field(None, description="34-kt radius SW quadrant (nm)")
    r34_nw_nm: Optional[float] = Field(None, description="34-kt radius NW quadrant (nm)")
    forward_speed_knots: Optional[float] = Field(None, description="Storm forward speed (knots)")
    forward_direction_deg: Optional[float] = Field(None, description="Storm heading (degrees, 0=N)")
    grid_resolution_km: float = Field(5.0, description="Grid resolution in km for IKE calculation")


class IBTrACSSearchInput(BaseModel):
    """Search IBTrACS by storm name and optional year. If year is omitted, returns the most recent match."""
    name: str = Field(description="Storm name (e.g., 'KATRINA')")
    year: Optional[int] = Field(None, description="Season year (if omitted, most recent match is used)")
    basin: Optional[str] = Field(None, description="Basin code: NA, EP, WP, NI, SI, SP, SA")
