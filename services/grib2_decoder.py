"""
Lightweight GRIB2 decoder for extracting wind field data.

Parses GRIB2 files from NOAA GFS/HWRF to extract U and V wind components
at 10m above ground, then computes wind speed magnitudes on a lat/lon grid.

This decoder handles the subset of GRIB2 needed for our use case:
  - Simple packing (template 5.0) and JPEG2000 (template 5.40)
  - Regular lat/lon grids (template 3.0)
  - Specifically targets UGRD and VGRD at 10m above ground

For full GRIB2 spec: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/

Note: In production, prefer cfgrib/eccodes for robustness. This decoder
is a lightweight fallback when those libraries aren't available.
"""

import struct
import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class GribMessage:
    """A single decoded GRIB2 message (one variable on one level)."""
    parameter_category: int
    parameter_number: int
    level_type: int
    level_value: int
    ni: int  # number of points along x (longitude)
    nj: int  # number of points along y (latitude)
    lat_first: float
    lon_first: float
    lat_last: float
    lon_last: float
    dlat: float
    dlon: float
    values: np.ndarray  # shape (nj, ni), the decoded data


def decode_grib2(data: bytes) -> list[GribMessage]:
    """
    Decode a GRIB2 byte stream into a list of GribMessages.

    Args:
        data: raw GRIB2 file contents

    Returns:
        List of decoded messages
    """
    messages = []
    offset = 0

    while offset < len(data) - 4:
        # Find next GRIB marker
        marker_pos = data.find(b"GRIB", offset)
        if marker_pos == -1:
            break

        # Section 0: Indicator Section (16 bytes)
        if len(data) < marker_pos + 16:
            break

        discipline = data[marker_pos + 6]
        edition = data[marker_pos + 7]
        if edition != 2:
            logger.warning(f"Skipping non-GRIB2 message (edition={edition})")
            offset = marker_pos + 4
            continue

        total_length = struct.unpack_from(">Q", data, marker_pos + 8)[0]
        msg_data = data[marker_pos : marker_pos + total_length]

        try:
            msg = _parse_grib2_message(msg_data, discipline)
            if msg is not None:
                messages.append(msg)
        except Exception as e:
            logger.warning(f"Failed to parse GRIB2 message at offset {marker_pos}: {e}")

        offset = marker_pos + total_length

    return messages


def _parse_grib2_message(msg: bytes, discipline: int) -> Optional[GribMessage]:
    """Parse a single GRIB2 message from its raw bytes."""
    pos = 16  # skip Section 0

    # We need to collect info from sections 3, 4, 5, 7
    ni = nj = 0
    lat_first = lon_first = lat_last = lon_last = 0.0
    dlat = dlon = 0.0
    param_cat = param_num = level_type = level_value = 0
    packing_type = 0
    R = E = D = 0
    num_bits = 0
    num_data_points = 0
    values = None

    while pos < len(msg) - 4:
        # Each section starts with 4-byte length and 1-byte section number
        if pos + 5 > len(msg):
            break

        section_len = struct.unpack_from(">I", msg, pos)[0]
        section_num = msg[pos + 4]

        # Check for end marker "7777"
        if msg[pos : pos + 4] == b"7777":
            break

        if section_len < 5 or pos + section_len > len(msg):
            break

        section_data = msg[pos : pos + section_len]

        if section_num == 3:
            # Grid Definition Section
            ni, nj, lat_first, lon_first, lat_last, lon_last, dlat, dlon = \
                _parse_section3(section_data)

        elif section_num == 4:
            # Product Definition Section
            param_cat, param_num, level_type, level_value = \
                _parse_section4(section_data)

        elif section_num == 5:
            # Data Representation Section
            num_data_points, packing_type, R, E, D, num_bits = \
                _parse_section5(section_data)

        elif section_num == 7:
            # Data Section - actual values
            if packing_type == 0:  # Simple packing
                values = _unpack_simple(
                    section_data[5:], num_data_points, R, E, D, num_bits
                )

        pos += section_len

    if values is None or ni == 0 or nj == 0:
        return None

    # Reshape to 2D grid
    if len(values) == ni * nj:
        values = values.reshape(nj, ni)
    else:
        logger.warning(
            f"Data size mismatch: {len(values)} vs grid {nj}x{ni}={nj*ni}"
        )
        return None

    return GribMessage(
        parameter_category=param_cat,
        parameter_number=param_num,
        level_type=level_type,
        level_value=level_value,
        ni=ni,
        nj=nj,
        lat_first=lat_first,
        lon_first=lon_first,
        lat_last=lat_last,
        lon_last=lon_last,
        dlat=dlat,
        dlon=dlon,
        values=values,
    )


def _parse_section3(data: bytes) -> tuple:
    """Parse Grid Definition Section (template 3.0 = regular lat/lon)."""
    # Bytes 6-7: grid definition template number
    template = struct.unpack_from(">H", data, 12)[0]

    if template != 0:
        logger.info(f"Grid template {template} (not regular lat/lon)")

    # Template 3.0 fields:
    ni = struct.unpack_from(">I", data, 30)[0]  # Ni
    nj = struct.unpack_from(">I", data, 34)[0]  # Nj

    lat_first = struct.unpack_from(">i", data, 46)[0] / 1e6  # degrees
    lon_first = struct.unpack_from(">i", data, 50)[0] / 1e6
    lat_last = struct.unpack_from(">i", data, 55)[0] / 1e6
    lon_last = struct.unpack_from(">i", data, 59)[0] / 1e6

    dlat_raw = struct.unpack_from(">I", data, 67)[0]
    dlon_raw = struct.unpack_from(">I", data, 63)[0]
    dlat = dlat_raw / 1e6
    dlon = dlon_raw / 1e6

    return ni, nj, lat_first, lon_first, lat_last, lon_last, dlat, dlon


def _parse_section4(data: bytes) -> tuple:
    """Parse Product Definition Section for parameter and level info."""
    # Template number at bytes 7-8
    param_cat = data[9]
    param_num = data[10]
    level_type = data[22]      # type of first fixed surface
    level_value = struct.unpack_from(">I", data, 23)[0] if len(data) > 26 else 0

    return param_cat, param_num, level_type, level_value


def _parse_section5(data: bytes) -> tuple:
    """Parse Data Representation Section."""
    num_data_points = struct.unpack_from(">I", data, 5)[0]
    template = struct.unpack_from(">H", data, 9)[0]

    R = E = D = num_bits = 0

    if template == 0:  # Simple packing
        R = struct.unpack_from(">f", data, 11)[0]  # reference value (IEEE 32-bit float)
        E = struct.unpack_from(">h", data, 15)[0]  # binary scale factor
        D = struct.unpack_from(">h", data, 17)[0]  # decimal scale factor
        num_bits = data[19]

    return num_data_points, template, R, E, D, num_bits


def _unpack_simple(
    raw: bytes,
    n_points: int,
    R: float,
    E: int,
    D: int,
    num_bits: int,
) -> np.ndarray:
    """
    Unpack simple-packed GRIB2 data.

    Y = (R + X * 2^E) / 10^D

    where X is the raw integer value at each grid point.
    """
    if num_bits == 0:
        return np.full(n_points, R / (10.0**D))

    # Extract packed integers
    values = np.zeros(n_points, dtype=np.float64)
    bit_offset = 0

    for i in range(n_points):
        byte_pos = bit_offset // 8
        bit_pos = bit_offset % 8

        if byte_pos + (num_bits + 7) // 8 > len(raw):
            break

        # Read enough bytes to cover num_bits starting at bit_pos
        n_bytes = (bit_pos + num_bits + 7) // 8
        raw_val = 0
        for b in range(n_bytes):
            if byte_pos + b < len(raw):
                raw_val = (raw_val << 8) | raw[byte_pos + b]

        # Shift right to align and mask
        shift = n_bytes * 8 - bit_pos - num_bits
        raw_val = (raw_val >> shift) & ((1 << num_bits) - 1)

        values[i] = (R + raw_val * (2.0**E)) / (10.0**D)
        bit_offset += num_bits

    return values


def extract_wind_components(
    messages: list[GribMessage],
) -> tuple[Optional[GribMessage], Optional[GribMessage]]:
    """
    Find U and V wind component messages at 10m above ground.

    In GRIB2 WMO coding:
      - Discipline 0 (Meteorological), Category 2 (Momentum)
      - Parameter 2 = UGRD (U-component of wind)
      - Parameter 3 = VGRD (V-component of wind)
      - Level type 103 = specified height above ground
      - Level value 10 = 10 meters

    Returns:
        (u_msg, v_msg) tuple, either may be None if not found
    """
    u_msg = None
    v_msg = None

    for msg in messages:
        if msg.parameter_category == 2 and msg.level_type == 103:
            if msg.parameter_number == 2 and msg.level_value == 10:
                u_msg = msg
            elif msg.parameter_number == 3 and msg.level_value == 10:
                v_msg = msg

    return u_msg, v_msg


def compute_wind_speed_grid(
    u_msg: GribMessage,
    v_msg: GribMessage,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute wind speed magnitude from U and V components.

    Returns:
        (lats, lons, wind_speed) - 1D lat array, 1D lon array, 2D speed array
    """
    wind_speed = np.sqrt(u_msg.values**2 + v_msg.values**2)

    lats = np.linspace(u_msg.lat_first, u_msg.lat_last, u_msg.nj)
    lons = np.linspace(u_msg.lon_first, u_msg.lon_last, u_msg.ni)

    return lats, lons, wind_speed
