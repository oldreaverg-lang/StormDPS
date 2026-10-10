"""Real coastal towns, for NAMING places on the US Gulf and Atlantic coasts.

Why this exists: core/land_proximity's coastline waypoints carry hand-written
labels whose coordinates are often far from the town they name. During
Hurricane Isaias (2026-10-09) the landfall panel read "near Mobile Bay", then
"near Panama City", then "near Pensacola" for a storm coming ashore between
Fort Walton Beach and Destin: the waypoint labelled "Panama City, FL" sits at
30.41N 86.51W (Destin; Panama City is 85 km east), "Pensacola, FL" sits near
Niceville, "Gulfport, MS" on Dauphin Island, "Galveston, TX" on downtown
Houston. Those waypoint COORDINATES feed distance-to-coast scoring and must
not move (that would change DPS and need a rebake), so display names come
from this list instead: the nearest real town to the point being described.

Coordinates are town centres to ~0.01 deg, each checked against
OpenStreetMap's geocoder (2026-10-09). Coverage: Texas to Maine, Puerto Rico
and the US Virgin Islands. Elsewhere callers fall back to their own labels.

Stdlib only.
"""
from __future__ import annotations

import math
from typing import Optional, Tuple

# (lat, lon, name) — west to east along the Gulf, then south to north.
US_COASTAL_PLACES: Tuple[Tuple[float, float, str], ...] = (
    # Texas
    (25.90, -97.50, "Brownsville, TX"),
    (26.11, -97.17, "South Padre Island, TX"),
    (26.55, -97.43, "Port Mansfield, TX"),
    (27.80, -97.40, "Corpus Christi, TX"),
    (27.83, -97.06, "Port Aransas, TX"),
    (28.02, -97.05, "Rockport, TX"),
    (28.45, -96.41, "Port O'Connor, TX"),
    (28.61, -96.63, "Port Lavaca, TX"),
    (28.69, -95.97, "Matagorda, TX"),
    (28.95, -95.36, "Freeport, TX"),
    (29.30, -94.80, "Galveston, TX"),
    (29.38, -94.90, "Texas City, TX"),
    (29.76, -95.37, "Houston, TX"),
    (29.57, -94.39, "High Island, TX"),
    (29.90, -93.93, "Port Arthur, TX"),
    (30.08, -94.13, "Beaumont, TX"),
    # Louisiana
    (29.80, -93.33, "Cameron, LA"),
    (30.23, -93.22, "Lake Charles, LA"),
    (29.65, -92.43, "Pecan Island, LA"),
    (29.78, -92.16, "Intracoastal City, LA"),
    (29.70, -91.21, "Morgan City, LA"),
    (29.60, -90.72, "Houma, LA"),
    (29.25, -90.66, "Cocodrie, LA"),
    (29.11, -90.20, "Port Fourchon, LA"),
    (29.24, -89.99, "Grand Isle, LA"),
    (29.95, -90.07, "New Orleans, LA"),
    (29.28, -89.35, "Venice, LA"),
    (30.28, -89.78, "Slidell, LA"),
    # Mississippi / Alabama
    (30.31, -89.33, "Bay St. Louis, MS"),
    (30.37, -89.09, "Gulfport, MS"),
    (30.40, -88.89, "Biloxi, MS"),
    (30.37, -88.56, "Pascagoula, MS"),
    (30.25, -88.11, "Dauphin Island, AL"),
    (30.69, -88.04, "Mobile, AL"),
    (30.25, -87.70, "Gulf Shores, AL"),
    (30.29, -87.57, "Orange Beach, AL"),
    # Florida Panhandle and Big Bend
    (30.42, -87.22, "Pensacola, FL"),
    (30.33, -87.14, "Pensacola Beach, FL"),
    (30.40, -86.86, "Navarre, FL"),
    (30.41, -86.62, "Fort Walton Beach, FL"),
    (30.39, -86.50, "Destin, FL"),
    (30.40, -86.23, "Santa Rosa Beach, FL"),
    (30.18, -85.81, "Panama City Beach, FL"),
    (30.16, -85.66, "Panama City, FL"),
    (29.95, -85.42, "Mexico Beach, FL"),
    (29.81, -85.30, "Port St. Joe, FL"),
    (29.73, -84.98, "Apalachicola, FL"),
    (29.85, -84.66, "Carrabelle, FL"),
    (30.16, -84.21, "St. Marks, FL"),
    (29.67, -83.39, "Steinhatchee, FL"),
    (29.14, -83.04, "Cedar Key, FL"),
    # Florida west coast and Keys
    (28.90, -82.59, "Crystal River, FL"),
    (28.24, -82.72, "New Port Richey, FL"),
    (27.97, -82.80, "Clearwater, FL"),
    (27.95, -82.46, "Tampa, FL"),
    (27.77, -82.64, "St. Petersburg, FL"),
    (27.50, -82.57, "Bradenton, FL"),
    (27.34, -82.53, "Sarasota, FL"),
    (27.10, -82.45, "Venice, FL"),
    (26.93, -82.05, "Punta Gorda, FL"),
    (26.75, -82.26, "Boca Grande, FL"),
    (26.64, -81.87, "Fort Myers, FL"),
    (26.45, -81.95, "Fort Myers Beach, FL"),
    (26.14, -81.79, "Naples, FL"),
    (25.94, -81.72, "Marco Island, FL"),
    (25.86, -81.38, "Everglades City, FL"),
    (24.56, -81.78, "Key West, FL"),
    (24.71, -81.09, "Marathon, FL"),
    (25.09, -80.45, "Key Largo, FL"),
    # Florida east coast
    (25.47, -80.48, "Homestead, FL"),
    (25.76, -80.19, "Miami, FL"),
    (26.12, -80.14, "Fort Lauderdale, FL"),
    (26.71, -80.05, "West Palm Beach, FL"),
    (27.20, -80.25, "Stuart, FL"),
    (27.45, -80.33, "Fort Pierce, FL"),
    (27.64, -80.40, "Vero Beach, FL"),
    (28.08, -80.61, "Melbourne, FL"),
    (28.39, -80.60, "Cape Canaveral, FL"),
    (29.21, -81.02, "Daytona Beach, FL"),
    (29.90, -81.31, "St. Augustine, FL"),
    (30.29, -81.39, "Jacksonville Beach, FL"),
    (30.67, -81.46, "Fernandina Beach, FL"),
    # Georgia / South Carolina
    (31.15, -81.49, "Brunswick, GA"),
    (32.08, -81.09, "Savannah, GA"),
    (32.00, -80.85, "Tybee Island, GA"),
    (32.22, -80.75, "Hilton Head Island, SC"),
    (32.43, -80.67, "Beaufort, SC"),
    (32.78, -79.93, "Charleston, SC"),
    (33.09, -79.46, "McClellanville, SC"),
    (33.38, -79.29, "Georgetown, SC"),
    (33.69, -78.89, "Myrtle Beach, SC"),
    # North Carolina / Virginia
    (33.92, -78.02, "Southport, NC"),
    (34.23, -77.94, "Wilmington, NC"),
    (34.37, -77.63, "Topsail Beach, NC"),
    (34.72, -76.73, "Morehead City, NC"),
    (35.11, -75.98, "Ocracoke, NC"),
    (35.25, -75.53, "Buxton, NC"),
    (35.96, -75.62, "Nags Head, NC"),
    (36.85, -75.98, "Virginia Beach, VA"),
    (36.85, -76.29, "Norfolk, VA"),
    (37.93, -75.38, "Chincoteague, VA"),
    # Mid-Atlantic
    (38.34, -75.08, "Ocean City, MD"),
    (38.72, -75.08, "Rehoboth Beach, DE"),
    (38.94, -74.91, "Cape May, NJ"),
    (39.36, -74.42, "Atlantic City, NJ"),
    (39.94, -74.07, "Seaside Heights, NJ"),
    (40.71, -74.01, "New York, NY"),
    (40.59, -73.66, "Long Beach, NY"),
    (41.04, -71.95, "Montauk, NY"),
    # New England
    (41.31, -72.92, "New Haven, CT"),
    (41.36, -72.10, "New London, CT"),
    (41.49, -71.31, "Newport, RI"),
    (41.82, -71.41, "Providence, RI"),
    (41.64, -70.93, "New Bedford, MA"),
    (41.28, -70.10, "Nantucket, MA"),
    (41.68, -69.96, "Chatham, MA"),
    (42.05, -70.19, "Provincetown, MA"),
    (42.36, -71.06, "Boston, MA"),
    (42.61, -70.66, "Gloucester, MA"),
    (43.07, -70.76, "Portsmouth, NH"),
    (43.66, -70.26, "Portland, ME"),
    (44.10, -69.11, "Rockland, ME"),
    (44.39, -68.20, "Bar Harbor, ME"),
    (44.91, -66.99, "Eastport, ME"),
    # Puerto Rico / US Virgin Islands
    (18.47, -66.11, "San Juan, PR"),
    (18.01, -66.61, "Ponce, PR"),
    (18.20, -67.14, "Mayaguez, PR"),
    (18.33, -65.65, "Fajardo, PR"),
    (18.34, -64.93, "Charlotte Amalie, USVI"),
    (17.75, -64.70, "Christiansted, USVI"),
)


def _km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))


def nearest_place(lat: float, lon: float, max_km: float = 45.0) -> Optional[Tuple[str, float]]:
    """(name, km) of the nearest listed town within max_km, else None."""
    best, best_d = None, max_km
    for pla, plo, name in US_COASTAL_PLACES:
        if abs(pla - lat) > 1.0 or abs(plo - lon) > 1.5:      # cheap reject
            continue
        d = _km(lat, lon, pla, plo)
        if d <= best_d:
            best, best_d = name, d
    return (best, best_d) if best else None


def place_name(lat: Optional[float], lon: Optional[float], fallback: Optional[str] = None,
               max_km: float = 45.0) -> Optional[str]:
    """Display name for a coastal point: the nearest real town when this list
    covers the area, otherwise the caller's own label."""
    if lat is None or lon is None:
        return fallback
    hit = nearest_place(float(lat), float(lon), max_km)
    return hit[0] if hit else fallback
