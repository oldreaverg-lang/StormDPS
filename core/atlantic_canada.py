"""Coast points for Atlantic Canada (and Saint-Pierre), which the waypoint DB lacks.

core.land_proximity's coastline waypoints stop at Maine, so a storm bound for
Nova Scotia or Newfoundland had "no coast ahead": the landfall panel said
"stays offshore" and the closest approach was measured to New England.
Josephine AL102026 (2026-10-21) passed ~250 km off Sable Island and Cape
Race, where NHC gave 45-60% odds of tropical-storm-force winds, and the page
named New London, CT as its closest coast.

Used for:
  - the forecast landfall / closest-approach panel (core/landfall_forecast.py)
    and the cone-reaches-coast test;
  - "is there a coast ahead" in DPS scoring (core/cumulative_dpi.offshore_fixes):
    a fix headed for these coasts is not damped as open ocean.
They do NOT assign a scoring region: no Canadian coastal or economic profile
exists, and a fix bound here keeps the nearest-waypoint region it had before.

(lat, lon, name), town centres checked against OpenStreetMap's geocoder
(2026-10-21). Spacing ~30-70 km along the coasts hurricanes reach. Sable
Island is left out on purpose: it is a coast point for NHC's wind odds but not
a populated coast, and a pass over it is not a landfall.

Stdlib only.
"""
from __future__ import annotations

from typing import Tuple

ATLANTIC_CANADA_POINTS: Tuple[Tuple[float, float, str], ...] = (
    # Nova Scotia: Yarmouth east along the Atlantic shore, round Cape Breton,
    # then the Northumberland Strait and the Bay of Fundy
    (43.84, -66.12, "Yarmouth, NS"),
    (43.56, -65.56, "Barrington, NS"),
    (43.76, -65.32, "Shelburne, NS"),
    (44.04, -64.72, "Liverpool, NS"),
    (44.38, -64.32, "Lunenburg, NS"),
    (44.65, -63.59, "Halifax, NS"),
    (44.79, -63.15, "Musquodoboit Harbour, NS"),
    (44.93, -62.54, "Sheet Harbour, NS"),
    (45.02, -62.00, "Liscomb, NS"),
    (45.11, -61.73, "Port Bickerton, NS"),
    (45.34, -61.00, "Canso, NS"),
    (45.39, -61.50, "Guysborough, NS"),
    (45.62, -61.36, "Port Hawkesbury, NS"),
    (45.66, -60.88, "St. Peter's, NS"),
    (45.92, -59.96, "Louisbourg, NS"),
    (46.19, -59.96, "Glace Bay, NS"),
    (46.14, -60.19, "Sydney, NS"),
    (46.69, -60.37, "Ingonish, NS"),
    (46.90, -60.47, "Dingwall, NS"),
    (46.63, -61.02, "Cheticamp, NS"),
    (46.23, -61.31, "Inverness, NS"),
    (45.68, -62.71, "Pictou, NS"),
    (45.85, -63.66, "Pugwash, NS"),
    (45.41, -64.33, "Parrsboro, NS"),
    (44.62, -65.76, "Digby, NS"),
    # New Brunswick
    (45.07, -67.05, "Saint Andrews, NB"),
    (45.27, -66.06, "Saint John, NB"),
    (45.60, -64.94, "Alma, NB"),
    (46.22, -64.55, "Shediac, NB"),
    (46.68, -64.88, "Richibucto, NB"),
    (47.05, -64.87, "Escuminac, NB"),
    (47.79, -64.95, "Caraquet, NB"),
    (47.62, -65.65, "Bathurst, NB"),
    # Prince Edward Island
    (46.24, -63.13, "Charlottetown, PE"),
    (46.39, -63.79, "Summerside, PE"),
    (46.36, -62.25, "Souris, PE"),
    (46.46, -63.31, "North Rustico, PE"),
    (46.81, -64.06, "Alberton, PE"),
    # Quebec: Magdalen Islands and the Gaspe
    (47.38, -61.87, "Cap-aux-Meules, QC"),
    (48.83, -64.48, "Gaspe, QC"),
    (48.52, -64.21, "Perce, QC"),
    # Newfoundland: south coast west to east, the Avalon, then north and west
    (47.58, -59.14, "Channel-Port aux Basques, NL"),
    (47.61, -57.61, "Burgeo, NL"),
    (47.58, -56.75, "Francois, NL"),
    (47.47, -55.83, "Harbour Breton, NL"),
    (47.17, -55.16, "Marystown, NL"),
    (46.92, -55.39, "St. Lawrence, NL"),
    (47.24, -53.96, "Placentia, NL"),
    (46.74, -53.37, "Trepassey, NL"),
    (47.02, -52.89, "Ferryland, NL"),
    (47.56, -52.71, "St. John's, NL"),
    (47.58, -53.28, "Bay Roberts, NL"),
    (48.17, -53.96, "Clarenville, NL"),
    (48.65, -53.11, "Bonavista, NL"),
    (49.65, -54.76, "Twillingate, NL"),
    (51.37, -55.59, "St. Anthony, NL"),
    (50.71, -57.36, "Port au Choix, NL"),
    (49.59, -57.92, "Rocky Harbour, NL"),
    (48.95, -57.95, "Corner Brook, NL"),
    (48.55, -58.59, "Stephenville, NL"),
    # France
    (46.78, -56.18, "Saint-Pierre (France)"),
)
