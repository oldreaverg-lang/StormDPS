"""Coastline points for the Pacific coasts the shared waypoint DB lacks.

core.land_proximity's CoastlineDatabase has only two Baja points on the
Pacific side of the Americas and none in Hawaii. That DB also feeds DPS
scoring and must not be extended casually, so these supplement it ONLY in
presentation paths: the forecast stall banner (api/routes._stall_near_land)
and the forecast landfall / closest-approach estimate
(core/landfall_forecast.py). (lat, lon, name); ~100-150 km spacing.
"""

PACIFIC_COAST_POINTS = (
    # Mainland Pacific Mexico, Chiapas -> Sonora
    (14.70, -92.40, "Puerto Chiapas, Mexico"), (15.94, -93.81, "Puerto Arista, Mexico"),
    (16.17, -95.20, "Salina Cruz, Mexico"), (15.75, -96.13, "Huatulco, Mexico"),
    (15.86, -97.07, "Puerto Escondido, Mexico"), (16.33, -98.57, "Punta Maldonado, Mexico"),
    (16.85, -99.88, "Acapulco, Mexico"), (17.27, -101.05, "Papanoa, Mexico"),
    (17.64, -101.55, "Zihuatanejo, Mexico"), (17.96, -102.20, "Lazaro Cardenas, Mexico"),
    (18.27, -103.35, "Maruata, Mexico"), (19.05, -104.32, "Manzanillo, Mexico"),
    (19.21, -104.68, "Barra de Navidad, Mexico"), (19.55, -105.10, "Chamela, Mexico"),
    (20.40, -105.70, "Cabo Corrientes, Mexico"), (20.62, -105.23, "Puerto Vallarta, Mexico"),
    (21.54, -105.29, "San Blas, Mexico"), (22.54, -105.75, "Teacapan, Mexico"),
    (23.22, -106.42, "Mazatlan, Mexico"), (24.63, -107.93, "Altata, Mexico"),
    (25.60, -109.05, "Topolobampo, Mexico"), (27.92, -110.90, "Guaymas, Mexico"),
    # Baja California
    (22.89, -109.91, "Cabo San Lucas, Mexico"), (23.06, -109.70, "San Jose del Cabo, Mexico"),
    (23.45, -110.22, "Todos Santos, Mexico"), (24.14, -110.31, "La Paz, Mexico"),
    (24.79, -112.11, "Puerto San Carlos, Mexico"), (26.01, -111.35, "Loreto, Mexico"),
    (27.34, -112.27, "Santa Rosalia, Mexico"), (27.97, -114.05, "Guerrero Negro, Mexico"),
    (31.86, -116.62, "Ensenada, Mexico"),
    # Central America, Pacific side
    (14.29, -91.91, "Champerico, Guatemala"), (13.92, -90.82, "Puerto San Jose, Guatemala"),
    (13.59, -89.83, "Acajutla, El Salvador"), (13.49, -89.32, "La Libertad, El Salvador"),
    (13.33, -87.84, "La Union, El Salvador"), (13.42, -87.45, "San Lorenzo, Honduras"),
    (12.48, -87.17, "Corinto, Nicaragua"), (11.25, -85.87, "San Juan del Sur, Nicaragua"),
    (10.30, -85.84, "Tamarindo, Costa Rica"), (9.98, -84.83, "Puntarenas, Costa Rica"),
    (9.43, -84.16, "Quepos, Costa Rica"), (8.64, -83.18, "Golfito, Costa Rica"),
    (8.37, -82.43, "Pedregal, Panama"), (8.95, -79.53, "Panama City, Panama"),
    # Hawaii
    (21.31, -157.86, "Honolulu, HI"), (21.09, -157.02, "Kaunakakai, HI"),
    (20.89, -156.47, "Kahului, HI"), (19.64, -155.99, "Kailua-Kona, HI"),
    (19.72, -155.08, "Hilo, HI"), (21.98, -159.37, "Lihue, HI"),
)
