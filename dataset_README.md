# StormDPS Historical Hurricane Database

A curated open-data record of major tropical cyclones from 1970 to the present, scored on the **Destructive Power Score (DPS)** — a 0–100 composite that combines peak intensity, Integrated Kinetic Energy, surge potential, duration of coastal exposure, and geographic reach. An open-data alternative to the Saffir-Simpson Hurricane Wind Scale.

**Web:** https://stormdps.com
**Methodology:** https://stormdps.com/methodology
**Live tracker:** https://stormdps.com/historic-storms
**Repository:** https://github.com/oldreaverg-lang/StormDPS
**License:** CC BY 4.0
**Version:** 1.0.0

---

## Contents

| File | Format | Description |
|---|---|---|
| `historical_storms_db.csv` | CSV | Primary tabular dataset. One row per named storm. |
| `historical_storms_db.json` | JSON | Same content as CSV, structured array form. |
| `README.md` | Markdown | This file. |

## Coverage

- **Temporal:** 1970–present
- **Spatial:** All ocean basins with tropical-cyclone activity (Atlantic, East Pacific, West Pacific, North Indian, South Indian, South Pacific, South Atlantic)
- **Storm count:** ~200 named cyclones, hand-curated for damage significance and observational completeness. Not a full IBTrACS-equivalent census — see <https://www.ncei.noaa.gov/products/international-best-track-archive> for that.

## Data dictionary

| Column | Type | Units | Description |
|---|---|---|---|
| `basin` | string | — | Originating ocean basin (Atlantic, East Pacific, West Pacific, North Indian, South Indian, South Pacific, South Atlantic). |
| `name` | string | — | Storm name as assigned by the WMO regional center. |
| `year` | integer | — | Year of peak intensity. |
| `damage_billions` | float | USD billions, inflation-adjusted to 2024 | Reported economic damage. Null if unknown. |
| `peak_wind_mph` | integer | mph | Peak one-minute sustained wind speed at any point in the storm's life. |
| `central_pressure_mb` | integer | mb (hPa) | Minimum central pressure. |
| `rmw_nm` | integer | nautical miles | Radius of maximum winds at peak intensity. |
| `r34_nm` | integer | nautical miles | Tropical-storm-force wind radius (R34) at peak. |
| `duration_days` | float | days | Days from genesis to dissipation. |
| `duration_factor` | float | 0–1.5 | DPS duration component — hours of coastal exposure normalized. |
| `rapid_intensification_24h_mph` | integer | mph | Peak 24-hour intensification rate. Null if storm did not undergo RI. |
| `landfalls` | string | — | Comma-separated list of landfall regions. |
| `notes` | string | — | Free-text storm description and contextual notes. |
| `validation_target` | boolean | — | Whether this storm is part of the DPS calibration set (True) or a held-out validation case (False). |

## Methodology

The Destructive Power Score is computed by combining five physically meaningful components:

1. **Peak intensity** — peak wind and minimum central pressure (the Saffir-Simpson variable, retained as a baseline).
2. **Integrated Kinetic Energy (IKE)** — total kinetic energy in the wind field above tropical-storm force, in terajoules. Captures storm size directly.
3. **Surge potential** — derived from peak wind, RMW, forward speed, and basin-specific bathymetry coefficients (SLOSH-calibrated surrogate).
4. **Duration of coastal exposure** — hours the wind field overlaps populated coastline.
5. **Geographic reach** — number of distinct coastal zones the wind field affects.

Components are combined into a normalized 0–100 score with basin-specific calibration (Atlantic, East Pacific, West Pacific, North Indian, South Indian, South Pacific) so that a DPS of 75 means roughly the same level of destructive potential regardless of basin.

Full methodology: <https://stormdps.com/methodology>.

## Sources

All upstream data is sourced from public records:

- **NHC (U.S. National Hurricane Center)** — operational advisories, HURDAT2 best-track archive
- **JTWC (Joint Typhoon Warning Center)** — West Pacific, North Indian, and Southern Hemisphere advisories
- **IBTrACS (NOAA NCEI)** — international best-track quality-controlled archive
- **ATCF (UCAR RAL)** — automated tropical cyclone forecast b-deck files
- **Damage estimates** — NOAA NCEI Billion-Dollar Weather and Climate Disasters database (Atlantic), plus published government/insurance industry reports (international basins)

No proprietary or paywalled data is used.

## Citation

```
Reaves, R. (2026). StormDPS Historical Hurricane Database (Version 1.0.0)
[Data set]. Zenodo. https://doi.org/10.5281/zenodo.20149123
```

BibTeX:

```bibtex
@dataset{reaves_2026_stormdps,
  author       = {Reaves, Ryan},
  title        = {StormDPS Historical Hurricane Database},
  month        = may,
  year         = 2026,
  publisher    = {Zenodo},
  version      = {1.0.0},
  doi          = {10.5281/zenodo.20149123},
  url          = {https://doi.org/10.5281/zenodo.20149123}
}
```

## License

This dataset is released under the **Creative Commons Attribution 4.0 International (CC BY 4.0)** license. You may:

- Share — copy and redistribute the material in any medium or format
- Adapt — remix, transform, and build upon the material
- Use commercially

Provided that you give appropriate credit, link to the license, and indicate if changes were made.

Full license: <https://creativecommons.org/licenses/by/4.0/>

The underlying observational records (NHC, JTWC, IBTrACS) are in the public domain and not subject to this license — only the DPS scores and derived components are.

## Changelog

- **1.0.0** (2026-05) — Initial public release. ~200 storms, DPS engine v9-wp-coastal with rainfall-footprint proxy and Mariana Islands coastal coverage.

## Contact

Issues, corrections, and suggestions: <https://github.com/oldreaverg-lang/StormDPS/issues>
