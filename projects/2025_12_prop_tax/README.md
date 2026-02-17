# 340B, consolidation, and property taxes

## Overview

This project examines a subtle second-order effect of the 340B Drug Pricing
Program. Namely, property tax increases for local taxpayers resulting from
nonprofit hospitals and health systems acquiring for-profit hospitals.

It uses MacNeal Hospital, acquired by Loyola Medicine from Tenet Healthcare
in March 2018, as a case study. MacNeal's conversion to nonprofit status
removed millions of dollars in assessed value from the tax base, causing an
average bill increase in the surrounding city of $100 per household.

## Data used

This analysis uses five primary data sources:

1. A manually compiled hospital acquisitions dataset covering 25 hospitals
  across 13 acquisition transactions (2015–2024). This includes acquisition
  dates, 340B enrollment dates, acquiring system details, and source links.
2. [CMS cost reports](https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/hospital-2552-2010-form)
  (Hospital 2552-2010 form SAS files) for hospital financial data.
3. The [HRSA 340B OPAIS database](https://340bopais.hrsa.gov/home) for
  identifying 340B enrollment dates for acquired hospitals.
4. Cook County property tax data via the
  [Socrata API](https://datacatalog.cookcountyil.gov/) (to determine MacNeal
  PINs) and the [PTAXSIM](https://github.com/ccao-data/ptaxsim) SQLite database
  for property tax simulation.
5. [CMS Hospital Change of Ownership (CHOW)](https://data.cms.gov/) data for
  identifying for-profit-to-nonprofit ownership transitions.

## Scope

### Providers

The acquisition tracking dataset includes 25 hospitals acquired by nonprofit
systems from for-profit operators across 13 transactions between 2015 and 2024.
Acquiring systems include SSM Health, Wellstar, Tower Health, Baptist Health,
Loyola Medicine, and others.

### Time periods

- Acquisition transactions span from July 2015 to November 2024
- CMS cost reports are loaded across multiple years to track hospital
  financials before and after acquisition
- Cook County property tax data covers 2015–2023, with the counterfactual
  simulation focused on the 2019–2020 transition (the first full tax year after
  MacNeal's exemption took effect)

### Geography

National for acquisition tracking (hospitals across many states).
Cook County, IL for the property tax case study.

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is *not* comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

The Python ingestion pipeline ([`ingest.py`](./ingest.py)) performs the
following steps:

1. **Hospital acquisitions**: Loads the manually compiled acquisitions CSV and
  adds hospital metadata from the Turquoise Health Trino database.
2. **Cook County PIN confirmation**: Takes a list of possible MacNeal Hospital
  property PINs (identified via the Cook County web map) and queries the Cook
  County Socrata API to retrieve mailing addresses. PINs are confirmed as
  MacNeal-related if their mailing address contains "MAC", "LOYOLA", or
  "EXEMPT".
3. **CMS cost reports**: Loads Hospital 2552-2010 form SAS files from the CMS
  website, extracting key financial fields.

### Tax simulation

The R-based tax simulation ([`tax_sim.R`](./tax_sim.R)) uses
[PTAXSIM](https://github.com/ccao-data/ptaxsim) to calculate property tax
bills for Berwyn Township properties. Two scenarios are considered, the
*actual* scenario in which MacNeal Hospital is exempt, and a counterfactual
scenario in which MacNeal's assessment was never changed. The resulting tax
bills are then compared to determine the impact of the MacNeal acquisition.

### Assumptions and limitations

- **Counterfactual growth rate**: The counterfactual assessed value for MacNeal
  PINs uses the average year-over-year assessed value change for Berwyn Township
  properties in the same major class. This assumes MacNeal's property would have
  appreciated at the same rate as surrounding properties.
- **Limited geography for tax analysis**: The property tax simulation is limited
  to Cook County, IL, as PTAXSIM only covers this area. Data is limited for
  other geographies, making a national analysis extremely difficult.
- **Manual acquisition data**: The hospital acquisitions dataset was compiled
  from news articles, press releases, and CMS CHOW data. It is not exhaustive
  and may not capture all for-profit-to-nonprofit transactions during the study
  period.

## Replication

This analysis is intended to be fully reproducible. However, the full
replication data is extremely large and difficult to share. If you'd like
to reproduce or expand the piece, please contact me directly.

### Dependencies

For the tax simulation and analysis, open R/RStudio and run:

```r
install.packages("renv")
renv::restore()
```

This will install all required R packages and dependencies.

If you're an internal Turquoise user or have access to Trino, you can also run
the data ingest step in Python. To do so, first:

1. Populate a `.env` file with the structure below and place it at the
  `pricepoints` repository root. The file should look like this:

    ```env
    TQ_TRINO_HOST=
    TQ_TRINO_PORT=
    TQ_TRINO_CATALOG=
    TQ_TRINO_USERNAME=
    TQ_TRINO_PASSWORD=
    SOCRATA_APP_TOKEN=
    ```

2. Create a virtual environment and install the required Python packages:

    ```bash
    uv venv
    source .venv/bin/activate
    uv sync
    ```

### Execution order

1. Run the Python ingestion script:

    ```bash
    uv run ingest.py
    ```

2. Run the R tax simulation:

    ```bash
    Rscript tax_sim.R
    ```

3. Render the Quarto analysis notebook:

    ```bash
    quarto render analysis.qmd
    ```

## References

### Articles

- Delano, J. (2023, March 13). *Recent Commonwealth Court case strengthens city's hand in going after tax-exempt hospitals.* CBS News Pittsburgh. <https://www.cbsnews.com/pittsburgh/news/commonwealth-court-case-strengthens-city-going-after-tax-exempt-hospitals/>
- Goldberg, L. (2018, January 25). *Loyola to pony up $270 million for MacNeal Hospital.* Crain's Chicago Business. <https://www.chicagobusiness.com/article/20180125/NEWS03/180129932/loyola-to-pony-up-270-million-for-macneal-hospital>
- Inklebarger, T. (2018, March 6). *Oak Park taxes could rise if hospital sold to nonprofit.* Oak Park. <https://www.oakpark.com/2018/03/06/oak-park-taxes-could-rise-if-hospital-sold-to-nonprofit/>
- Lown Institute. (2025, April). *Making the Hospital Tax Exemption Work for Communities.* <https://lownhospitalsindex.org/wp-content/uploads/2025/04/fair-share-property-brief-2025-20250409.pdf>
- Loyola Medicine. (2017, October 10). *Loyola Medicine to Acquire MacNeal Hospital and its Affiliated Operations.* <https://www.loyolamedicine.org/newsroom/press-releases/loyola-medicine-acquire-macneal-hospital-and-its-affiliated-operations>
- McFarlane, L. (2026, January 8). *CHS, Tenor waiting on Pa. Department of Health OK before they finalize NEPA hospital sale.* WVIA News. <https://www.wvia.org/news/local/2026-01-08/chs-tenor-waiting-on-pa-department-of-health-to-finalize-sale-of-three-nepa-hospitals>
- SEIU HCILIN. (2018, February). *The Real Impact of the MacNeal Hospital Sale to Loyola Medicine.* <https://seiuhcilin.org/2018/02/the-real-impact-of-the-macneal-hospital-sale-to-loyola-medicine/>

### Datasets

- Centers for Medicare & Medicaid Services. (2023). Hospital 2552-2010 Form Cost Reports. <https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/hospital-2552-2010-form>
- Centers for Medicare & Medicaid Services. (2026). Hospital Change of Ownership (CHOW) Data. <https://data.cms.gov/>
- Cook County Assessor's Office. (2023). Property Tax Data via Socrata API. <https://datacatalog.cookcountyil.gov/>
- Cook County Assessor's Office. (2023). PTAXSIM. <https://github.com/ccao-data/ptaxsim>
- Health Resources & Services Administration. (2025). 340B OPAIS Database. <https://340bopais.hrsa.gov/home>
