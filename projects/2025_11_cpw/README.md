# Cost Plus Wellness

## Overview

This project analyzes the reimbursement rates and network size of
[Cost Plus Wellness (CPW)](https://www.costpluswellness.com/), a new employer
network/company from Mark Cuban. The project primarily compares
[Baylor Scott & White (BSWH)](https://www.bswhealth.com/) rates negotiated by
Cost Plus Wellness to the equivalent rates from other major Texas payers.
The final piece [can be found on Substack](https://www.pricepoints.health/p/cpw).

## Data used

This analysis compares data from two main sources:

1. The Baylor Scott & White
  [institutional](https://www.costpluswellness.com/contracts/baylor-scott-and-white-facilities)
  and [professional](https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional)
  rates posted to the Cost Plus Wellness website.
2. Around 800K equivalent rates from other major payers in Texas, drawn from
  [Price Transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency)
  (PT) and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC)
  data.

Rates from other major payers are extracted from the Turquoise Health "Clear Rates"
database, which joins hospital and payer machine-readable files to create a single
"canonical rate" per provider, payer, and code.

The following additional data sources are used:

- Provider and payer metadata from Turquoise Health is used to filter and weight
  facilities (e.g., bed counts, facility types) and payers (e.g., market share).
- Claims data is used to weight rates by utilization when aggregating. Codes that
  are more commonly used have a greater impact on final averages.

See [Replication](#replication) for instructions on how to download and use
the data necessary to replicate this project.

## Scope

### Codes

The analysis only includes billing codes from the Cost Plus Wellness rate sheets
for Baylor Scott & White, which cover:

- **[Institutional rates](https://www.costpluswellness.com/contracts/baylor-scott-and-white-facilities)**: Mix of MS-DRGs (inpatient) and HCPCS codes (outpatient)
  spanning a wide range of procedures from hospital stays to outpatient services
- **[Professional rates](https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional)**: HCPCS codes covering physician services, split between
  facility and non-facility settings

Each BSWH billing code must have a rate from at least one non-CPW payer to be
included. Note that CPW rates from BSWH are identical across the entire BSWH
system (i.e. the same at every location), while rates from other payers vary
by location/provider.

### Providers

For **institutional rates**, only Baylor Scott & White facilities listed
in the Cost Plus Wellness contract are included. These are matched to other
payers' rates using facility EINs.

For **professional rates**, only Baylor Scott & White physician groups
listed in the Cost Plus Wellness contract are included. These are matched to
other payers' rates using provider NPIs.

For the **network size comparison**, the analysis counts the facilities in the
Dallas-Fort Worth-Arlington, TX CBSA (Core Based Statistical Area) with rates
from major Texas payers and compares that to the count of facilities listed
on the [Cost Plus Wellness provider directory](https://www.costpluswellness.com/directory).

### Payers

To simplify the analysis, only the following major Texas payers are included
in the comparison to Cost Plus Wellness:

- Blue Cross Blue Shield of Texas
- United Healthcare
- Cigna
- Aetna
- Baylor Scott & White Health Plan

These are essentially the BUCA payers plus BSWH's own health plan,
and represent the vast majority of covered lives in the state.

### Plans

Only each payer's main commercial PPO plan is included. HMO and other plan types
are excluded to ensure comparability with Cost Plus Wellness rates.

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is *not* comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

#### Cost Plus Wellness rates

Cost Plus Wellness rates were manually extracted from PDF rate sheets published
on their website using Tabula, with light hand-editing to clean up formatting
issues. The institutional and professional rate sheets were processed separately:

- [Institutional rates](https://www.costpluswellness.com/contracts/baylor-scott-and-white-facilities)
  include both inpatient and outpatient facility rates
- [Professional rates](https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional)
  include physician service rates with separate facility and non-facility amounts

Provider lists (EINs for facilities, NPIs for physician groups) were also
extracted from the CPW contract PDFs.

#### Comparison rates

Negotiated rates for Baylor Scott & White providers from other major Texas
payers are pulled from the Turquoise Health Clear Rates database using SQL
queries in the `queries/` directory:

- [bswh_inst_rates.sql](./queries/bswh_inst_rates.sql) for institutional rates
- [bswh_pro_rates.sql](./queries/bswh_pro_rates.sql) for professional rates
- [dfw_provider_counts.sql](./queries/dfw_provider_counts.sql) for network size
  comparison

The following filters are applied to ensure data quality:

- Only rates directly sourced from MRFs are used (no imputed rates)
- Only commercial PPO plan rates are included
- Rates must match CPW provider EINs/NPIs
- Professional rates must match on both billing code and facility/non-facility
  setting

### Data aggregation and weighting

To facilitate comparison, data is aggregated using weighted quantiles. The
following weights are used:

1. **Claims utilization**: Each billing code is weighted by its relative
   utilization in Texas claims data. More commonly used codes have a greater
   impact on final statistics.
2. **Hospital size**: For institutional rates, each provider's rates are
   weighted by total bed count to ensure larger hospitals have appropriate
   influence. Professional rates are not weighted by bed count.
3. **Payer market share**: When aggregating across payers, rates are weighted
   by each payer's market share in Texas to ensure the comparison reflects
   what most patients would see.

For network size analysis, provider counts are simple tallies without weighting.

### Assumptions and limitations

The nature of the data means that there are a number of assumptions necessary
to make the analysis workable:

- **Rate representativeness**: The analysis assumes that Baylor Scott & White
  rates are roughly representative of Cost Plus Wellness's broader pricing
  strategy. CPW may have different rate structures with other provider systems.
- **Medicare baseline**: The analysis uses Medicare rates as a baseline for
  comparison. These rates are provider and code-specific, sourced from Turquoise
  Health data. For CPW rates, Medicare rates are attached from Turquoise
  Health's provider-adjusted Medicare pricing data.
- **Network coverage**: The provider count comparison uses Transparency in
  Coverage data for other payers, which may not perfectly reflect actual network
  breadth or in-network status at any given time.
- **Plan comparability**: The analysis assumes PPO plan rates are generally
  comparable across payers, though different plans may have different benefit
  designs that affect out-of-pocket costs.

And the analysis has a number of limitations:

- **Limited provider scope**: Only Baylor Scott & White providers in Texas
  are analyzed. CPW's rates with other professional providers aren't compared.
- **Sample selection**: Only rates that can be matched between CPW and other
  payers are included, which may introduce selection bias.
- **Point-in-time**: Rates reflect published data as of September 2025 and
  may not reflect later pricing or contract terms.

## Replication

This analysis is intended to be fully reproducible. Replication data (the
results of running the ingestion scripts) are available via the ZIP file
below. Extract the ZIP file to the `data/` directory, install dependencies, and
run the [`analysis.qmd`](./analysis.qmd) notebook to reproduce the analysis.

#### [Link to replication data](https://drive.google.com/uc?export=download&id=1MWsvlO6N5D6MK3AkirPIjpg5wkFyLtP2)

### Dependencies

For analysis, assuming you've downloaded the replication data, open R/RStudio
and run:

```r
install.packages("renv")
renv::restore()
```

This will install all required R packages and dependencies. Depending on your
system, you may need to install additional system dependencies (e.g. GDAL).

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
    ```

2. Create a virtual environment and install the required Python packages:

    ```bash
    uv venv
    source .venv/bin/activate
    uv sync
    ```

3. Run the data ingest scripts:

    ```bash
    uv run ingest_inst.py  # For institutional rates
    uv run ingest_pro.py   # For professional rates
    uv run ingest.py       # For network size data
    ```

## References

- Cost Plus Wellness. (2025). *Baylor Scott & White - Facilities.* <https://www.costpluswellness.com/contracts/baylor-scott-and-white-facilities>
- Cost Plus Wellness. (2025). *Baylor Scott & White - Professional.* <https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional>
- Cost Plus Wellness. (2025). *Provider Directory.* <https://www.costpluswellness.com/directory>
