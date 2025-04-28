# How much does it cost to give birth in the United States?

## Overview

This project uses hospital [price transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency)
data to analyze the commercial cost of childbirth in the United States.
It finds clear price patterns based on geography, type of care, payer,
and other factors. It suggests that national or regional analyses
using price transparency data are possible but difficult, and highlights
some relevant data and methodological challenges.

## Data used

This analysis primarily uses 551,477 negotiated rates exported from the
Turquoise Health hospital rates database. It uses hospital rates (instead of
payer rates) to simplify the analysis and because hospital rates should be
broadly representative of the cost of delivery in the U.S. (since most births
[occur in hospitals](https://pmc.ncbi.nlm.nih.gov/articles/PMC6642827/)).

The following additional data sources are used:

- Policy Reporter data, which is used to weight different payers when
  aggregating to geographies like states. Included as a column in the main
  rates data extract.
- Census Bureau [TIGER/Line shapefiles](https://www2.census.gov/geo/tiger/).
  Used for creating maps of states and ZIP codes. Loaded via the `tigris` R
  package.
- Census Bureau [2023 5-year ACS estimates](https://api.census.gov/data/2023/acs/acs5).
  Used as additional context for/comparison to pricing data. Included via
  columns in the main rates data extract.
- [NCHS Urban-Rural Classification Scheme for Counties](https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html).
  Used to classify hospitals as urban or rural for driving distance weightings.
- [OpenTimes](https://opentimes.org/) (driving times) between U.S. ZIP codes.
  Used to create a detailed map of hospital prices.

See [Replication](#replication) for instructions on how to download and use
the data necessary to replicate this project.

## Scope

### Codes and modifiers

The analysis focuses on prices for 12 [MS-DRGs](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software)
related to childbirth/delivery, including:

| Code    | Procedure                                  | MCC                | CC                 |
|---------|--------------------------------------------|--------------------|--------------------|
| **783** | Cesarean section with sterilization        | :white_check_mark: | :x:                |
| **784** |                                            | :x:                | :white_check_mark: |
| **785** |                                            | :x:                | :x:                |
| **786** | Cesarean section without sterilization     | :white_check_mark: | :x:                |
| **787** |                                            | :x:                | :white_check_mark: |
| **788** |                                            | :x:                | :x:                |
| **796** | Vaginal delivery with sterilization/D&C    | :white_check_mark: | :x:                |
| **797** |                                            | :x:                | :white_check_mark: |
| **798** |                                            | :x:                | :x:                |
| **805** | Vaginal delivery without sterilization/D&C | :white_check_mark: | :x:                |
| **806** |                                            | :x:                | :white_check_mark: |
| **807** |                                            | :x:                | :x:                |

**MCC** = Major Complications or Comorbidities
**CC** = Complications or Comorbidities

The main focus is on MS-DRG 807 - vaginal delivery without sterilization and/or
dilation and curettage - since it represents the most common type of inpatient
birth.

Some hospitals use exclusively APR-DRGs, rather than MS-DRGs. In such cases,
APR-DRGs are translated to an equivalent MS-DRG:

- `APR-DRG 560-1` becomes `MS-DRG 807`
- `APR-DRG 560-4` becomes `MS-DRG 805`

Rates with billing code modifiers and revenue codes are included in the
analysis, but only if they are used in conjunction with an MS-DRG.
Hospitals that use *only* revenue codes for their inpatient billing
(e.g. a generic obstetrics room/bed rate) are excluded.

### Providers

Only `Short Term Acute Care` and `Critical Access` hospitals are included
in the rates sample. Hospitals must currently exist in the Turquoise
Health hospital data and report a price for *at least one* delivery-related
DRG to be included.

Many providers post price transparency data but do not include delivery DRGs,
as they aren't one of the CMS
[70 standard shoppable services](https://www.cms.gov/files/document/steps-making-public-standard-charges-shoppable-services.pdf).
Such providers are excluded from this analysis.

Other providers report only a subset of delivery DRGs (e.g. Catholic hospitals
often do not report prices for DRGs involving sterilization (`MS-DRG 796`)).
These providers are included in the analysis, but may be excluded from certain
charts.

Doulas and other outpatient childbirth/delivery providers are excluded.

Overall, the analysis includes 2,534 unique providers, out of around 4,500
total acute care and critical access hospitals in the United States.

### Payers

Only inpatient commercial rates/payers are included. Exchange, VA, Medicaid,
and Medicare rates are ignored.

No specific commercial payers are excluded from the analysis.
However, note that payer rates are weighted by the number of lives the payer
covers in the state of the provider, which results in smaller payers having a
low impact on aggregated values (e.g. a provider's median rate).

### Plans

All plan types (PPO, HMO, EPO, etc.) are included, however they are not
aggregated equally. If a provider has multiple plans with the same payer,
the following logic applies:

- If the provider has a PPO *and* an HMO plan with the same payer, take the
  median of all PPO/HMO rates (drop the rest).
- If a provider has a PPO *or* an HMO plan with a payer, take the median
  of the PPO or HMO rates (drop the rest).
- If a provider has only non-PPO/HMO plans with a payer, take the median
  of all plan rates.

Additionally, plan names that include "exchange" or "indemnity" are excluded,
as these plans tend to have unusually low, unrepresentative rates.

### Contract types

To increase the sample size for the analysis, all contract types/methodologies
are included. Rates are transformed into a flat "case rate" using the following
logic:

- **Case rate** - Negotiated dollar amount taken as-is (no transformation)
- **Percent of total billed charges** - Negotiated percentage is multiplied by
  the DRG list price
- **Per diem** - Negotiated dollar amount is multiplied by CMS' geometric
  mean length of stay (GLOS) for each DRG
- **Estimated allowed amount** - Used as a fallback if no other rate
  types exist
- **Fee schedule** - Negotiated dollar amount taken as-is (no transformation)
- **Other** - Rate type isn't specified, but the negotiated dollar amount
  is taken as-is, if it exists.

If a provider has multiple rate types with a payer after
[aggregating to the provider-payer-plan-DRG level](#data-aggregation), then
the first rate type in the list is used, following the order above (e.g.
case rates are used first, then percent of total billed charges, etc.).

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is *not* comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

Data is pulled from the Turquoise Health hospital rates dataset using the
[SQL here](./queries/rates.sql). In addition to the [scope limitations](#scope)
described above, the following filters are applied:

- Only rates with an `Inpatient` setting are included
- Rates greater than $3,000 and less than $500,000 are included
- [Turquoise-flagged](https://turquoisehealth.zendesk.com/hc/en-us/articles/31190981752603-Outlier-Management-in-hospital-rates) outliers are removed
- % of TBC rates with a list price greater than $500K are removed
- % of TBC rates with a negotiated percentage greater than 110% are removed
- Per diem rates that are less than half the Medicare day rate (Medicare price
  / Average Length of Stay) are removed
- Rates that exceed the provider's list price are removed, as long as the list
  price is reasonable (between 60% and 1000% of the Medicare price)
- Rates must be between 60% and 1000% of the Medicare price, and the Medicare
  price must not be 0 or missing
- Rates with revenue codes must relate to an inpatient stay
  (i.e. have a revenue code starting with a 1 or 2). Additionally, only rates
  with revenue codes occurring more than 10 times in the dataset are included
  (some hospitals post a rate for every DRG/revenue code combination).
- Per diem rates that apply to days after the initial stay are removed.
- Plans with an `Other` contract method and a description containing "Medicare",
  "Medicaid", or "Tricare" are removed.

### Data aggregation

Rates are aggregated with the goal of creating a single rate per provider per
MS-DRG. To achieve this, the following steps are taken:

1. Collapse negotiated rates across all revenue codes associated
   with a provider-payer-plan-DRG combination, taking the mean of only
   `NULL` revenue code rates first (if there are any).
2. Collapse rates across all plans associated with a
   provider-payer-DRG combination, taking the median of PPO and HMO plans first
   (if there are any).
3. Collapse rates for a provider-DRG combination, taking the *weighted*
   median of rates from the previous step and weighting by payer market share
   (number of covered lives in the provider's state).
4. When aggregating the provider-level rates from the previous step, take the
   weighted median of rates for each DRG, weighting by the provider's total
   bed count.

The goal of weighting is to ensure that the largest payers, plans, and providers
have more impact on final aggregated rates.

### Assumptions and limitations

The nature of the data means that there are a number of assumptions necessary
to make the analysis workable. The analysis assumes that:

- The provider sample is roughly representative of all U.S. hospitals i.e.
  missingness is random. This is unlikely but difficult to verify, as the
  reasons for missingness are rarely clear.
- PPO/HMO rates are more common and more broadly representative than other
  plan types. This is likely true, as PPO/HMO plans are the most common
  commercial plan types in the U.S.
- Providers should be weighted by their total number of beds. This is a rough
  proxy for the size and utilization of the provider, but it's imperfect.
  Some providers may have a high number of beds but a low number of deliveries,
  or vice versa. Providers with the same number of beds may differ in quality
  and utilization.
- MS-DRG rates are comparable across hospitals and payers. The analysis performs
  cleaning and transformations to try to ensure this, but *many*
  outlier/inapplicable rates likely remain.

And the analysis has a number of limitations:

- It doesn't account for the patient mix at each hospital. It's possible that
  hospitals with higher rates are treating more uninsured or Medicare patients
  (cross-subsidization) and using high commercial rates to offset their
  costs.
- The analysis likely significantly understates the total complexity and cost of
  delivery care. For example, it doesn't account for the cost of antepartum or
  postpartum care, epidurals, or other common procedures.

## Replication

This analysis is intended to be fully reproducible. Replication data (the
results of running [`ingest.py`](./ingest.py)) are available via the ZIP file
below. Extract the ZIP file to the `data/` directory, install dependencies, and
run the [`analysis.qmd`](./analysis.qmd) notebook to reproduce the analysis.

**[Link to replication data](link to s3)**

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
    CENSUS_API_KEY=
    ```

2. Create a virtual environment and install the required Python packages:

    ```bash
    uv venv
    source .venv/bin/activate
    uv sync
    ```

3. Run the data ingest script:

    ```bash
    uv run ingest.py
    ```

## References

- Severn, C. (2024, September 11). *How much does it cost to give birth in the U.S.?.* STAT News. <https://www.statnews.com/2024/09/10/childbirth-cost-giving-birth-us-insurance-total>
- Dieleman, JL., Beauchamp, M., Crosby, SW., et al. (2025, February 14). *Tracking US Health Care Spending by Health Condition and County.* JAMA. <https://jamanetwork.com/journals/jama/article-abstract/2830568>
- Claxton, G., Cotter, L., Rakshit, S. (2025, February 25) *Challenges with effective price transparency analyses.* KFF. <https://www.healthsystemtracker.org/brief/challenges-with-effective-price-transparency-analyses/>
- Lo, J., Claxton, G., Wager, E., Cox C., Amin K. (2023, February 10) *Ongoing challenges with hospital price transparency.* KFF. <https://www.healthsystemtracker.org/brief/ongoing-challenges-with-hospital-price-transparency/>
- Raval, D., & Rosenbaum, T. (2018, June 15). *Why is Distance Important for Hospital Choice? Separating Home Bias from Transport Costs.* <https://www.ftc.gov/system/files/documents/reports/why-distance-important-hospital-choice-separating-home-bias-transport-costs/working_paper_335_revised.pdf>
- U.S. Census Bureau. 2023 TIGER/Line Shapefiles (machine readable data files). U.S. Department of Commerce. <https://www2.census.gov/geo/tiger>
- U.S. Census Bureau. American Community Survey 5-Year Estimates Subject Tables, Tables B01001, B19013, C27014, 2023, <https://data.census.gov>
- Snow, D. OpenTimes (Version 0.0.1) [Data set]. <https://github.com/dfsnow/opentimes>
