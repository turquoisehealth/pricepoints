# Illinois 340B

## Overview

The fight over the [340B drug pricing program](https://www.hrsa.gov/opa) is
heating up behind the scenes in Illinois, driven by intense lobbying and little
media scrutiny. The federal program, intended to support safety net hospitals
via discounted drug prices, has grown rapidly – over half of Illinois hospitals
now participate, including large academic medical centers in Chicago.

While designed to subsidize low-margin safety-net care, the program’s expansion
has introduced market distortions and incentives for hospital consolidation.
The proposed Illinois state bill protects the status quo, part of a broader
trend of states seeking to retain 340B funds. As the program grows, so does
the need for transparency, scrutiny, and honest policy evaluation.

## Data used

This analysis uses three primary data sources:

1. Around 75,000 aggregated negotiated rates and gross charges for outpatient
  drugs, sourced from the Turquoise Health "Clear Rates" database. Clear Rates
  joins hospital rates, payer rates, claims data, and more into a single
  "canonical rate" per provider, payer, and code. It also includes dose
  standardization for drugs. As such, the difficult work of choosing,
  cleaning, and standardizing rates is mostly performed outside this repo.
2. [CMS cost reports](https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports)
  for all Illinois hospitals (via HCRIS SAS files). This data source is used
  to retrieve net patient revenue, drug charges/costs, and disproportionate
  share percentages for each hospital.
3. The [340B OPAIS database](https://340bopais.hrsa.gov/home) is used to
  identify 340B entities, look at their growth over time, and identify contract
  pharmacy relationships with each entity.

The following additional data sources are used:

- Illinois' annual [Health Facilities Inventories Hospital Profiles](https://hfsrb.illinois.gov/inventories-data.html)
  dataset is used to retrieve charity care case counts for each hospital.
- A manually compiled legislative tracking flatfile that uses the
  [NACHC](https://www.nachc.org/wp-content/uploads/2025/06/06_20_25_nachc_state-level-340b-laws-and-legislation_tracker.pdf)
  tracker as a starting point.

See [Replication](#replication) for instructions on how to download and use
the data necessary to replicate this project.

## Scope

This analysis focuses primarily on the 340B program as it affects Illinois. It
draws from national and state-level datasets to examine the uptake of the
program, who benefits most from it, and how it's changing provider incentives.

### Time periods

The analysis focuses on two separate time periods:

1. Current data (2025) is used in the legislative map, plots about 340B
  program growth, and in basically any plot that doesn't include hospital
  financials.
2. Data from 2023 is used in all plots sourced from the CMS cost reports or
  Illinois hospital inventory data, as 2023 is the most recent complete year
  for those reports. Plots using 2023 data have clearly labeled legends or axes denoting the year.

### Providers

All `Short Term Acute Care`, `Critical Access`, and `Children's` hospitals in
Illinois are included in the overall analysis. Some plots exclude certain
hospitals that are missing specific data.

For example, Critical Access Hospitals do not typically report their
disproportionate share percentage on their CMS cost report, so they are
excluded from any plots featuring that stat.

In total, 210 unique hospitals are included in the analysis.

### Rates, plans, and payers

The analysis uses negotiated and gross rates for two main purposes:

1. For comparison of prices between 340B and non-340B hospitals
2. To estimate the gross-to-net factor used to calculate drug profits for
  each 340B hospital (see [Drug profit calculations](#drug-profit-calculations))

Only rates from commercial payers are included. Exchange, VA, Medicaid,
and Medicare Advantage rates are ignored. Medicare Part B rates are used
for reference only.

Only commercial PPO plan rates are included. HMO, EPO, and other plan types
are ignored.

Each negotiated/gross rate is unique at the payer-provider-network-code level.
When aggregating, individual rates are weighted by market share, prevalence,
and hospital size. See [Weighting](#data-aggregation-and-weighting) for more
details.

### Codes

The analysis includes common HCPCS for outpatient, hospital-administered drugs.
These are typically things like infused oncology drugs or injectables e.g.
[J-codes](https://www.hcpcsdata.com/Codes/J) like Keytruda. All included
codes must also have a Medicare ASP and an OPPS status indicator of G or K.

Only the top 500 (by claims volume) J-codes in Illinois are considered "common"
and included in the analysis. Of those, 190 unique codes have a sufficient
number of rates (and corresponding [weights](#data-aggregation-and-weighting))
for aggregation.

All rates are dose-standardized for comparability across different settings.

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is *not* comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

#### Rates data

Negotiate drug rates and gross charges are pulled from the Turquoise Health
Clear Rates database using the [SQL here](./queries/drug_rates.sql).
In addition to the [scope limitations](#scope) described above, the following
filters are applied:

- All [Turquoise-flagged outliers](https://cld.turquoise.health/components/accuracy/scores)
  are excluded (rate score must be >= 3). The flagging process finds rates that
  fall outside reasonable bounds determined by Medicare and claims data.
- Very high and low negotiated rates are excluded via hard bounds (must be
  between $1 and $250K).
- Gross charges from machine-readable files are standardized to the same dosage
  unit as the canonical rate to ensure comparability.
- Gross charges are filtered to be between 1% and 1000% of the Medicare Average
  Sales Price (ASP) to remove extreme outliers.

#### CMS cost reports

Data from Medicare cost reports is pulled for all Illinois hospitals included
in the analysis using the [SQL here](./queries/medicare_cost_reports.sql).
This data is used to retrieve key financial metrics like drug costs, drug
charges, net patient revenue, and Disproportionate Share Hospital (DSH)
percentages.

The SQL table is constructed from Hospital 2552-2010 form SAS files,
specifically the `hosp10-sas` export available [on the CMS site](https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/hospital-2552-2010-form).

Since hospitals can file multiple cost reports in a single year, only the most
recent report from 2023 is used for each hospital.

#### OPAIS data

The HRSA OPAIS database is used to identify 340B-participating hospitals, their
child sites, and their associated contract pharmacies. The daily export file
from 2025-06-13 was used to:

- Identify parent 340B entities and their initial qualification date.
- Count the number of child sites and contract pharmacies associated with each
  parent hospital.
- Join this information with the main hospital dataset to flag 340B status and
  quantify the scale of each hospital's 340B network.

Hospitals are considered 340B-eligible from their qualification date until
the present, assuming no termination date is specified.

### Data aggregation and weighting

To facilitate comparison, data is aggregated to the hospital level. Weights are
used to ensure that more commonly used drugs, larger payers, and larger
hospitals have a greater impact on final averages. The following aggregation
steps are performed:

1. Negotiated rates and gross charges, which are unique at the
  provider-payer-code level, are first aggregated to the provider-code level.
  This is done by calculating a weighted average, using each payer's market
  share in Illinois as the weight.
2. The provider-code averages are then further aggregated to the provider level.
  This rollup is also a weighted average, using state-level claims volume (`count_enc`) for each drug code as the weight.
3. For state-level statistics, the provider-level averages are finally
  aggregated using each provider's total bed count as the weight.

Note that altering or excluding these weights does not significantly
change the overall results (i.e. results are robust).

### Drug profit calculations

> [!NOTE]
> The analysis does not explicitly calculate 340B profits, since it isn't
> possible with publicly available data. Instead, it estimates outpatient drug
> profits for 340B hospitals, which is likely a close proxy.

This analysis estimates outpatient drug profits for Illinois hospitals using
financial data from their CMS cost reports and negotiated rates from the
Turquoise Health Clear Rates database. The profit calculation is based the
method outlined by [Health Data Atlas](https://healthdataatlas.com/2024/12/06/northside-hospitals-340b-drug-empire-would-make-marlo-stanfield-jealous/), but
modified to use an average gross-to-net ratio for the upper bound. Here's how it
works for each bound:

#### Lower bound

The method for calculating the lower bound is nearly the same as the one used by
Health Data Atlas:

1. Calculate the hospital's overall conversion rate from gross charges to net
  revenue using the CMS cost report data. This is done by dividing
  the total net patient revenue (Worksheet G-3, Column 1, Row 3) by the total
  hospital gross charges (Worksheet C, Part I, Columns 6+7, Row 202).

  $$
  \text{Conversion Factor} = \frac{\text{Total Net Patient Revenue}}{\text{Total Gross Charges}}
  $$

2. Multiply the conversion factor by the total outpatient drug charges
  (Worksheet C, Part I, Column 7, Row 73) to estimate the net revenue from
  outpatient drugs.

  $$
  \text{Net Revenue from Outpatient Drugs} = \text{Conversion Factor} \times \text{Total Outpatient Drug Charges}
  $$

3. Calculate the hospital's outpatient drug *costs* by multiplying the total
  drug cost by the percentage of total *charges* that are outpatient. Total
  costs are from Worksheet C, Part I, Column 5, Row 73. Charges are from
  Worksheet C, Part I, Columns 6 and 7, Row 73.

  $$
  \text{Outpatient Drug Costs} = \text{Total Drug Costs} \times \left(\frac{\text{Total Outpatient Drug Charges}}{\text{Total Drug Charges}}\right)
  $$

4. Finally, subtract the costs from the estimated net revenue to get the lower
  bound profit estimate.

This method makes two major assumptions:

- The hospital's overall gross-to-net conversion factor is a good lower bound
  for what the hospital receives for outpatient drugs.
- The ratio of outpatient drug charges to total drug charges is similar to that
  same ratio for costs i.e. if 70% of a hospital's drug charges are outpatient,
  then 70% of its drug costs are also outpatient.

#### Upper bound

Health Data Atlas uses a margin number pulled from the literature to calculate
the upper bound of profits, but we swap that for a more data-driven approach
using the following steps:

1. Calculate each hospital's average gross-to-net ratio for outpatient drugs
  using pairs of negotiated rates and gross charges. This is done by dividing
  the negotiated rate by the gross charge for each drug code, then taking the
  weighted average across all drug codes for each hospital. The weights are
  based on the number of claims for each drug code.

  $$
  \text{Gross-to-Net Ratio} = \frac{\sum_i (\text{Claim Count}_i \times \frac{\text{Negotiated Rate}_i}{\text{Gross Charge}_i})}{\sum_i \text{Claim Count}_i}
  $$

2. Follow the same steps as the lower bound to estimate the net revenue from
  outpatient drugs, but use the hospital's average gross-to-net ratio instead
  of the overall conversion factor.

3. Calculate the hospital's outpatient drug costs as before, using the total
  drug costs and the ratio of outpatient drug charges to total drug charges.

4. Finally, subtract the costs from the estimated net revenue to get the upper
  bound profit estimate.

The major assumption here is that an average gross-to-net ratio derived from
rates will be representative of the hospital's actual outpatient gross-to-net
ratio. Given the constrained sample of rates available, this may not hold true.
The [billing codes included](#codes) are only for J-codes, but hospitals
administer many other outpatient drugs that are not included in this analysis.
As such, the calculated gross-to-net ratio may not fully capture the hospital's
true return on outpatient drugs.

To improve representativeness, each hospital must have at least 20 gross-net
pairs to be included in the upper bound calculation. If a hospital has fewer
than 20 pairs, it is assigned the average gross-to-net ratio of similarly-sized
hospitals (i.e. those with the same number of beds).

### Assumptions and limitations

- **Rate representativeness** - The analysis assumes that the available
  negotiated rates for J-codes are representative of the broader market for
  hospital-administered drugs in Illinois.
- **Gross-to-net method** - The gross-to-net factors used to
  [estimate drug profits](#drug-profit-calculations) are assumed to be roughly
  representative each hospital's outpatient drug reimbursement structure. This
  is difficult to verify and may not hold true for all hospitals, especially
  those with a small sample of rates.
- **Profit calculations over time** - The current (2025) gross-to-net
  factors are applied backwards in time to estimate historical profits. This
  assumes that the gross-to-net relationship has remained relatively stable,
  which may not be true for all hospitals or drugs.
- **Community benefit** - The analysis uses charity care as the main metric for
  community benefit, but this is not the only form. Hospitals provide other
  uncompensated care and community services that are not captured here. Other
  forms of 340B spending, such as cross-subsidization of other lines of
  business, are also not considered.
- **340B profits** - The analysis estimates outpatient drug profitability; it
  does not isolate profits specifically attributable to the 340B program.
- **Data recency** - Most of the hospital financial data is from 2023, with some
  2025 data from legislative tracking and OPAIS. As such, the analysis
  necessarily does not capture the current state of the 340B program in
  Illinois, which is rapidly evolving.

## Replication

This analysis is intended to be fully reproducible. Replication data (the
results of running [`ingest.py`](./ingest.py)) are available via the ZIP file
below. Extract the ZIP file to the `data/` directory, install dependencies, and
run the [`analysis.qmd`](./analysis.qmd) notebook to reproduce the analysis.

#### [Link to replication data](https://drive.google.com/uc?export=download&id=1laYO2FUzfx-VQN6-V9apFPqQC1qGL0ru)

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

### Articles and papers

- Apexus. (n.d.). *Understanding the Medicare Cost Report.* <https://www.340bpvp.com/Documents/Public/340B%20Tools/understanding-the-medicare-cost-report.pdf>
- Asplund, J. (2025, May 23). *Pharmas push back as deadline looms for contentious drug discount bill.* Crain's Chicago Business. <https://www.chicagobusiness.com/health-pulse/drugmakers-push-back-illinois-340b-drug-discount-bill>
- Blalock, E. (2024, March). *Rural Referral Center Participation in the 340B Drug Discount Program.* BRG. <https://media.thinkbrg.com/wp-content/uploads/2024/03/22110813/BRG_340B-Rural-Referral-Center-White-Paper_March2024.pdf>
- Courtemanche, C. Garuccio, J. *What Economists Should Know about the 340B Drug Discounting Program.* IZA Institute of Labor Economics. <https://docs.iza.org/dp17880.pdf>
- Cuban, M. (2025, June 9). *Via Twitter/X.* <https://x.com/mcuban/status/1932250748819316755?t=JYpjue40-2ZQXVnT3CSTCA&s=19>
- Fein, A. (2024, October 22). *The 340B Program Reached $66 Billion in 2023—Up 23% vs. 2022: Analyzing the Numbers and HRSA’s Curious Actions.* Drug Channels. <https://www.drugchannels.net/2024/10/the-340b-program-reached-66-billion-in.html>
- Fein, A. (2025, May 6). *Follow the 340B Dollar: Senator Cassidy Exposes How CVS Health and Walgreens Profit as 340B Contract Pharmacies.* Drug Channels. <https://www.drugchannels.net/2025/05/follow-340b-dollar-senator-cassidy.html>
- Gabler, E. (2025, January 15). *How a Company Makes Millions Off a Hospital Program Meant to Help the Poor.* The New York Times. <https://www.nytimes.com/2025/01/15/us/340b-apexus-drugs-middleman.html>
- Health Resources & Services Administration. *2023 340B Covered Entity Purchases.* <https://www.hrsa.gov/opa/updates/2023-340b-covered-entity-purchases>
- Madden, B. (2024, December 5) *340B: Breaking down the most misunderstood healthcare program in history.* Hospitalogy. <https://hospitalogy.com/articles/2024-12-05/340b-breakdown/>
- Mathews, A. W. Overberg, P. Walker, J. McGinty, T. (2022, December 20). *Many Hospitals Get Big Drug Discounts. That Doesn’t Mean Markdowns for Patients.* The Wall Street Journal. <https://www.wsj.com/articles/340b-drug-discounts-hospitals-low-income-federal-program-11671553899>
- National Association of Community Health Centers. (2025, June 20). *State-Level Laws to Protect CHCs’ 340B Savings.* <https://www.nachc.org/wp-content/uploads/2025/06/06_20_25_nachc_state-level-340b-laws-and-legislation_tracker.pdf>
- Newton, William. (2025, May 20). *California Assembly Passes 340B Contract Pharmacy Protections for Grantees, Sends Bills to Senate.* 340B Report. <https://340breport.com/california-assembly-passes-340b-contract-pharmacy-protections-for-grantees-sends-bills-to-senate/>
- Schencker, L. (2023, January 10). *UChicago Medicine finalizes deal to acquire controlling interest in four west suburban hospitals.* The Chicago Tribune. <https://www.chicagotribune.com/2023/01/10/uchicago-medicine-finalizes-deal-to-acquire-controlling-interest-in-four-west-suburban-hospitals/>
- Senate Majority Staff Report. (2025, April). *Congress Must Act to Bring Needed Reforms to the 340b Drug Pricing Program.* <https://www.help.senate.gov/imo/media/doc/final_340b_majority_staff_reportpdf1.pdf>
- Stratton, M. (2024, December 6). *Northside Hospital’s (340b) Drug Empire Would Make Marlo Stanfield Jealous.* Health Data Atlas. <https://healthdataatlas.com/2024/12/06/northside-hospitals-340b-drug-empire-would-make-marlo-stanfield-jealous/>
- Talamonti, J. (2025, May 30). *Industry groups take sides as lawmakers consider discount drug program bill.* Advantage News. <https://www.advantagenews.com/news/local/industry-groups-take-sides-as-lawmakers-consider-discount-drug-program-bill/article_7dbcd8ba-c4f8-4f8c-9194-1cfd21abc5e4.html>
- Thomas, K. Silver-Greenberg, J. (2022, September 27). *How a Hospital Chain Used a Poor Neighborhood to Turn Huge Profits.* The New York Times. <https://www.nytimes.com/2022/09/24/health/bon-secours-mercy-health-profit-poor-neighborhood.html>
- Thomas, S. Schulman, K. (2020, Mar 1). *The unintended consequences of the 340B safety‐net drug discount program.* Health Services Research. <https://doi.org/10.1111/1475-6773.13281>
- USC Schaeffer Center for Health Policy & Economics. (2024, October). *The 340B Drug Pricing Program: Background, ongoing challenges, and recent developments.* <https://healthpolicy.usc.edu/research/the-340b-drug-pricing-program-background-ongoing-challenges-and-recent-developments/>

### Datasets

- Centers for Medicare & Medicaid Services. (2023). Cost Reports by Fiscal Year. <https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/cost-reports-fiscal-year>
- Health Resources & Services Administration. (2025). 340B OPAIS Database. <https://340bopais.hrsa.gov/home>
- Illinois Health Facilities and Services Review Board. (2023). Annual Hospital Questionnaire/Profiles. <https://hfsrb.illinois.gov/inventories-data.html>
