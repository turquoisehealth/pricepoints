# Rural v Urban Prices

## Overview

This project uses a large national sample of aggregated, cleaned [Price Transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency) (PT) and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC) data to compare rural and urban hospital prices in the United States. Specifically, it uses hospital rates from the Turquoise Health Clear Rates database to compare commercial PPO reimbursement trends across U.S. counties classified by their rurality. The full written piece can be found on [Substack](https://www.pricepoints.health/p/rural_v_urban).

## Data used

This analysis primarily uses \~92M commercial PPO rates negotiated between providers and insurers. The rates were exported from the Turquoise Health Clear Rates database, which collapses hospital rates, payer rates, claims data, and more into a single "canonical rate" per provider, payer, and code. Clear Rates also standardizes payment and contract methods by converting most rates into a flat rate per code, vastly expanding the number of comparable rates. Note however, that the \~92M rates represent a small, high-confidence subset of the larger Clear Rates database.

The following additional data sources are used:

- Policy Reporter data, which is an online subscription commercial and government coverage policy database containing the number of covered lives per policy/payer. This data is used to weight payers when aggregating to the state level. Low market share payers are downweighted to make sure rates are representative of what most people will see on their bills.
- Census Bureau [TIGER/Line shapefiles](https://www2.census.gov/geo/tiger/). Used for creating maps of states and counties. Loaded via the `tigris` R package.
- [NCHS Urban-Rural Classification Scheme for Counties](https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html). Main classification scheme used to classify hospitals as urban or rural for this analysis.
- Various [USDA urban-rural classification codes](https://www.ers.usda.gov/topics/rural-economy-population/rural-classifications), including RUCAs, RUCCs, and Urban Influence codes. These are used to compare to and validate the NCHS classification scheme.
- [OpenTimes](https://opentimes.org/) (driving times) between U.S. ZIP codes. Used to create a detailed map of hospital prices.
- The [340B OPAIS database](https://340bopais.hrsa.gov/home) is used to identify 340B entities, specifically current Critical Access Hospitals (CAHs).

Replication data is *not* provided for this analysis for two reasons:

1. The size of the rate slice needed for replication is very large, such that sharing it would be both prohibitively cumbersome and harmful to Turquoise's business.
2. Extracts created by `ingest.py` include third-party, non-Turquoise data that we're not allowed to share publicly.

## Scope

### Geography

The main unit of analysis for this project is the U.S. county. Counties are classified as rural or urban using the [NCHS Urban-Rural Classification Scheme](https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html). Counties with NCHS codes 5 or 6 are classified as rural, while counties with codes 1-4 are classified as urban. This cutoff follows the standard OMB metro/non-metro classification of counties. Additional classification schemes (USDA RUCCs, RUCAs, and Urban Influence codes) are used for validation and comparison purposes.

Turquoise mostly uses pre-2022 county names in its `county` field. However, some county names changed in 2022 (especially in Connecticut), so a crosswalk and lookup is used to match 2023+ Census counties to Turquoise providers. See `data/input` for the relevent crosswalks.

Some counties do not have any providers or are missing providers. This is especially noticeable on the county-level price map. For the price map only, counties with no providers are filled using the weighted median of providers reachable within an hour's drive (from the county center). This is mostly to provide a smoothing effect for the map and doesn't impact the overall conclusions of the piece.

### Providers

All `Short Term Acute Care`, `Rehabilitation`, `Children's`, and `Critical Access` hospitals with commercial PPO rates from the Turquoise Health Clear Rates database are included. Providers must have at least 500 rates across all billing codes (at the payer-provider level) to be included in the main analysis. For code-specific analysis (i.e. plotting by service line), providers must have at least 4 rates per billing code (at the provider-code level) to be included.

### Payers and plans

Only commercial PPO rates are included in this analysis. Medicare Advantage, Exchange, VA, Medicaid, and traditional Medicare rates are excluded. Payers are weighted by their state-level market share (number of covered lives) from Policy Reporter data. Payers with missing market share data are assigned a minimal weight of 0.01 (1st percentile). The analysis includes 51 of the largest payers in the U.S., including all BUCAH payers.

Medicare rates are used as a benchmark for comparison (i.e., rates are expressed as a percentage of Medicare). Medicare rates are derived from Turquoise's internal Medicare rate database and are typically provider-adjusted (e.g. for DSH percentage).

> [!NOTE]
> This analysis has an important limitation in that it uses estimated Medicare fee-for-service rates as a benchmark even in cases where those rates may not apply. For example, Critical Access Hospitals (CAHs) are reimbursed by Medicare at 101% of cost (99% with sequestration), rather than at a fixed rate. However, the cost reimbursement data is unavailable to Turquoise, so we fallback to the fee-for-service rates.
>
> As a result of the cost-based reimbursement, CAH reimbursement is probably *higher* in reality than the rates used in this analysis. That means the denominator in the "percentage of Medicare" calculation is artificially low. This means that the reported commercial-to-Medicare ratios for CAHs are likely *overstated*, and rural hospitals may be making even less as a percentage of Medicare than reported here.

### Codes

The analysis includes both HCPCS (Level 1, i.e. CPT codes) and MS-DRG billing codes. HCPCS codes must be 5-digit numeric codes, while MS-DRGs are included without restriction. Drug codes and device codes are excluded. Rates must be between 40% and 1000% of the Medicare rate to be included. A subset of representative codes is also used for certain analyses, including common HCPCS procedures (e.g., 27130, 29881, 71046, 73720, 77066) and MS-DRGs (e.g., 177, 195, 280, 291, 460, 470, 743, 788, 807, 871).

Codes are weighted according to their state-level utilization, as derived from claims data from 2024. Codes with higher utilization receive higher weights when aggregating rates across codes. Codes with missing utilization data are assigned a minimal weight of 0.01 (1st percentile).

### Time periods

This analysis uses the most recent available rates from the Turquoise Health Clear Rates database (v2.2.0), which includes rates mostly from spring and summer of 2025. Claims utilization data is from 2024. Census population and income data are from the 2023 5-year ACS estimates. NCHS urban-rural classification codes are from the 2023 vintage. USDA codes are the most recent available vintage (2023 for RUCCs and UICs, 2020 for RUCAs). Driving times are from the most recent OpenTimes dataset (2023).

## Methods

### Data ingest and cleaning

Data is extracted from the Turquoise Health Clear Rates database using two SQL queries ([rates.sql](./queries/rates.sql) and [rates_code.sql](./queries/rates_code.sql)). The queries perform the following key steps:

1. **Rate filtering**: Only commercial PPO rates from hospitals are included. Rates must have a canonical rate score of 3 or higher, which means high-confidence rates that have been validated with Medicare benchmark prices. Rates must also fall between 40% and 1000% of Medicare rates.
2. **Provider filtering**: Only Short Term Acute Care, Rehabilitation, Childrens, and Critical Access hospitals are included.
3. **Code filtering**: HCPCS codes must be 5-digit numeric codes. MS-DRGs are included without restriction. Drug and device codes are excluded.
4. **Payer market share weighting**: Policy Reporter data provides state-level covered lives per payer, which is converted to a 0-1 weight using percentile ranks within each state. Payers with missing market share are assigned a weight of 0.01.
5. **Code utilization weighting**: Claims data from 2024 is used to calculate state-level percentile ranks for billing codes based on utilization frequency. Codes with missing utilization data are assigned a percentile of 0.01.

After extraction, data is joined with several additional datasets in [ingest.py](./ingest.py):

- County FIPS codes are matched to Turquoise state/county names using a manual crosswalk
- NCHS, RUCC, UIC, and RUCA urban-rural classification codes are joined by county FIPS
- 340B OPAIS data is joined by Medicare ID to identify Critical Access Hospitals
- Census population and income data are joined by county
- OpenTimes county-to-county driving times are extracted for counties within 1 hour of each other

### Data aggregation and weighting

The analysis uses two different aggregation approaches depending on the query:

**Provider-level aggregation** ([rates.sql](./queries/rates.sql)):

1. Aggregate from payer-provider-code level to payer-provider level, taking weighted means of rates across codes (weighted by code utilization percentiles)
2. Aggregate from payer-provider level to provider level, taking weighted means of rates across payers (weighted by payer market share)
3. When aggregating across providers (e.g., to geography or classification), take weighted means/medians using provider bed counts as weights

**Code-level aggregation** ([rates_code.sql](./queries/rates_code.sql)):

1. Aggregate from payer-provider-code level to provider-code level, taking weighted means of rates across payers (weighted by payer market share)
2. Aggregate from provider-code level to geography-code or classification-code level using custom weighting in Polars (e.g., weighted by provider bed counts)

Both approaches require a minimum number of rates at each level (500 for provider-level, 4 for code-level) to reduce noise.

### Assumptions and limitations

The analysis assumes that:

- The provider sample is roughly representative of all U.S. hospitals, though systematic missingness is likely and difficult to verify
- PPO rates are broadly representative of commercial insurance rates across all plan types
- Provider bed counts are a reasonable proxy for provider size and utilization
- County-level urban-rural classifications accurately reflect the characteristics of providers within those counties (i.e. an "urban" county's hospitals are all urban in character)
- Payer market share at the state level is a reasonable weight for aggregating rates across payers
- Provider-adjusted Medicare rates accurately represent what a hospital would receive and are not systematically biased or wrong in a way that would change the conclusions of the project

Key limitations include:

- **Selection bias**: Only providers that report price transparency data and meet minimum rate thresholds are included, which may systematically exclude certain types of providers (e.g. the very smallest rural hospitals that only use revenue codes)
- **Missing context**: The analysis does not account for patient mix, payer mix, hospital quality, local market dynamics, hospital ownership, or a host of other factors. These factors may confound the relationship between rurality and prices
- **Rate comparability**: Despite extensive cleaning, rates may not be directly comparable across providers due to differences in contract structures, billing practices, and coding
- **Geographic resolution**: County-level classifications may not capture intra-county variation in rurality, particularly for large or diverse counties

## References

### Articles, videos, and papers

- Abelson, R. (2020, September 18). Many Hospitals Charge More Than Twice What Medicare Pays for the Same Care. The New York Times. <https://www.nytimes.com/2020/09/18/health/covid-hospitals-medicare-rates.html>
- Bai, G. (2025). Sharp rise in urban hospitals with rural status in Medicare, 2017–23. Health Affairs, 44(8), 963–969. <https://doi.org/10.1377/hlthaff.2025.00019>
- Centers for Medicare & Medicaid Services. (2025, August). Information for Critical Access Hospitals (MLN006400). <https://www.cms.gov/files/document/mln006400-information-critical-access-hospitals.pdf>
- Hammerslag, L. R., & Talbert, J. (2023, May 17). Differences in Insurance-Negotiated Prices in Rural Hospitals [poster]. National Rural Health Association Annual Conference. <https://ruhrc.uky.edu/assets/Hammerslag_NRHA-poster-negotiated-pricing.pdf>
- Levinson, L. Hulver, S. Godwin, J. Neuman, T. (2025, April 16). 10 Things to Know About Rural Hospitals. KFF. <https://www.kff.org/health-costs/10-things-to-know-about-rural-hospitals/>
- Levinson, L. Hulver, S. Godwin, J. Neuman, T. (2025, October 1). Key Facts About Hospitals. KFF. <https://www.kff.org/health-costs/key-facts-about-hospitals/>
- Markey, E. J., Wyden, R., Merkley, J. A., & Schumer, C. E. (2025, June 12). Letter on rural hospitals. <https://www.markey.senate.gov/imo/media/doc/letter_on_rural_hospitals.pdf>
- Mullens, C. L., Mead, M., Lee, J. D., Probst, J. C., Dimick, J. B., & Ibrahim, A. M. (2025). Negotiated Prices for Care at Independent and System-Affiliated Rural Hospitals. JAMA Network Open. <https://doi.org/10.1001/jamanetworkopen.2025.16188>
- Orozco Rodriguez, J. (2025, June 12). ‘One Big Beautiful Bill’ Would Batter Rural Hospital Finances, Researchers Say. KFF Health News. <https://kffhealthnews.org/news/article/rural-hospitals-battered-by-big-beautiful-bill-researchers/>
- Quealy, K. and Sanger-Katz, M. (2015, December 15). The Experts Were Wrong About the Best Places for Better and Cheaper Health Care. The New York Times. <https://www.nytimes.com/interactive/2015/12/15/upshot/the-best-places-for-better-cheaper-health-care-arent-what-experts-thought.html>
- Rural Health Information Hub. (2024, December 10). Critical Access Hospitals (CAHs). <https://www.ruralhealthinfo.org/topics/critical-access-hospitals/organizations>
- Tribble, S. (2025, October 17). States Jostle Over $50B Rural Health Fund as Trump’s Medicaid Cuts Trigger Scramble. KFF. <https://kffhealthnews.org/news/article/rural-health-fund-medicaid-cuts-hospitals-cms-maha/>
- Vox. (2025, June 12). How America is failing its rural hospitals. <https://www.youtube.com/watch?v=3onNLEpMZ00>)
- Zionts, A. (2025, September 23). RFK Jr. Misses Mark in Touting Rural Health Transformation Fund as Historic Infusion of Cash. KFF Health News. <https://kffhealthnews.org/news/article/fact-check-rfk-jr-misses-mark-calling-rural-health-transformation-program-historic-cash-infusion/>

### Datasets

- Flex Monitoring Team. (2025, April). CAH Financial Indicators Report: Summary of Indicator Medians by State. <https://www.flexmonitoring.org/sites/flexmonitoring.umn.edu/files/media/state-medians-2023data_report-final_2025.pdf>
- National Center for Health Statistics. (2024, September 17). NCHS Urban–Rural Classification Scheme for Counties. Centers for Disease Control and Prevention. <https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html>
- U.S. Department of Agriculture, Economic Research Service. (2025, September 26). Rural–Urban Commuting Area (RUCA) codes. <https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes>
- U.S. Department of Agriculture, Economic Research Service. (n.d.). Rural–Urban Continuum Codes. <https://www.ers.usda.gov/data-products/rural-urban-continuum-codes>
- U.S. Department of Agriculture, Economic Research Service. (n.d.). Urban Influence Codes. <https://www.ers.usda.gov/data-products/urban-influence-codes>
