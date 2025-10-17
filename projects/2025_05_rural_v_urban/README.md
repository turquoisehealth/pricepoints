# Rural v Urban

## Overview

This project uses a large national sample of aggregated, cleaned [Price Transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency) (PT) and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC) data to compare rural and urban hospitals in the United States. Specifically, it uses \~90M hospitals rates from the Turquoise Health Clear Rates database to compare commercial PPO reimbursement trends across hospitals located in different geographies. The full written piece can be found on [Substack](https://www.pricepoints.health/p/rural_v_urban).

## Data used

This analysis primarily uses \~92M rates commercial PPO rates negotiated between providers and insurers. The rates were exported from the Turquoise Health Clear Rates database, which collapses hospital rates, payer rates, claims data, and more into a single "canonical rate" per provider, payer, and code. Clear Rates also standardizes payment and contract methods by converting most rates into a flat "case rate" per code. This vastly expands the number of the comparable rates.

The following additional data sources are used:

- Policy Reporter data, which is an online subscription commercial and government coverage policy database containing the number of covered lives per policy/payer. This data is used to weight payers when aggregating to geographies like states. Low market share payers are downweighted to make sure rates are representative of what most people will see on their bills.
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

The main unit of analysis is project is the U.S. county

### Providers

All `Short Term Acute Care` and `Critical Access` hospitals with rates from

### Payers and plans

### Codes

### Time periods

## Methods

### Data ingest and cleaning

### Data aggregation and weighting

### Assumptions and limitations

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
- Vox. (2025, June 12). How America is failing its rural hospitals. <https://www.youtube.com/watch?v=3onNLEpMZ00>)
- Zionts, A. (2025, September 23). RFK Jr. Misses Mark in Touting Rural Health Transformation Fund as Historic Infusion of Cash. KFF Health News. <https://kffhealthnews.org/news/article/fact-check-rfk-jr-misses-mark-calling-rural-health-transformation-program-historic-cash-infusion/>

### Datasets

- Flex Monitoring Team. (2025, April). CAH Financial Indicators Report: Summary of Indicator Medians by State. <https://www.flexmonitoring.org/sites/flexmonitoring.umn.edu/files/media/state-medians-2023data_report-final_2025.pdf>
- National Center for Health Statistics. (2024, September 17). NCHS Urban–Rural Classification Scheme for Counties. Centers for Disease Control and Prevention. <https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html>
- U.S. Department of Agriculture, Economic Research Service. (2025, September 26). Rural–Urban Commuting Area (RUCA) codes. <https://www.ers.usda.gov/data-products/rural-urban-commuting-area-codes>
- U.S. Department of Agriculture, Economic Research Service. (n.d.). Rural–Urban Continuum Codes. <https://www.ers.usda.gov/data-products/rural-urban-continuum-codes>
- U.S. Department of Agriculture, Economic Research Service. (n.d.). Urban Influence Codes. <https://www.ers.usda.gov/data-products/urban-influence-codes>
