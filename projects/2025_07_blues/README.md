# BlueCard Underpayments

## Overview

This project uses [Price Transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency)
(PT) and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC)
data to analyze BlueCard underpayments, a loophole in the
[BlueCard program](https://www.blueshieldca.com/en/provider/guidelines-resources/bluecard)
that providers are exploiting for profit. BlueCard underpayments involve
arbitraging prices between different BCBSA-affiliated insurers located
in the same state. The full loophole is described in the
[written Substack piece](https://www.pricepoints.health/p/blues).

## Data used

This analysis primarily uses ~2.1M rates negotiated between providers and Blue
insurers. The rates were exported from the Turquoise Health Clear Rates alpha
database, which collapses hospital rates, payer rates, claims data, and more
into a single "canonical rate" per provider, payer, and code.

The following additional data sources are used:

- Aggregated [Form 5500 data](https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets)
  is used to determine which large U.S. employers have a Blue-affiliated plan,
  and how many employees they have.

Replication data is _not_ provided for this analysis for two reasons:

1. The size of the rate slice needed for replication is very large, such that
  sharing it would be both prohibitively cumbersome and harmful to Turquoise's
  business.
2. Sharing Blue negotiated rates for the whole country would help others
  perform the exact arbitrage that this piece is about.

## Scope

### Geography

This analysis uses rates from the seven states which have multiple Blue
payers: CA, WA, ID, VA, PA, MO, and NY. Other states are ignored.

### Providers

All `Short Term Acute Care` and `Critical Access` hospitals with rates from
two or more Blue payers are included in the analysis, since these are the
providers subject to BlueCard underpayments. ASCs, physician groups, and other
outpatient providers are excluded for simplicity.

In total, there are around 1,200 providers that meet the above criteria.

### Payers and plans

Only payers from the [official BCBS licensees list](https://www.bcbs.com/about-us/blue-cross-blue-shield-system/state-health-plan-companies)
are included in the analysis. Blue company names are cross-walked to Turquoise
and internal payer ID in [./data/blues.csv](./data/blues.csv).

Only commercial PPO plan rates are included. HMO, MA, EPO, and other plan types
are ignored.

### Codes

All inpatient (MS-DRG) and outpatient (HCPCS) codes are initially
included _except_ those for devices and drugs (e.g. J-codes).

This initial set of codes is then filtered to only include codes with rates
from _two or more_ Blue payers.

Codes must also have a Medicare rate of greater than $1,000 to be included.
Lower value codes are excluded because they highly distort percentage
difference calculations.

Finally, in cases where a code is represented by more than two rates
(e.g. one rate from each of the four Blue payers in NY), the minimum and
maximum rates are kept.

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is _not_ comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

Data is pulled from the Turquoise Health hospital rates dataset using the
[SQL here](./queries/blue_rates.sql). In addition to the
[scope limitations](#scope) described above, the following filters are applied:

- All rates with a [canonical rate score](https://cld.turquoise.health/components/accuracy/scores)
  of less than 3 are excluded.
- Rates with canonical rate percent of Medicare less than 70% or greater than
  1000% are excluded as outliers.
- For each provider-code combination, only the highest scored rate per payer
  is retained, then min/max rates are kept if multiple payers remain.
- Rate pairs with percentage differences exceeding 1000% are excluded as
  likely data errors.

### Assumptions and limitations

The nature of the data means that there are a number of assumptions necessary
to make the analysis workable. The analysis assumes that:

- PPO plans are representative of the broader opportunities for arbitrage
  between Blue payers.
- Rates included in aggregated plots do not include complications such as
  carve-outs or low stop-loss provisions. I manually checked for these in
  the MRF notes of each rate, but they aren't always recorded.
- The 20% contingency rate for BlueCard vendors is representative of the
  reimbursement they'd actually receive in the market. This roughly
  corresponded to information gathered from conversations with BlueCard and other underpayment
  vendors.
- I did my best to clean up the 5500 data and manually check that each
  employer listed actually offers a Blue plan. However, there's no way to
  check what percentage of employees actually take the Blue plan when it's
  offered, or to know what percentage live outside the "home" Blue state.
- Stop-loss clauses shown are correct and representative of actual Blue
  contract provisions. The clauses are pulled directly from posted MRFs,
  however additional stop-loss information may be available in payer-provider
  contracts that aren't publicly available.

## References

- Blue Cross Blue Shield of Washington. (n.d.). *BlueCard Washington service area map.* <https://beonbrand.getbynder.com/m/784a816d980c4f00/original/BlueCard-Washington-service-area-map.pdf>
- Blue Cross Blue Shield. (n.d.). *BCBS State Health Plans.* <https://www.bcbs.com/about-us/blue-cross-blue-shield-system/state-health-plan-companies>
- Blue Shield of California. (2025, March). *The BlueCard Program Provider Manual.* <https://www.bcbsil.com/docs/provider/il/standards/bluecard/bluecard-program-manual.pdf>
- Consumer Reports. (2007, December 30). *Blue Cross and Blue Shield: A historical compilation.* <https://advocacy.consumerreports.org/wp-content/uploads/2013/03/yourhealthdollar.org_blue-cross-history-compilation.pdf>
- Goforth, A. (2025, August 13). Hospitals reject $2.8B BCBS settlement, claiming anticompetitive practices persist. *BenefitsPro.* <https://www.benefitspro.com/2025/08/13/hospitals-reject-28b-bcbs-settlement-claiming-anticompetitive-practices-persist>
- Minemyer, P. (2025, August 21). Judge signs off on Blues plans’ $2.8B antitrust settlement with providers. *Fierce Healthcare.* <https://www.fiercehealthcare.com/payers/new-lawsuits-dozens-health-systems-opt-out-28b-blues-network-antitrust-settlement>
- Premera Blue Cross. (n.d.). FEP claims submission map. *Premera.* <https://www.premera.com/fep/fep-claims-submission-map/>
- Whatley Kallas, LLP. (2024, October). *Exhibit A: Settlement Agreement (Case No. 2:13-cv-20000-RDP).* <https://whatleykallas.com/wp-content/uploads/2024/10/3192-2-Exhibit-A-Settlement-Agreement.pdf>
