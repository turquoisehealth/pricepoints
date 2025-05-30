# Orange County’s Big Healthcare Brawl

## Overview

This project uses [price transparency](https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency)
and [Transparency in Coverage](https://www.cms.gov/files/document/transparency-coverage-webinar-naic-06-27-22-508.pdf) (TiC)
data to analyze the May 2025 contract dispute between Hoag Health
and Blue Shield of California. The goal is to lift the curtain on such
disputes and to demonstrate how they can be quantified/contextualized
using price transparency data.

## Data used

This analysis primarily uses 6,917 negotiated rates exported from an alpha
version of the Turquoise Health "CLD" database. CLD collapses hospital rates,
payer rates, claims data, and more into a single "canonical rate" per provider,
payer, and code.

The following additional data sources are used:

- Policy Reporter data, which is an online subscription commercial and
  government coverage policy database containing the number of covered lives
  per policy/payer. This data is only used to provide context within the
  article (e.g. percent market share per payer in the Los Angeles market).
- Claims data is used to manually choose a set of representative procedures.
  See [Codes](#codes) for more information. This data is _not_ included in the
  export for licensing reasons.

See [Replication](#replication) for instructions on how to download and use
the data necessary to replicate this project.

## Scope

### Codes

The analysis compares the negotiated rates for 20 billing codes across
hospitals and major insurers in Los Angeles. The codes include a mix of
inpatient and outpatient procedures:

| Code      | Type   | Description           |
|-----------|--------|-----------------------|
| **788**   | MS-DRG | C-section no MCC      |
| **807**   | MS-DRG | Vag delivery no MCC   |
| **871**   | MS-DRG | Sepsis w MCC          |
| **331**   | MS-DRG | Bowel proc no MCC     |
| **853**   | MS-DRG | Infection proc w MCC  |
| **419**   | MS-DRG | Gall bladder removal  |
| **743**   | MS-DRG | Uterine proc w CC     |
| **439**   | MS-DRG | Pancreas disorders    |
| **392**   | MS-DRG | GI cond no MCC        |
| **897**   | MS-DRG | Alc/drug abuse no MCC |
| **27130** | HCPCS  | Hip/knee replacement  |
| **45378** | HCPCS  | Colonoscopy           |
| **93005** | HCPCS  | Electrocardiogram     |
| **93452** | HCPCS  | Card cath             |
| **42820** | HCPCS  | Tonsils removal       |
| **47562** | HCPCS  | Gall bladder removal  |
| **99213** | HCPCS  | Est. patient visit    |
| **99283** | HCPCS  | Emergency visit       |
| **62323** | HCPCS  | Epidural injection    |
| **17110** | HCPCS  | Skin lesion removal   |
| **74176** | HCPCS  | CT abdomen/pelvis     |

These codes were chosen because they are:

- Highly utilized, based on claims data from the Los Angeles CBSA
- Representative of many different types of care and complexity
- Generally discrete events, rather than associated with chronic diseases or
  ongoing care
- Included in the CMS
  [list of 70 shoppable services](https://www.cms.gov/files/document/steps-making-public-standard-charges-shoppable-services.pdf)
  (not all of them)
- Present across payer and provider MRFs of the target market (Los Angeles)
  i.e. they don't have much missingness

### Providers

The exported rates data included hospitals, ASCs, and other professional
service centers. However, only `Short Term Acute Care` and `Critical Access`
hospitals are used in the analysis.

Hospitals must currently exist in the Turquoise Health CLD database and have
rates for at least 10 (out of 20) codes to be included. Hospitals must also
have multiple payers (more than 1, out of 5) to be included.

In total, 87 unique hospitals are included in the analysis. Notably, Kaiser
Permanente is excluded since it's an HMO (difficult to compare to PPO prices)
and somewhat of a unique case.

### Payers

Only commercial rates/payers are included. Exchange, VA, Medicaid,
and Medicare rates are ignored (though Medicare rates are used as a benchmark).

To simplify the analysis, only the following major California payers are
included:

- Blue Shield of California
- United Healthcare
- Cigna
- Anthem/Elevance
- Aetna

Kaiser is excluded from this analysis even though it's LA's largest
insurer by market share.

### Plans

Only commercial PPO plans are currently included. HMO and other plan types
are not currently available in CLD.

### Contract types

CLD includes all contract types, provided they can be transformed to
represent a single price per procedure. See the
[CLD documentation](https://cld.turquoise.health/methodology/airflow/transformations)
for more information.

## Methods

This section outlines data cleaning steps and transformations performed as part
of the analysis. It is *not* comprehensive and instead tries to highlight only
the most important decisions.

### Data ingest and cleaning

Data is pulled from the Turquoise Health hospital rates dataset using the
[SQL here](./queries/rates.sql). In addition to the [scope limitations](#scope)
described above, the following filters are applied:

- All [CLD-flagged outliers](https://cld.turquoise.health/components/accuracy/scores)
  are excluded. The flagging process used finds rates that fall outside
  reasonable bounds determined by Medicare and claims data.
- Only rates directly sourced from MRFs are used (no imputed rates or rates
  derived from claims).
- Rates must be between 50% and 1000% of the Medicare price. This mostly prunes
  a few very low outliers.

### Assumptions and limitations

The nature of the data means that there are a number of assumptions necessary
to make the analysis workable. The analysis assumes that:

- The sample of [codes chosen above](#codes) is roughly representative of the
  cost of care across hospitals. Different patient volumes, utilization
  patterns, and even coding procedures could make this assumption shaky at best.
- PPO plans are representative of the broader cost differences between payers.
- Rates do not include complications such as carve-outs or low stop-loss
  provisions. I manually checked for these in the MRF notes of each rate, but
  they aren't always recorded.

And the analysis has a number of limitations:

- It doesn't reflect any financial information such as net patient revenue
  or revenue sources. Such information is difficult to source beyond what's
  provided to Medicare.

## Replication

This analysis is intended to be fully reproducible. Replication data (the
results of running [`ingest.py`](./ingest.py)) are available via the ZIP file
below. Extract the ZIP file to the `data/` directory, install dependencies, and
run the [`analysis.qmd`](./analysis.qmd) notebook to reproduce the analysis.

#### [Link to replication data](https://drive.google.com/uc?export=download&id=1bJZ2L5VUYdVCgbU9Ii41ZLNPJS3t2MFK)

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

- Blue Shield of California. (2025, May 1). *Blue Shield Focused on Affordability as Nonprofit Health Plan Negotiates in Good Faith With Hoag Clinic and Facilities.* <https://news.blueshieldca.com/2025/05/01/blue-shield-focused-on-affordability-as-nonprofit-health-plan-negotiates-in-good-faith-with-hoag-clinic-and-facilities>
- FTI Consulting. (2025, January 1). *2024 End-of-Year Provider-Payer Dispute Data Update.* <https://fticommunications.com/2024-year-end-provider-payer-dispute-data-update-medicare-advantage-under-fire/>
- Hoag. (2025, April 30). *FAQ: Hoag’s Contract Negotiations with Blue Shield.* <https://www.hoag.org/articles/faq-hoags-contract-negotiations-with-blue-shield/>
- Policy Reporter, by Mercalis. (2025). *Covered Lives retrieved from PolicyCore database.* <https://www.policyreporter.com>. 2025, 05 01.
