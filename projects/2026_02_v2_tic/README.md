# TiC 2.0 schema adoption

## Overview

The Transparency in Coverage (TiC) 2.0 schema - finalized by CMS in
October 2025 - introduced significant structural changes to payers' in-network
rate Machine-Readable Files (MRFs): a new TIN/EIN-related `business_name` field,
a required `network_name` field, and a shift toward internal provider group references. The CMS-mandated compliance date was February 2nd, 2026.

This piece examines how quickly U.S. health insurers adopted
the new schema. Using Turquoise Health data from December 2025 and March
2026, we find that adoption is still early: most of the top 50 payers have not
yet fully switched to the V2 schema. The broader pattern is partial compliance
– many payers have adopted one or two provisions, or updated most of their files
while leaving a long tail of older 1.x-formatted files behind.

## Data used

Payer in-network rate file metadata and summary statistics are sourced from
Turquoise Health's payer MRF processing pipeline, which ingests files posted
under the Transparency in Coverage rule.

Two snapshots are compared:

- **December 2025** - baseline data before the January 2026 compliance deadline
- **March 2026** - a post-deadline snapshot reflecting TiC 2.0 adoption at the
  end of the second month of implementation

## Assumptions and limitations

- Schema version strings are self-reported by payers and may not reflect
  full structural compliance with TiC 2.0.
- File parsing success rates depend on Turquoise Health's pipeline and may
  not capture all posted files.
- Covered lives estimates are approximate and updated periodically.
- The March 2026 snapshot reflects files available as of March 30, 2026;
  some payers may have updated files later in the quarter.

## References

- Centers for Medicare & Medicaid Services. (2025). *FAQs About ACA and CARES Act Implementation Part 70.* <https://www.cms.gov/files/document/aca-faqs-part-70.pdf>
- Centers for Medicare & Medicaid Services. (2025). *price-transparency-guide v2.0.0 Release.* <https://github.com/CMSgov/price-transparency-guide/releases/tag/v2.0.0>
- Turquoise Health. (2025). *Updated schema & enforcement date for payers, 5 months to comply.* <https://turquoise.health/resources/blog/updated-schema-enforcement-date-for-payers-5-months-to-comply>
