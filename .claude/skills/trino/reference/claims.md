# Claims Benchmarks

Schema: `tq_production.claims_benchmarks`

Tables follow the pattern: `claims_benchmarks_{metric}_{geography}[_{payer_grouping}]`

**Metrics:** `allowable`, `gross_charges`, `utilization`
**Geographies:** `npi`, `cbsa`, `zip3`, `zip`, `state`, `national`
**Payer groupings (optional):** `_payer`, `_payerchannel`

Common tables (not exhaustive):

| Table | Description |
|---|---|
| `claims_benchmarks_allowable_cbsa_payer` | Allowed amounts by CBSA and payer |
| `claims_benchmarks_allowable_npi_payer` | Allowed amounts by NPI and payer |
| `claims_benchmarks_allowable_state_payer` | Allowed amounts by state and payer |
| `claims_benchmarks_allowable_zip3_payer` | Allowed amounts by ZIP3 and payer |
| `claims_benchmarks_gross_charges_npi` | Gross charges by NPI |
| `claims_benchmarks_gross_charges_zip3` | Gross charges by ZIP3 |
| `claims_benchmarks_utilization_npi_payer` | Utilization by NPI and payer |
| `claims_benchmarks_utilization_zip_payer` | Utilization by ZIP and payer |
