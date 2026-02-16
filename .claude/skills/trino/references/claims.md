# Claims Benchmarks

Schema: `tq_production.claims_benchmarks`

Tables follow the pattern: `claims_benchmarks_{metric}_{geography}[_{payer_grouping}]`

**Metrics:** `allowable`, `gross_charges`, `utilization`
  - `allowable` must have a payer grouping
  - `gross_charges` must NOT have a payer grouping
  - `utilization` optionally has a payer grouping
**Geographies:** `npi`, `cbsa`, `zip3`, `zip`, `state`, `national`
**Payer groupings (optional):** `_payer`, `_payerchannel`

## Example

```sql
SELECT *
FROM tq_production.claims_benchmarks.claims_benchmarks_utilization_state
WHERE billing_code = '99213'
LIMIT 100;
```
