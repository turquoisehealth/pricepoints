# Clear Rates

The most important tables containing clean, validated price data. These combine hospital and payer transparency data into a single, deduplicated dataset.

| Table | Path |
|---|---|
| Abridged (latest) | `tq_production.clear_rates.prod_combined_abridged` |
| Full (latest) | `tq_production.clear_rates.prod_combined_all` |

## Gotchas

- These tables are large — always filter by `billing_code`, geography, or payer before aggregating
- The abridged and full tables share the same rows; abridged just has fewer columns. Use the abridged table by default

## Example

```sql
SELECT
    payer_id,
    payer_name,
    provider_id,
    provider_name,
    billing_code,
    canonical_rate
FROM tq_production.clear_rates.prod_combined_abridged
WHERE billing_code = '99213'
LIMIT 100;
```
