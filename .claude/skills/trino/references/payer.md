# Payer Data (Transparency in Coverage)

Schema: `tq_production.public_${YEAR}_${MONTH}`

Substitute `$YEAR` and `$MONTH` (two digits). Usually look 2 months prior for the most complete data.

| Table | Description |
|---|---|
| `core_rates` | Core payer rates |
| `compressed_rates` | Compressed/deduplicated rates |

Other tables under this schema are raw payer data (direct representations of raw JSON files).

## Gotchas

- Schema names are date-stamped. If a schema doesn't exist, use `SHOW SCHEMAS FROM tq_production` to find available months
- The most recent month may have incomplete data; go back 2 months for reliable coverage

## Example

```sql
SELECT billing_code, negotiated_rate, payer_id, provider_id
FROM tq_production.public_2025_10.core_rates
WHERE billing_code = '99213'
LIMIT 100;
```
