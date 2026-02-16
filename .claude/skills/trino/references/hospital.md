# Hospital Price Transparency Data

Schema: `tq_production.hospital_data`

| Table | Description |
|---|---|
| `hospital_rates` | Hospital rate/price data |
| `hospital_provider` | Hospital provider information |

## Example

```sql
SELECT p.provider_name, p.state, r.negotiated_dollar, r.billing_code
FROM tq_production.hospital_data.hospital_rates AS r
JOIN tq_production.hospital_data.hospital_provider AS p
    ON r.provider_id = p.id
WHERE r.billing_code = '99213'
    AND p.state = 'CA'
LIMIT 100;
```
