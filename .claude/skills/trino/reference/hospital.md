# Hospital Price Transparency Data

Schema: `tq_production.hospital_data`

| Table | Description |
|---|---|
| `hospital_rates` | Hospital rate/price data |
| `hospital_provider` | Hospital provider information |
| `hospital_billing_code_group` | Billing code groupings |

Historical snapshots: `tq_production.hospital_historical_${YEAR}_${MONTH}` with the same table structure (e.g. `hospital_historical_2025_08.hospital_rates`).
