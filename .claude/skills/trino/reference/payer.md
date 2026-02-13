# Payer Data (Transparency in Coverage)

Schema: `tq_production.public_${YEAR}_${MONTH}`

Substitute $YEAR and $MONTH (two digits). Usually look 2 months prior for the most complete data.

| Table | Description |
|---|---|
| `core_rates` | Core payer rates (unfiltered) |
| `compressed_rates` | Compressed/deduplicated rates |
| `aa` | Allowed amounts |
| `aa_out_of_network` | Out-of-network allowed amounts |
| `plans_to_networks` | Plan-to-network mappings |
| `compressed_idx_plan` | Compressed index: plans |
| `compressed_idx_file_label` | Compressed index: file labels |
| `idx_network_map` | Network mapping index |
| `compressed_providers` | Compressed provider data |
| `file_labels` | File label metadata |

Other tables under this schema are raw payer data (direct representations of raw JSON files).
