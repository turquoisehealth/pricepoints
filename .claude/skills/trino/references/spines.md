# Spines (Reference Data)

Schema: `tq_production.spines`

Spines are tables with the canonical list of key reference information
we use in our products:

- Geography (`spines_geo`)
- Medical services, e.g. billing/procedure codes (`spines_services`)
- Payers (`spines_payer`) and related networks (`spines_product_network_corpus`)
- Providers (`spines_provider`)

## Gotchas

The spines reference tables are usually normalized for data storage and
refresh purposes. This also means that most of the spines tables are not
ready to be queried for general analysis as standalone table - typically at
least one join would be needed.
