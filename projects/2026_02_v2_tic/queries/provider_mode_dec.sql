SELECT DISTINCT
    payer_id,
    data_source_name,
    'inline' AS provider_mode
FROM tq_production.public_2025_12.inr_in_network_neg_rates_provider_groups

UNION ALL

SELECT DISTINCT
    payer_id,
    data_source_name,
    'reference' AS provider_mode
FROM tq_production.public_2025_12.inr_in_network_neg_rates_provider_references;
