SELECT
    payer_id,
    data_source_name,
    COUNT(DISTINCT network_name) AS n_distinct_names,
    ARBITRARY(network_name) AS sample_network_name
FROM hive.public_2026_02.inr_provider_references
GROUP BY payer_id, data_source_name;
