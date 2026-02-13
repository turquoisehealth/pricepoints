SELECT
    payer_id,
    data_source_name,
    ARBITRARY(network_name) AS sample_network_name
FROM hive.public_2026_02.inr_provider_references
WHERE network_name IS NOT NULL
    AND network_name != ''
GROUP BY payer_id, data_source_name;
