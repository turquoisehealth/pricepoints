SELECT
    payer_id,
    data_source_name,
    network_name
FROM hive.public_2026_02.inr_provider_references
WHERE network_name IS NOT NULL
    AND network_name != ''
LIMIT 500;
