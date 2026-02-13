SELECT
    inr_provider_references_provider_groups.payer_id,
    inr_provider_references_provider_groups.data_source_name,
    tin.type AS tin_type,
    tin.value AS tin_value,
    tin.business_name
FROM hive.public_2026_02.inr_provider_references_provider_groups
WHERE tin.type = 'ein'
LIMIT 5000;
