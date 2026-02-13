SELECT
    inr_provider_references_provider_groups.payer_id,
    inr_provider_references_provider_groups.data_source_name,
    BOOL_OR(
        tin.business_name IS NOT NULL AND tin.business_name != ''
    ) AS has_any_business_name
FROM hive.public_2026_02.inr_provider_references_provider_groups
WHERE tin.type = 'ein'
GROUP BY
    inr_provider_references_provider_groups.payer_id, inr_provider_references_provider_groups.data_source_name;
