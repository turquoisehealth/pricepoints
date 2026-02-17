SELECT
    inr_provider_references_provider_groups.payer_id,
    inr_provider_references_provider_groups.data_source_name,
    tin.value AS tin_value,  -- noqa: RF01
    tin.business_name  -- noqa: RF01
FROM hive.public_2026_02.inr_provider_references_provider_groups
WHERE tin.type = 'ein'  -- noqa: RF01
    AND tin.business_name IS NOT NULL  -- noqa: RF01
    AND tin.business_name != ''  -- noqa: RF01
ORDER BY 1
LIMIT 500;
