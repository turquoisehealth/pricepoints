SELECT
    inr_provider_references_provider_groups.payer_id,
    inr_provider_references_provider_groups.data_source_name,
    COUNT(*) AS n_ein_records,
    COUNT(
        CASE WHEN tin.business_name IS NOT NULL  -- noqa: RF01
                AND tin.business_name != '' THEN 1  -- noqa: RF01
        END
    ) AS n_with_business_name
FROM hive.public_2026_02.inr_provider_references_provider_groups
WHERE tin.type = 'ein'  -- noqa: RF01
GROUP BY
    inr_provider_references_provider_groups.payer_id,
    inr_provider_references_provider_groups.data_source_name;
