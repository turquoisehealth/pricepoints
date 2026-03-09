SELECT
    pg.payer_id,
    sp.payer_name,
    pg.data_source_name,
    COUNT(DISTINCT tin.business_name) AS unique_name_count  -- noqa: RF01
FROM hive.public_2026_02.inr_provider_references_provider_groups AS pg
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(pg.payer_id AS VARCHAR) = sp.payer_id
WHERE tin.type = 'ein'  -- noqa: RF01
    AND tin.business_name IS NOT NULL  -- noqa: RF01
    AND tin.business_name != ''  -- noqa: RF01
GROUP BY pg.payer_id, sp.payer_name, pg.data_source_name;
