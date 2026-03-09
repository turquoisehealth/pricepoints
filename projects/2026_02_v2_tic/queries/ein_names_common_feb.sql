SELECT
    payer_id,
    payer_name,
    data_source_name,
    business_name,
    name_count
FROM (
    SELECT
        pg.payer_id,
        sp.payer_name,
        pg.data_source_name,
        tin.business_name,  -- noqa: RF01
        COUNT(*) AS name_count,
        ROW_NUMBER() OVER (
            PARTITION BY pg.payer_id ORDER BY COUNT(*) DESC
        ) AS rn
    FROM hive.public_2026_02.inr_provider_references_provider_groups AS pg
    LEFT JOIN tq_production.spines.spines_payer AS sp
        ON CAST(pg.payer_id AS VARCHAR) = sp.payer_id
    WHERE tin.type = 'ein'  -- noqa: RF01
        AND tin.business_name IS NOT NULL  -- noqa: RF01
        AND tin.business_name != ''  -- noqa: RF01
    GROUP BY
        pg.payer_id,
        sp.payer_name,
        pg.data_source_name,
        tin.business_name  -- noqa: RF01
)
WHERE rn = 1;
