WITH top_names AS (
    SELECT
        pg.payer_id,
        APPROX_MOST_FREQUENT(
            10, tin.business_name, 5000  -- noqa: RF01
        ) AS name_map,
        APPROX_DISTINCT(tin.business_name) AS unique_name_count  -- noqa: RF01
    FROM hive.public_2026_03.inr_provider_references_provider_groups AS pg
    WHERE
        tin.type = 'ein'  -- noqa: RF01
        AND tin.business_name IS NOT NULL  -- noqa: RF01
        AND tin.business_name != ''  -- noqa: RF01
    GROUP BY pg.payer_id
)

SELECT
    tn.payer_id,
    sp.payer_name,
    MAP_KEYS(tn.name_map) AS business_name_value,
    MAP_VALUES(tn.name_map) AS business_name_count,
    tn.unique_name_count AS business_name_n_uniq
FROM top_names AS tn
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(tn.payer_id AS VARCHAR) = sp.payer_id;
