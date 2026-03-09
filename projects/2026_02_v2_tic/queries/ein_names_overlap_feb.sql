WITH payer_eins AS (
    SELECT DISTINCT
        pg.payer_id,
        sp.payer_name,
        pg.data_source_name,
        tin.value AS tin_value,  -- noqa: RF01
        COALESCE(
            tin.business_name IS NOT NULL  -- noqa: RF01
            AND tin.business_name != '', FALSE  -- noqa: RF01
        ) AS has_tic_name
    FROM hive.public_2026_02.inr_provider_references_provider_groups AS pg
    LEFT JOIN tq_production.spines.spines_payer AS sp
        ON CAST(pg.payer_id AS VARCHAR) = sp.payer_id
    WHERE tin.type = 'ein'  -- noqa: RF01
),

ref_eins AS (
    SELECT DISTINCT tin
    FROM tq_production.reference_external.tin_reference
    WHERE tin_type = 'ein'
        AND tin_name IS NOT NULL
        AND tin_name != ''
)

SELECT
    pe.payer_id,
    pe.payer_name,
    pe.data_source_name,
    COUNT(DISTINCT CASE
        WHEN re.tin IS NOT NULL AND NOT pe.has_tic_name
            THEN pe.tin_value
    END) AS tq_ein_values,
    COUNT(DISTINCT CASE
        WHEN re.tin IS NOT NULL AND pe.has_tic_name
            THEN pe.tin_value
    END) AS overlap_ein_values,
    COUNT(DISTINCT CASE
        WHEN re.tin IS NULL AND pe.has_tic_name
            THEN pe.tin_value
    END) AS tic_ein_values
FROM payer_eins AS pe
LEFT JOIN ref_eins AS re
    ON pe.tin_value = re.tin
GROUP BY pe.payer_id, pe.payer_name, pe.data_source_name;
