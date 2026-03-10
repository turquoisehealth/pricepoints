WITH payer_eins AS (
    SELECT DISTINCT
        pg.payer_id,
        LPAD(tin.value, 9, '0') AS tin_value  -- noqa: RF01
    FROM hive.public_2026_02.inr_provider_references_provider_groups AS pg
    WHERE tin.type = 'ein'  -- noqa: RF01
        AND tin.business_name IS NOT NULL  -- noqa: RF01
        AND tin.business_name != ''  -- noqa: RF01
),

ref_eins AS (
    SELECT DISTINCT LPAD(tin, 9, '0') AS tin
    FROM tq_production.reference_external.tin_reference
    WHERE tin_type = 'ein'
        AND tin_name IS NOT NULL
        AND tin_name != ''
),

counts AS (
    SELECT
        pe.payer_id,
        APPROX_DISTINCT(CASE
            WHEN re.tin IS NULL THEN pe.tin_value
        END) AS new_ein_names,
        APPROX_DISTINCT(CASE
            WHEN re.tin IS NOT NULL THEN pe.tin_value
        END) AS existing_ein_names
    FROM payer_eins AS pe
    LEFT JOIN ref_eins AS re
        ON pe.tin_value = re.tin
    GROUP BY pe.payer_id
)

SELECT
    ct.payer_id,
    sp.payer_name,
    ct.new_ein_names,
    ct.existing_ein_names
FROM counts AS ct
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(ct.payer_id AS VARCHAR) = sp.payer_id;
