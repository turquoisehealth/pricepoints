WITH payer_eins_named AS (
    SELECT DISTINCT
        pg.payer_id,
        LPAD(
            REGEXP_REPLACE(tin.value, '[^0-9]', ''),  -- noqa: RF01
            9,
            '0'
        ) AS tin_value
    FROM
        tq_production.public_2026_02.inr_provider_references_provider_groups
            AS pg
    WHERE tin.type = 'ein'  -- noqa: RF01
        AND tin.value IS NOT NULL  -- noqa: RF01
        AND REGEXP_REPLACE(tin.value, '[^0-9]', '') != ''  -- noqa: RF01
        AND LPAD(
            REGEXP_REPLACE(tin.value, '[^0-9]', ''),  -- noqa: RF01
            9,
            '0'
        ) != '000000000'  -- noqa: RF01
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

new_ein_counts AS (
    SELECT
        pe.payer_id,
        APPROX_DISTINCT(CASE
            WHEN re.tin IS NULL THEN pe.tin_value
        END) AS new_ein_names
    FROM payer_eins_named AS pe
    LEFT JOIN ref_eins AS re ON pe.tin_value = re.tin
    GROUP BY pe.payer_id
),

existing_ein_counts AS (
    SELECT
        pe.payer_id,
        APPROX_DISTINCT(pe.tin_value) AS existing_ein_names
    FROM payer_eins_named AS pe
    INNER JOIN ref_eins AS re ON pe.tin_value = re.tin
    GROUP BY pe.payer_id
)

SELECT
    COALESCE(nc.payer_id, ec.payer_id) AS payer_id,
    sp.payer_name,
    COALESCE(nc.new_ein_names, 0) AS new_ein_names,
    COALESCE(ec.existing_ein_names, 0) AS existing_ein_names
FROM new_ein_counts AS nc
FULL JOIN existing_ein_counts AS ec ON nc.payer_id = ec.payer_id
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON COALESCE(nc.payer_id, ec.payer_id) = sp.payer_id;
