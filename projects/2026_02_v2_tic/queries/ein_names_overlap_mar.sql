WITH payer_eins_named_raw AS (
    SELECT
        pg.payer_id,
        pg.row_id,
        pg.data_source_name,
        tin.value AS tin_value  -- noqa: RF01
    FROM
        hive.public_2026_03.inr_provider_references_provider_groups
            AS pg
    WHERE
        tin.type = 'ein'  -- noqa: RF01
        AND tin.value IS NOT NULL  -- noqa: RF01
        AND tin.value != ''  -- noqa: RF01
        AND tin.value != '000000000'  -- noqa: RF01
        AND tin.business_name IS NOT NULL  -- noqa: RF01
        AND tin.business_name != ''  -- noqa: RF01
        AND pg.payer_id IN (
            '7', '643', '389', '169', '151', '49', '504', '174', '229', '388'
        )
),

payer_eins_named AS (
    SELECT DISTINCT
        payer_id,
        tin_value
    FROM payer_eins_named_raw
),

ref_eins AS (
    SELECT DISTINCT tin
    FROM tq_production.reference_external.tin_reference
    WHERE
        tin_type = 'ein'
        AND tin_name IS NOT NULL
        AND tin_name != ''
),

new_ein_counts AS (
    SELECT
        pe.payer_id,
        APPROX_DISTINCT(pe.tin_value) AS new_ein_names
    FROM payer_eins_named AS pe
    LEFT JOIN ref_eins AS re ON pe.tin_value = re.tin
    WHERE re.tin IS NULL
    GROUP BY pe.payer_id
),

existing_ein_counts AS (
    SELECT
        pe.payer_id,
        APPROX_DISTINCT(pe.tin_value) AS existing_ein_names
    FROM payer_eins_named AS pe
    INNER JOIN ref_eins AS re ON pe.tin_value = re.tin
    GROUP BY pe.payer_id
),

-- We only need one arbitrary row per TIN, since each TIN *should* share the
-- same set of NPIs (I hope)
new_ein_rows AS (
    SELECT
        raw.payer_id,
        raw.data_source_name,
        raw.tin_value,
        ARBITRARY(raw.row_id) AS row_id
    FROM payer_eins_named_raw AS raw
    LEFT JOIN ref_eins AS re ON raw.tin_value = re.tin
    WHERE re.tin IS NULL
    GROUP BY raw.payer_id, raw.data_source_name, raw.tin_value
),

npi_type_counts AS (
    SELECT
        nei.payer_id,
        APPROX_DISTINCT(
            CASE WHEN nppes.entity_type_code = '1' THEN nei.tin_value END
        ) AS approx_type1,
        APPROX_DISTINCT(
            CASE WHEN nppes.entity_type_code = '2' THEN nei.tin_value END
        ) AS approx_type2,
        APPROX_DISTINCT(
            CASE
                WHEN nppes.entity_type_code IS NOT NULL THEN nei.tin_value
            END
        ) AS approx_classified,
        APPROX_DISTINCT(
            CASE
                WHEN nppes.entity_type_code IS NULL THEN nei.tin_value
            END
        ) AS approx_unclassified
    FROM new_ein_rows AS nei
    INNER JOIN
        hive.public_2026_03.inr_provider_references_provider_groups_npi
            AS pgn
        ON nei.payer_id = pgn.payer_id
        AND nei.data_source_name = pgn.data_source_name
        AND nei.row_id = pgn.inr_provider_references_provider_groups_row_id
    INNER JOIN tq_production.reference_external.ref_cms_nppes_npi AS nppes
        ON TRY_CAST(pgn.value AS BIGINT) = nppes.npi
    GROUP BY nei.payer_id
)

SELECT
    COALESCE(nc.payer_id, ec.payer_id) AS payer_id,
    COALESCE(ec.existing_ein_names, 0) AS existing_ein_names,
    COALESCE(nc.new_ein_names, 0) AS new_ein_names,
    GREATEST(
        0,
        COALESCE(ntc.approx_classified, 0) - COALESCE(ntc.approx_type2, 0)
    ) AS npi_type_1_only,
    GREATEST(
        0,
        COALESCE(ntc.approx_classified, 0) - COALESCE(ntc.approx_type1, 0)
    ) AS npi_type_2_only,
    GREATEST(
        0,
        COALESCE(ntc.approx_type1, 0) + COALESCE(ntc.approx_type2, 0)
        - COALESCE(ntc.approx_classified, 0)
    ) AS npi_type_mixed,
    GREATEST(0, ntc.approx_unclassified) AS npi_type_unknown
FROM new_ein_counts AS nc
LEFT JOIN existing_ein_counts AS ec ON nc.payer_id = ec.payer_id
LEFT JOIN npi_type_counts AS ntc
    ON COALESCE(nc.payer_id, ec.payer_id) = ntc.payer_id
WHERE nc.payer_id IN (
        '7', '643', '389', '169', '151', '49', '504', '174', '229', '388'
    )
