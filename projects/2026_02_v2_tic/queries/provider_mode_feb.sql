WITH modes AS (
    SELECT DISTINCT
        payer_id,
        data_source_name,
        'inline' AS provider_mode
    FROM hive.public_2026_02.inr_in_network_neg_rates_provider_groups

    UNION ALL

    SELECT DISTINCT
        payer_id,
        data_source_name,
        'reference' AS provider_mode
    FROM hive.public_2026_02.inr_in_network_neg_rates_provider_references
)

SELECT
    mo.payer_id,
    sp.payer_name,
    mo.data_source_name,
    mo.provider_mode
FROM modes AS mo
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(mo.payer_id AS VARCHAR) = sp.payer_id;
