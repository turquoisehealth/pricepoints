-- Insert codes data provided via CSV or a separate query
WITH codes AS (
    SELECT *
    FROM ( {{ codes_table }} )
),

code_count AS (
    SELECT COUNT(*) AS total_codes
    FROM codes
),

cld AS (
    SELECT *
    FROM tq_dev.internal_dev_csong_cld_v2_0_1.prod_combined_abridged
    WHERE canonical_rate IS NOT NULL
        AND network_class = 'Commercial'
        AND taxonomy_grouping = 'Hospitals'
        AND canonical_rate_score >= ( {{ min_rate_score }} )
)

SELECT
    cld.provider_id,
    cld.provider_name,
    cld.payer_id,
    cld.payer_name,
    cld.payer_network_name,
    ARBITRARY(cld.state) AS state,
    ARBITRARY(cld.cbsa_name) AS cbsa_name,
    SUM(CASE WHEN cld.billing_code IS NOT NULL THEN 1 END) AS cld_codes,
    ARBITRARY(cc.total_codes) AS total_codes,
    CAST(SUM(CASE WHEN cld.billing_code IS NOT NULL THEN 1 END) AS REAL)
    / ARBITRARY(cc.total_codes) AS percent_coverage
FROM codes
CROSS JOIN code_count AS cc
LEFT JOIN cld
    ON codes.billing_code = cld.billing_code
    AND codes.billing_code_type = cld.billing_code_type
GROUP BY 1, 2, 3, 4, 5
