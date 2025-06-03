-- Insert codes data provided via CSV or a separate query
WITH codes AS (
    SELECT *
    FROM ( {{ codes_table }} )
),

cld AS (
    SELECT *
    FROM tq_dev.internal_dev_mnajarian_cld_v1_1.prod_combined_abridged
    WHERE canonical_rate IS NOT NULL
        AND network_class = 'Commercial'
        AND taxonomy_grouping = 'Hospitals'
        -- Keep only validated/MRF rates, no outliers or imputed
        AND canonical_rate_score >= 3
),

provider_count_total AS (
    SELECT COUNT(DISTINCT provider_id) AS total_providers
    FROM cld
),

provider_count_by_code_per_payer AS (
    SELECT
        codes.billing_code,
        codes.billing_code_type,
        cld.provider_id,
        COUNT(DISTINCT cld.payer_id) AS num_payers
    FROM codes
    INNER JOIN cld
        ON codes.billing_code = cld.billing_code
        AND codes.billing_code_type = cld.billing_code_type
    GROUP BY codes.billing_code, codes.billing_code_type, cld.provider_id
),

provider_count_by_code AS (
    SELECT
        billing_code,
        billing_code_type,
        COUNT(DISTINCT provider_id) AS providers_by_code
    FROM provider_count_by_code_per_payer
    WHERE num_payers >= 2
    GROUP BY billing_code, billing_code_type
)

SELECT
    codes.billing_code,
    codes.billing_code_type,
    ctbc.providers_by_code AS providers_w_gte_2_payers,
    ct.total_providers
FROM codes
CROSS JOIN provider_count_total AS ct
LEFT JOIN provider_count_by_code AS ctbc
    ON codes.billing_code = ctbc.billing_code
    AND codes.billing_code_type = ctbc.billing_code_type
