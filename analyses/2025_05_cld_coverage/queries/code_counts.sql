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
        AND canonical_rate_score >= ( {{ min_rate_score }} )
),

code_descriptions AS (
    SELECT DISTINCT
        cld.billing_code,
        cld.billing_code_type,
        cld.service_line,
        cld.service_description
    FROM cld
    INNER JOIN codes
        ON cld.billing_code = codes.billing_code
        AND cld.billing_code_type = codes.billing_code_type
),

provider_count_total AS (
    SELECT COUNT(DISTINCT provider_id) AS total_providers
    FROM tq_dev.internal_dev_mnajarian_cld_v1_1.prod_combined_abridged
    WHERE taxonomy_grouping = 'Hospitals'
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
    WHERE num_payers >= ( {{ min_n_payers }} )
    GROUP BY billing_code, billing_code_type
)

SELECT
    codes.billing_code,
    codes.billing_code_type,
    cd.service_line,
    cd.service_description,
    ctbc.providers_by_code AS providers_w_gte_n_payers,
    ct.total_providers
FROM codes
CROSS JOIN provider_count_total AS ct
LEFT JOIN code_descriptions AS cd
    ON codes.billing_code = cd.billing_code
    AND codes.billing_code_type = cd.billing_code_type
LEFT JOIN provider_count_by_code AS ctbc
    ON codes.billing_code = ctbc.billing_code
    AND codes.billing_code_type = ctbc.billing_code_type
