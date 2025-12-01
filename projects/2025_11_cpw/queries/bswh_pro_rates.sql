WITH code_utilization AS (
    SELECT DISTINCT
        *,
        PERCENT_RANK()
            OVER (
                PARTITION BY state, billing_code_type
                ORDER BY state_claims_count
            )
            AS state_claims_percentile_sep,
        PERCENT_RANK()
            OVER (
                PARTITION BY state
                ORDER BY state_claims_count
            )
            AS state_claims_percentile_all
    FROM (
        SELECT
            state,
            billing_code,
            billing_code_type,
            SUM(total_count_encounters) AS state_claims_count
        FROM tq_production.claims_benchmarks.claims_benchmarks_utilization_state
        WHERE service_year = 2024
            AND state = 'TX'
        GROUP BY state, billing_code, billing_code_type
    )
)

SELECT
    cr.payer_name,
    cr.payer_id,
    cr.provider_id,
    cr.provider_name,
    cr.billing_code_type,
    cr.billing_code,
    cr.bill_type,
    cr.service_line,
    cr.service_description,
    cr.medicare_rate,
    cr.canonical_rate,
    cr.facility,
    COALESCE(cu.state_claims_percentile_sep, 0.01)
        AS state_claims_percentile_sep,
    COALESCE(cu.state_claims_percentile_all, 0.01)
        AS state_claims_percentile_all
FROM tq_dev.internal_dev_csong_cld_v2_2_2.prod_combined_abridged AS cr
LEFT JOIN code_utilization AS cu
    ON cr.billing_code = cu.billing_code
    AND cr.billing_code_type = cu.billing_code_type
WHERE cr.network_type = 'PPO'
    AND cr.canonical_rate IS NOT NULL
    AND cr.canonical_rate_score >= 3
    AND ARRAYS_OVERLAP(cr.npi, ARRAY[{{ bswh_npis }}])
