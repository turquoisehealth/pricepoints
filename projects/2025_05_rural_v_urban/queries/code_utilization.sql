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
    WHERE REGEXP_LIKE(billing_code, '^[0-9]{3}$|^[0-9]{5}')
        AND service_year = 2024
    GROUP BY state, billing_code, billing_code_type
)
