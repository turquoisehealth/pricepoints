SELECT
    ut_cbsa.*,
    ut_cbsa_bsca.cbsa_bsca_claims_count,
    ut_cbsa_bsca.cbsa_bsca_claims_rank,
    ut_cbsa_hoag.cbsa_hoag_claims_count,
    ut_cbsa_hoag.cbsa_hoag_claims_rank,
    ut_cbsa_bsca_hoag.cbsa_bsca_hoag_claims_count,
    ut_cbsa_bsca_hoag.cbsa_bsca_hoag_claims_rank
FROM (
    SELECT DISTINCT
        *,
        RANK()
            OVER (
                ORDER BY cbsa_claims_count DESC
            )
            AS cbsa_claims_rank
    FROM (
        SELECT
            billing_code,
            billing_code_type,
            SUM(total_count_encounters) AS cbsa_claims_count
        FROM hive.claims_benchmarks.claims_benchmarks_utilization_cbsa_payer
        WHERE cbsa_name = 'Los Angeles-Long Beach-Anaheim, CA'
            AND REGEXP_LIKE(billing_code, '^[0-9]{3}$|^[0-9]{5}')
            AND service_year = 2023
        GROUP BY billing_code, billing_code_type
    )
) AS ut_cbsa
LEFT JOIN (
    SELECT DISTINCT
        *,
        RANK()
            OVER (
                ORDER BY cbsa_bsca_claims_count DESC
            )
            AS cbsa_bsca_claims_rank
    FROM (
        SELECT
            billing_code,
            billing_code_type,
            SUM(total_count_encounters) AS cbsa_bsca_claims_count
        FROM hive.claims_benchmarks.claims_benchmarks_utilization_cbsa_payer
        WHERE cbsa_name = 'Los Angeles-Long Beach-Anaheim, CA'
            AND payer_name = 'Blue Shield of California'
            AND REGEXP_LIKE(billing_code, '^[0-9]{3}$|^[0-9]{5}')
            AND service_year = 2023
        GROUP BY billing_code, billing_code_type
    )
) AS ut_cbsa_bsca
    ON ut_cbsa.billing_code = ut_cbsa_bsca.billing_code
    AND ut_cbsa.billing_code_type = ut_cbsa_bsca.billing_code_type
LEFT JOIN (
    SELECT DISTINCT
        *,
        RANK()
            OVER (
                ORDER BY cbsa_hoag_claims_count DESC
            )
            AS cbsa_hoag_claims_rank
    FROM (
        SELECT
            billing_code,
            billing_code_type,
            SUM(count_encounters) AS cbsa_hoag_claims_count
        FROM hive.claims_benchmarks.claims_benchmarks_utilization_npi
        WHERE npi IN ('1518951300', '1154803773')
            AND REGEXP_LIKE(billing_code, '^[0-9]{3}$|^[0-9]{5}')
            AND service_year = 2023
        GROUP BY billing_code, billing_code_type
    )
) AS ut_cbsa_hoag
    ON ut_cbsa.billing_code = ut_cbsa_hoag.billing_code
    AND ut_cbsa.billing_code_type = ut_cbsa_hoag.billing_code_type
LEFT JOIN (
    SELECT DISTINCT
        *,
        RANK()
            OVER (
                ORDER BY cbsa_bsca_hoag_claims_count DESC
            )
            AS cbsa_bsca_hoag_claims_rank
    FROM (
        SELECT
            billing_code,
            billing_code_type,
            SUM(count_encounters) AS cbsa_bsca_hoag_claims_count
        FROM hive.claims_benchmarks.claims_benchmarks_utilization_npi_payer
        WHERE npi IN ('1518951300', '1154803773')
            AND payer_name = 'Blue Shield of California'
            AND REGEXP_LIKE(billing_code, '^[0-9]{3}$|^[0-9]{5}')
            AND service_year = 2023
        GROUP BY billing_code, billing_code_type
    )
) AS ut_cbsa_bsca_hoag
    ON ut_cbsa.billing_code = ut_cbsa_bsca_hoag.billing_code
    AND ut_cbsa.billing_code_type = ut_cbsa_bsca_hoag.billing_code_type
ORDER BY ut_cbsa.cbsa_claims_rank
