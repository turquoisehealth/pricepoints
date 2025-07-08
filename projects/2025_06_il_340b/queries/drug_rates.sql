WITH cld_drug_codes AS (
    SELECT DISTINCT
        provider_id,
        state,
        billing_code
    FROM tq_dev.internal_dev_csong_cld_v1_2_1.prod_combined_abridged
    WHERE state = 'IL'
        -- Only non-self-administered drugs
        AND SUBSTRING(billing_code, 1, 1) = 'J'
),

utilization_state AS (
    SELECT
        state,
        billing_code,
        SUM(total_count_encounters) AS count_enc
    FROM hive.claims_benchmarks.claims_benchmarks_utilization_state
    WHERE claim_type_code IN ('institutional')
        AND npi_source = 'hco'
        AND service_year = 2023
        AND state = 'IL'
    GROUP BY state, billing_code
),

top_n_drugs AS (
    SELECT DISTINCT
        cld.state,
        cld.billing_code,
        us.count_enc
    FROM cld_drug_codes AS cld
    LEFT JOIN utilization_state AS us
        ON cld.state = us.state
        AND cld.billing_code = us.billing_code
    ORDER BY us.count_enc DESC
    LIMIT 500
),

parsed_rates AS (
    SELECT
        *,
        CAST(
            JSON_EXTRACT_SCALAR(
                canonical_method_params,
                '$.parsed_quantity'
            ) AS DECIMAL(10, 2)
        ) AS parsed_quantity,
        CAST(
            JSON_EXTRACT_SCALAR(
                canonical_method_params,
                '$.asp_quantity'
            ) AS DECIMAL(10, 2)
        ) AS asp_quantity
    FROM tq_dev.internal_dev_csong_cld_v1_2_1.prod_combined_abridged
),

-- Manual calculations for state-level payer market share since the
-- CLD columns are national
policy_reporter AS (
    SELECT
        pr.line_of_business,
        pr.plan_type,
        pr.state_short AS state,
        pr.covered_lives,
        pr.tq_payer_payer_id
    FROM redshift.reference.policy_reporter_county AS pr
    WHERE pr.line_of_business = 'Commercial'
),

state_payer_total AS (
    SELECT
        line_of_business,
        state,
        tq_payer_payer_id,
        SUM(covered_lives) AS payer_covered_lives
    FROM policy_reporter
    GROUP BY line_of_business, state, tq_payer_payer_id
),

state_total AS (
    SELECT
        line_of_business,
        state,
        SUM(covered_lives) AS state_covered_lives
    FROM policy_reporter
    GROUP BY line_of_business, state
),

payer_market_share AS (
    SELECT DISTINCT
        pr.state,
        pr.line_of_business,
        pr.tq_payer_payer_id AS payer_id,
        spt.payer_covered_lives / st.state_covered_lives AS state_market_share
    FROM policy_reporter AS pr
    LEFT JOIN state_payer_total AS spt
        ON pr.state = spt.state
        AND pr.tq_payer_payer_id = spt.tq_payer_payer_id
        AND pr.line_of_business = spt.line_of_business
    LEFT JOIN state_total AS st
        ON pr.state = st.state
        AND pr.line_of_business = st.line_of_business
)

SELECT
    pr.provider_id,
    pr.provider_name,
    pr.payer_id,
    pr.payer_name,
    pms.state_market_share,
    pr.billing_code,
    pr.medicare_rate,
    pr.asp_payment_limit / 1.06 AS asp,
    pr.canonical_rate,
    pr.canonical_rate_source,
    pr.canonical_rate_type,
    pr.canonical_gross_charge,
    pr.canonical_gross_charge_type,
    -- MRF gross charges aren't (yet) standardized, so here just applying the
    -- same dose transformation as was used for the rate
    CASE WHEN pr.canonical_gross_charge_type = 'mrf_gross_charge_provider'
            THEN (pr.canonical_gross_charge / pr.parsed_quantity)
            * pr.asp_quantity
        ELSE
            pr.canonical_gross_charge
    END AS gross_charge_std,
    tn.count_enc
FROM parsed_rates AS pr
INNER JOIN top_n_drugs AS tn
    ON pr.state = tn.state
    AND pr.billing_code = tn.billing_code
LEFT JOIN payer_market_share AS pms
    ON pr.state = pms.state
    AND pr.payer_id = pms.payer_id
WHERE pr.taxonomy_grouping = 'Hospitals'
    AND pr.canonical_rate_score >= 3
