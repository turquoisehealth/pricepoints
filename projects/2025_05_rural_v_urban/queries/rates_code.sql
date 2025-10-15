-- Keep only commercial rates from hospitals, no MA, no drug codes, no ASCs, etc
WITH cld_subset AS (
    SELECT cld.*
    FROM tq_dev.internal_dev_csong_cld_v2_2_0.prod_combined_all AS cld
    WHERE cld.taxonomy_grouping = 'Hospitals'
        AND cld.network_type = 'PPO'
        AND cld.canonical_rate IS NOT NULL
        AND cld.canonical_rate_score >= 3
        AND NOT cld.is_drug_code
        AND (
            -- No device rates, drug rates, etc
            (
                cld.billing_code_type = 'HCPCS'
                AND REGEXP_LIKE(cld.billing_code, '^[0-9]{5}$')
            )
            OR cld.billing_code_type = 'MS-DRG'
        )
        AND cld.canonical_rate_percent_of_medicare BETWEEN 0.4 AND 10.0
),

-- Use Policy Reporter to get state-level number of covered lives per payer,
-- then turn that into a 0-1 weight using a percentile rank
policy_reporter AS (
    SELECT
        pr.line_of_business,
        pr.plan_type,
        pr.state_short AS state,
        pr.covered_lives,
        pr.tq_payer_payer_id
    FROM redshift.reference.policy_reporter_state AS pr
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
        PERCENT_RANK()
            OVER (
                PARTITION BY pr.state
                ORDER BY spt.payer_covered_lives / st.state_covered_lives
            )
            AS state_market_share
    FROM policy_reporter AS pr
    LEFT JOIN state_payer_total AS spt
        ON pr.state = spt.state
        AND pr.tq_payer_payer_id = spt.tq_payer_payer_id
        AND pr.line_of_business = spt.line_of_business
    LEFT JOIN state_total AS st
        ON pr.state = st.state
        AND pr.line_of_business = st.line_of_business
),

-- Join the weights data and replace missing weights with a 1st
-- percentile rank (lowest)
cld_filled AS (
    SELECT
        cs.*,
        COALESCE(pm.state_market_share, 0.01) AS state_market_share
    FROM cld_subset AS cs
    LEFT JOIN payer_market_share AS pm
        ON cs.state = pm.state
        AND cs.payer_id = pm.payer_id
),

-- Aggregate payer-provider-code level rates to the provider-code level,
-- weighting by the payer market share
agg_provider_code AS (
    SELECT
        state,
        county,
        provider_id,
        provider_name,
        billing_code,
        billing_code_type,
        service_line,
        total_beds,
        COUNT() AS num_rates,
        AVG(canonical_rate) AS mean_rate,
        AVG(medicare_rate) AS mean_medicare,
        AVG(canonical_rate_percent_of_medicare) AS mean_pct_of_medicare,
        SUM(canonical_rate * state_market_share)
        / SUM(state_market_share) AS wtd_mean_rate,
        SUM(medicare_rate * state_market_share)
        / SUM(state_market_share) AS wtd_mean_medicare,
        SUM(
            canonical_rate_percent_of_medicare
            * state_market_share
        )
        / SUM(state_market_share)
            AS wtd_mean_pct_of_medicare
    FROM cld_filled
    -- Keep only provider-code combos with more than 4 rates across all codes
    -- Fewer than this tends to introduce a lot of noise
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8 HAVING COUNT() >= 4 -- noqa
)

SELECT *
FROM agg_provider_code
