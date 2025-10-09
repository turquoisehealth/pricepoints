-- Keep only commercial rates from hospitals, no MA, no drug codes, no ASCs, etc
WITH cld_subset AS (
    SELECT cld.*
    FROM tq_dev.internal_dev_csong_cld_v2_1_1.prod_combined_all AS cld
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

-- Turn claim counts into a 0-1 percentile rank by state. Used to downweight
-- codes that aren't heavily used
code_utilization AS (
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
),

-- Join the weights data and replace missing weights with a 1st
-- percentile rank (lowest)
cld_filled AS (
    SELECT
        cs.*,
        COALESCE(cu.state_claims_percentile_sep, 0.01)
            AS state_claims_percentile_sep,
        COALESCE(cu.state_claims_percentile_all, 0.01)
            AS state_claims_percentile_all,
        COALESCE(pm.state_market_share, 0.01) AS state_market_share
    FROM cld_subset AS cs
    LEFT JOIN code_utilization AS cu
        ON cs.state = cu.state
        AND cs.billing_code = cu.billing_code
        AND cs.billing_code_type = cu.billing_code_type
    LEFT JOIN payer_market_share AS pm
        ON cs.state = pm.state
        AND cs.payer_id = pm.payer_id
),

-- Extract provider-level information into its own CTE so we don't have to
-- carry it through all the aggregations later
provider_info AS (
    SELECT DISTINCT
        provider_id,
        provider_name,
        zip_code,
        hq_longitude,
        hq_latitude,
        health_system_id,
        health_system_name,
        medicare_provider_id,
        provider_hospital_ownership,
        provider_geographic_classification,
        provider_340b_hospital_type,
        provider_net_patient_revenue,
        provider_cms_overall_rating,
        total_beds,
        nashp_medicare_mix,
        nashp_medicaid_mix,
        nashp_commercial_payer_mix
    FROM cld_filled
),

-- Aggregate payer-provider-code level rates to the payer-provider level,
-- weighting by the code utilization percentiles
agg_payer_provider AS (
    SELECT
        state,
        county,
        provider_id,
        provider_name,
        payer_id,
        ARBITRARY(state_market_share) AS state_market_share,
        COUNT() AS num_rates,
        AVG(canonical_rate) AS mean_rate,
        AVG(medicare_rate) AS mean_medicare,
        AVG(canonical_rate_percent_of_medicare) AS mean_pct_of_medicare,
        SUM(canonical_rate * state_claims_percentile_all)
        / SUM(state_claims_percentile_all) AS wtd_mean_rate,
        SUM(medicare_rate * state_claims_percentile_all)
        / SUM(state_claims_percentile_all) AS wtd_mean_medicare,
        SUM(
            canonical_rate_percent_of_medicare
            * state_claims_percentile_all
        )
        / SUM(state_claims_percentile_all)
            AS wtd_mean_pct_of_medicare
    FROM cld_filled
    -- Keep only payer-provider combos with more than 500 rates across all codes
    -- Fewer than this tends to introduce a lot of noise
    GROUP BY 1, 2, 3, 4, 5 HAVING COUNT() >= 500 -- noqa
),

-- Aggregate the payer-provider level data to the provider level, weighting
-- payers by their state market share
agg_provider AS (
    SELECT
        state,
        county,
        provider_id,
        SUM(num_rates) AS num_rates,
        AVG(mean_rate) AS mean_rate,
        AVG(mean_medicare) AS mean_medicare,
        AVG(mean_pct_of_medicare) AS mean_pct_of_medicare,
        SUM(wtd_mean_rate * state_market_share)
        / SUM(state_market_share) AS wtd_mean_rate,
        SUM(wtd_mean_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_medicare,
        SUM(wtd_mean_pct_of_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_pct_of_medicare
    FROM agg_payer_provider
    GROUP BY 1, 2, 3 -- noqa
),

-- Same aggregations as above, but this time broken out by setting/type
agg_payer_provider_type AS (
    SELECT
        state,
        county,
        provider_id,
        provider_name,
        payer_id,
        billing_code_type,
        ARBITRARY(state_market_share) AS state_market_share,
        COUNT() AS num_rates,
        AVG(canonical_rate) AS mean_rate,
        AVG(medicare_rate) AS mean_medicare,
        AVG(canonical_rate_percent_of_medicare) AS mean_pct_of_medicare,
        SUM(canonical_rate * state_claims_percentile_all)
        / SUM(state_claims_percentile_all) AS wtd_mean_rate,
        SUM(medicare_rate * state_claims_percentile_all)
        / SUM(state_claims_percentile_all) AS wtd_mean_medicare,
        SUM(
            canonical_rate_percent_of_medicare
            * state_claims_percentile_all
        )
        / SUM(state_claims_percentile_all)
            AS wtd_mean_pct_of_medicare
    FROM cld_filled
    -- Lower code count threshold since we've now split by setting
    GROUP BY 1, 2, 3, 4, 5, 6 HAVING COUNT() >= 100 -- noqa
),

agg_provider_type AS (
    SELECT
        state,
        county,
        provider_id,
        billing_code_type,
        SUM(num_rates) AS num_rates,
        AVG(mean_rate) AS mean_rate,
        AVG(mean_medicare) AS mean_medicare,
        AVG(mean_pct_of_medicare) AS mean_pct_of_medicare,
        SUM(wtd_mean_rate * state_market_share)
        / SUM(state_market_share) AS wtd_mean_rate,
        SUM(wtd_mean_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_medicare,
        SUM(wtd_mean_pct_of_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_pct_of_medicare
    FROM agg_payer_provider_type
    GROUP BY 1, 2, 3, 4 -- noqa
),

-- Same aggregations as above, but now with a subset of representative codes
agg_payer_provider_subset AS (
    SELECT
        state,
        county,
        provider_id,
        provider_name,
        payer_id,
        ARBITRARY(state_market_share) AS state_market_share,
        COUNT() AS num_rates,
        AVG(canonical_rate) AS mean_rate,
        AVG(medicare_rate) AS mean_medicare,
        AVG(canonical_rate_percent_of_medicare) AS mean_pct_of_medicare
    FROM cld_filled
    WHERE (
        billing_code_type = 'HCPCS'
        AND billing_code IN (
            '27130',
            '29881',
            '29877',
            '42820',
            '66984',
            '71046',
            '73720',
            '73721',
            '77066'
        )
    ) OR (
        billing_code_type = 'MS-DRG'
        AND billing_code IN (
            '177',
            '195',
            '280',
            '291',
            '343',
            '460',
            '470',
            '743',
            '788',
            '807',
            '871'
        )
    )
    -- Each payer-provider combo should have at least 1/3 of the total codes
    GROUP BY 1, 2, 3, 4, 5 HAVING COUNT() >= 10 -- noqa
),

agg_provider_subset AS (
    SELECT
        state,
        county,
        provider_id,
        SUM(num_rates) AS num_rates,
        AVG(mean_rate) AS mean_rate,
        AVG(mean_medicare) AS mean_medicare,
        AVG(mean_pct_of_medicare) AS mean_pct_of_medicare,
        SUM(mean_rate * state_market_share)
        / SUM(state_market_share) AS wtd_mean_rate,
        SUM(mean_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_medicare,
        SUM(mean_pct_of_medicare * state_market_share)
        / SUM(state_market_share) AS wtd_mean_pct_of_medicare
    FROM agg_payer_provider_subset
    GROUP BY 1, 2, 3 -- noqa
)

SELECT
    ap.state,
    ap.county,
    pi.*,
    ap.num_rates AS all_num_rates,
    ap.mean_rate AS all_mean_rate,
    ap.mean_medicare AS all_mean_medicare,
    ap.mean_pct_of_medicare AS all_mean_pct_of_medicare,
    ap.wtd_mean_rate AS all_wtd_mean_rate,
    ap.wtd_mean_medicare AS all_wtd_mean_medicare,
    ap.wtd_mean_pct_of_medicare AS all_wtd_mean_pct_of_medicare,
    ip.num_rates AS ip_num_rates,
    ip.mean_rate AS ip_mean_rate,
    ip.mean_medicare AS ip_mean_medicare,
    ip.mean_pct_of_medicare AS ip_mean_pct_of_medicare,
    ip.wtd_mean_rate AS ip_wtd_mean_rate,
    ip.wtd_mean_medicare AS ip_wtd_mean_medicare,
    ip.wtd_mean_pct_of_medicare AS ip_wtd_mean_pct_of_medicare,
    op.num_rates AS op_num_rates,
    op.mean_rate AS op_mean_rate,
    op.mean_medicare AS op_mean_medicare,
    op.mean_pct_of_medicare AS op_mean_pct_of_medicare,
    op.wtd_mean_rate AS op_wtd_mean_rate,
    op.wtd_mean_medicare AS op_wtd_mean_medicare,
    op.wtd_mean_pct_of_medicare AS op_wtd_mean_pct_of_medicare,
    sub.num_rates AS sub_num_rates,
    sub.mean_rate AS sub_mean_rate,
    sub.mean_medicare AS sub_mean_medicare,
    sub.mean_pct_of_medicare AS sub_mean_pct_of_medicare,
    sub.wtd_mean_rate AS sub_wtd_mean_rate,
    sub.wtd_mean_medicare AS sub_wtd_mean_medicare,
    sub.wtd_mean_pct_of_medicare AS sub_wtd_mean_pct_of_medicare
FROM agg_provider AS ap
LEFT JOIN
    (
        SELECT * FROM agg_provider_type
        WHERE billing_code_type = 'HCPCS'
    ) AS op
    ON ap.state = op.state
    AND ap.county = op.county
    AND ap.provider_id = op.provider_id
LEFT JOIN
    (
        SELECT * FROM agg_provider_type
        WHERE billing_code_type = 'MS-DRG'
    ) AS ip
    ON ap.state = ip.state
    AND ap.county = ip.county
    AND ap.provider_id = ip.provider_id
LEFT JOIN agg_provider_subset AS sub
    ON ap.state = sub.state
    AND ap.county = sub.county
    AND ap.provider_id = sub.provider_id
LEFT JOIN provider_info AS pi
    ON ap.provider_id = pi.provider_id
