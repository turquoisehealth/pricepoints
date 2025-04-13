-- Market share of payers by state. Using lower resolution data doesn't
-- work since the data is (seemingly) state-level to start.
-- Adapted from CLD alpha

WITH policy_reporter AS (
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
    state,
    CAST(payer_id AS BIGINT) AS payer_id,
    CAST(state_market_share AS DOUBLE) AS state_market_share
FROM payer_market_share
