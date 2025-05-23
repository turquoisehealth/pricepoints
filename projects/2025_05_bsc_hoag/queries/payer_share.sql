WITH payer_lives AS (
    SELECT
        pr.tq_payer_payer_id,
        SUM(CAST(pr.covered_lives AS DOUBLE)) AS lives
    FROM redshift.reference.policy_reporter_county AS pr
    WHERE pr.line_of_business = 'Commercial'
        AND pr.state_short = 'CA'
        AND pr.county = 'Orange'
        AND pr.policy_reporter_payer IN (
            'Blue Shield California',
            'Aetna',
            'Anthem',
            'United Healthcare',
            'Cigna',
            'Kaiser Permanente'
        )
    GROUP BY pr.tq_payer_payer_id
),

oc_total AS (
    SELECT SUM(CAST(pr.covered_lives AS DOUBLE)) AS lives
    FROM redshift.reference.policy_reporter_county AS pr
    WHERE pr.line_of_business = 'Commercial'
        AND pr.state_short = 'CA'
        AND pr.county = 'Orange'
)

SELECT
    pl.tq_payer_payer_id AS payer_id,
    pl.lives / oc_total.lives AS payer_market_share
FROM payer_lives AS pl
CROSS JOIN oc_total
