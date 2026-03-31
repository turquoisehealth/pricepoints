SELECT
    pr.tq_payer_payer_id,
    SUM(CAST(pr.covered_lives AS DOUBLE)) AS lives
FROM redshift.reference.policy_reporter_state AS pr
GROUP BY pr.tq_payer_payer_id;
