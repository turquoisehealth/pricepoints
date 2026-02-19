SELECT
    pr.payer_id,
    sp.payer_name,
    pr.data_source_name,
    ARRAY_AGG(DISTINCT pr.network_name)
    FILTER (WHERE pr.network_name != '') AS network_names
FROM hive.public_2026_02.inr_provider_references AS pr
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(pr.payer_id AS VARCHAR) = sp.payer_id
GROUP BY pr.payer_id, sp.payer_name, pr.data_source_name;
