SELECT
    inr.payer_id,
    sp.payer_name,
    inr.data_source_name,
    inr.reporting_entity_name,
    inr.reporting_entity_type,
    inr.issuer_name,
    inr.plan_sponsor_name,
    inr.version
FROM hive.public_2026_02.inr AS inr
LEFT JOIN tq_production.spines.spines_payer AS sp
    ON CAST(inr.payer_id AS VARCHAR) = sp.payer_id;
