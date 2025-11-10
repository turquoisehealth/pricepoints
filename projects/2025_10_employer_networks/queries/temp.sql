SELECT plans.*
FROM hive.public_2025_07.compressed_idx_plan AS plans
WHERE
    LOWER(plans.plan_id_type) = 'ein'
    AND plans.file_hash IS NOT NULL
    AND plans.file_type = 'in-network-rates'
    AND plans.plan_market_type = 'group'
    AND plans.plan_id IN ('043099750', '842071583')
LIMIT 100
