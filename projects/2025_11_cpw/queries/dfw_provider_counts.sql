SELECT
    payer_id,
    payer_name,
    payer_network_name,
    COUNT(DISTINCT provider_id) AS provider_count
FROM tq_dev.internal_dev_csong_cld_v2_2_0.prod_combined_abridged
WHERE cbsa_name = 'Dallas-Fort Worth-Arlington, TX'
    AND network_type = 'PPO'
GROUP BY payer_id, payer_name, payer_network_name
