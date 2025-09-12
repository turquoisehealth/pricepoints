SELECT DISTINCT
    cld.state,
    cld.payer_id,
    cld.provider_id,
    cld.provider_name,
    cld.hq_latitude,
    cld.hq_longitude,
    cld.total_beds
FROM tq_dev.internal_dev_csong_cld_v2_0_1.prod_combined_abridged AS cld
WHERE cld.taxonomy_grouping = 'Hospitals'
    AND cld.network_type = 'PPO'
    AND cld.canonical_rate IS NOT NULL
    AND cld.canonical_rate_score >= 3
    AND cld.payer_id IN ( {{ blue_payer_ids }} )
    AND cld.state IN ( {{ blue_states }})
    AND cld.medicare_rate >= 1000
    AND NOT cld.is_drug_code
    AND (
        -- No device rates, drug rates, etc
        (
            cld.billing_code_type = 'HCPCS'
            AND REGEXP_LIKE(cld.billing_code, '^[0-9]{5}$')
        )
        OR cld.billing_code_type = 'MS-DRG'
    )
