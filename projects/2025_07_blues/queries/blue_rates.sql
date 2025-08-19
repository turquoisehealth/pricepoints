SELECT
    cld.roid,
    cld.state,
    cld.cbsa,
    cld.cbsa_name,
    cld.hq_latitude,
    cld.hq_longitude,
    cld.payer_name,
    cld.network_id,
    cld.network_name,
    cld.payer_network_name,
    cld.network_type,
    cld.network_class,
    cld.health_system_id,
    cld.health_system_name,
    cld.provider_id,
    cld.provider_name,
    cld.taxonomy_grouping,
    cld.provider_type,
    cld.total_beds,
    cld.bill_type,
    cld.billing_code_type,
    cld.billing_code,
    cld.service_description,
    cld.service_line,
    cld.therapeutic_area,
    cld.medicare_rate,
    cld.medicare_pricing_type,
    cld.medicare_reference_source,
    cld.state_avg_medicare_rate,
    cld.discounted_cash_rate,
    cld.canonical_gross_charge,
    cld.canonical_rate,
    cld.canonical_rate_percent_of_medicare,
    cld.canonical_rate_percent_of_state_avg_medicare,
    cld.canonical_rate_percent_of_list,
    cld.canonical_rate_score,
    cld.payer_id
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
