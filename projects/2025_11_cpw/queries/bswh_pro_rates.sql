SELECT
    cr.payer_name,
    cr.payer_id,
    cr.provider_id,
    cr.provider_name,
    cr.billing_code_type,
    cr.billing_code,
    cr.bill_type,
    cr.service_line,
    cr.service_description,
    cr.medicare_rate,
    cr.canonical_rate
FROM tq_dev.internal_dev_csong_cld_v2_2_0.prod_combined_abridged AS cr
WHERE cr.network_type = 'PPO'
    AND cr.canonical_rate IS NOT NULL
    AND cr.canonical_rate_score >= 3
    AND cr.billing_code_type IN ('MS-DRG', 'HCPCS')
    AND ARRAYS_OVERLAP(cr.npi, ARRAY[{{ bswh_npis }}])
