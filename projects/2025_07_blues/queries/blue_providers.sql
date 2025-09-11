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
    AND cld.canonical_rate IS NOT NULL
    AND cld.canonical_rate_score >= 2
    AND cld.payer_id IN ( {{ blue_payer_ids }} )
    AND cld.state IN ( {{ blue_states }})
