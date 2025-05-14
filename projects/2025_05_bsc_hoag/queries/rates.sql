SELECT
    state,
    county,
    zip_code,
    cbsa,
    cbsa_name,
    city,
    hq_latitude,
    hq_longitude,
    payer_name,
    payer_id,
    network_id,
    network_name,
    payer_network_name,
    network_type,
    network_class,
    health_system_name,
    provider_id,
    provider_name,
    total_beds,
    bill_type,
    facility,
    billing_code_type,
    billing_code,
    canonical_rate_original_billing_codes,
    canonical_rate_original_billing_code_type,
    service_description,
    service_line,
    therapeutic_area,
    is_drug_code,
    is_surg_code,
    asp_payment_limit,
    medicare_rate,
    medicare_pricing_type,
    discounted_cash_rate,
    canonical_gross_charge,
    canonical_gross_charge_type,
    canonical_rate,
    canonical_rate_source,
    canonical_rate_type,
    canonical_rate_percent_of_medicare,
    canonical_rate_percent_of_list,
    canonical_rate_validation_method
FROM tq_dev.internal_dev_csong_cld_v1_1.prod_combined_abridged
WHERE cbsa_name = 'Los Angeles-Long Beach-Anaheim, CA'
    AND canonical_rate IS NOT NULL
    AND canonical_rate_source IN ('hospital', 'payer')
    AND canonical_rate_validation_method != 'outlier'
    AND canonical_rate_percent_of_medicare BETWEEN 0.5 AND 10
    AND LOWER(provider_name) NOT LIKE '%children%'
    AND LOWER(provider_name) NOT LIKE '%cancer%'
    AND (
        (
            billing_code_type = 'MS-DRG'
            AND billing_code IN ('807', '788', '871')
        ) OR (
            billing_code_type = 'HCPCS'
            AND billing_code IN (
                '45378', '27130', '93452', '93005', '42820', '47562'
            )
        )
    )
