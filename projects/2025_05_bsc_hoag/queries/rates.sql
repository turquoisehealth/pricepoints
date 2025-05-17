SELECT
    cld.state,
    cld.county,
    cld.zip_code,
    cld.cbsa,
    cld.cbsa_name,
    cld.city,
    cld.hq_latitude,
    cld.hq_longitude,
    cld.payer_name,
    cld.payer_id,
    cld.network_id,
    cld.network_name,
    cld.payer_network_name,
    cld.network_type,
    cld.network_class,
    cld.health_system_name,
    cld.provider_id,
    cld.provider_name,
    cld.total_beds,
    cld.bill_type,
    cld.facility,
    cld.billing_code_type,
    cld.billing_code,
    cpt.service_group,
    cld.canonical_rate_original_billing_codes,
    cld.canonical_rate_original_billing_code_type,
    cld.service_description,
    cld.service_line,
    cld.therapeutic_area,
    cld.is_drug_code,
    cld.is_surg_code,
    cld.asp_payment_limit,
    cld.medicare_rate,
    cld.medicare_pricing_type,
    cld.discounted_cash_rate,
    cld.canonical_gross_charge,
    cld.canonical_gross_charge_type,
    cld.canonical_rate,
    cld.canonical_rate_source,
    cld.canonical_rate_type,
    cld.canonical_rate_percent_of_medicare,
    cld.canonical_rate_percent_of_list,
    cld.canonical_rate_validation_method
FROM tq_dev.internal_dev_csong_cld_v1_1.prod_combined_abridged AS cld
LEFT JOIN (
    SELECT DISTINCT
        code AS billing_code,
        description AS service_group
    FROM glue.hospital_data.price_transparency_cpthierarchy
    WHERE level = 1
        AND REGEXP_LIKE(code, '^[0-9]{5}')
) AS cpt
    ON cld.billing_code = cpt.billing_code
WHERE cld.cbsa_name = 'Los Angeles-Long Beach-Anaheim, CA'
    AND cld.canonical_rate IS NOT NULL
    AND cld.canonical_rate_source IN ('hospital', 'payer')
    AND cld.canonical_rate_validation_method != 'outlier'
    AND cld.canonical_rate_percent_of_medicare BETWEEN 0.5 AND 10
    AND LOWER(cld.provider_name) NOT LIKE '%children%'
    AND LOWER(cld.provider_name) NOT LIKE '%cancer%'
    AND (
        (
            cld.billing_code_type = 'MS-DRG'
            AND cld.billing_code IN (
                '807', '788', '871', '743', '439',
                '419', '331', '392', '853', '897'
            )
        ) OR (
            cld.billing_code_type = 'HCPCS'
            AND cld.billing_code IN (
                '45378', '27130', '93452', '93005', '42820',
                '47562', '74176', '99283', '62323', '17110'
            )
        )
    )
