SELECT
    hr.billing_code,
    hr.billing_code_type,
    hr.revenue_code,
    hr.billing_code_modifiers,
    hr.billing_class,
    hr.setting,
    hr.description,
    hr.code_description,
    hr.negotiated_dollar,
    hr.negotiated_percentage,
    hr.gross_charge,
    hr.discounted_cash_rate,
    hr.medicare_rate,
    hr.medicare_pricing_type,
    hr.negotiated_algorithm,
    hr.estimated_allowed_amount,
    hr.min_standard_charge,
    hr.max_standard_charge,
    hr.contract_methodology,
    hr.additional_generic_notes,
    hr.additional_payer_notes,
    hr.plan_name,
    hr.payer_id,
    hr.payer_name,
    hr.parent_payer_name,
    hr.payer_product_network,
    hr.payer_class_id,
    hr.payer_class_name,
    hr.provider_name,
    hr.provider_npi,
    hr.provider_id,
    hr.hospital_type,
    hr.health_system_name,
    hr.health_system_id,
    hr.rate_is_outlier,
    hr.outlier_reason,
    hp.zip_code AS provider_zip_code,
    hp.total_beds AS provider_total_beds,
    hp.hq_longitude AS provider_lon,
    hp.hq_latitude AS provider_lat
FROM glue.hospital_data.hospital_rates AS hr
LEFT JOIN glue.hospital_data.hospital_provider AS hp
    ON hr.provider_id = hp.id
WHERE NOT hr.rate_is_outlier
    AND hr.provider_npi IS NOT NULL
    AND hr.setting = 'Inpatient'
    AND hr.payer_class_name = 'Commercial'
    AND hr.payer_name != 'Unsorted'
    AND (
        hr.billing_code_type IN ('MS-DRG', 'DRG', 'APR-DRG')
        OR hr.billing_code_type IS NULL
    )
    AND (
        hr.hospital_type IN (
            'Short Term Acute Care Hospital',
            'Critical Access Hospital',
            'Childrens Hospital'
        )
        OR hr.hospital_type IS NULL
    )
    -- All delivery related DRGs
    AND hr.billing_code IN (
        -- Cesarean Section with Sterilization
        '783',
        '784',
        '785',
        -- Cesarean Section without Sterilization
        '786',
        '787',
        '788',
        -- Vaginal Delivery with O.R. Procedure
        '768',
        -- Vaginal Delivery with Sterilization and/or D&C
        '796',
        '797',
        '798',
        -- Vaginal Delivery without Sterilization/D&C
        '805',
        '806',
        '807'
    )
