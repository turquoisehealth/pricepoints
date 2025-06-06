WITH cbsa_xwalk AS (
    SELECT DISTINCT
        cbsa,
        npi
    FROM redshift.reference.provider_demographics
)

SELECT
    CASE
        -- Convert APR-DRG to MS-DRG using TQ crosswalk
        -- https://www.notion.so/turquoisehealth/MS-DRG-APR-DRG-PRD-157b775c0e97807eb9dcda1641a40580?pvs=4 # -- noqa: LT05
        WHEN hr.billing_code_type IN ('MS-DRG', 'DRG')
            THEN SUBSTR(hr.billing_code, -3)
        WHEN hr.billing_code_type IN ('APR-DRG')
            AND hr.billing_code IN ('5601', '560-1')
            THEN '807'
        WHEN hr.billing_code_type IN ('APR-DRG')
            AND hr.billing_code IN ('5604', '560-4')
            THEN '805'
        ELSE hr.billing_code
    END AS billing_code,
    hr.billing_code_type,
    hr.revenue_code,
    hr.billing_code_modifiers,
    hr.billing_class,
    hr.setting,
    hr.description,
    hr.code_description,
    CASE
        -- There are loads of obvious case rates labeled as 'per diem'
        -- (e.g. a $15K per diem for DRG 807), so set the upper bound for
        -- per diem rates at 3x the Medicare "day rate" for the same DRG
        WHEN hr.contract_methodology = 'per diem'
            AND (
                hr.negotiated_dollar < (hr.medicare_rate / drg.glos) * 3
                OR hr.medicare_rate IS NULL
            )
            AND drg.glos IS NOT NULL
            THEN hr.negotiated_dollar * drg.glos
        -- Lots of rates are labeled as a % TBC but have a negotiated dollar
        -- amount instead. We want to default to using this amount
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_dollar IS NOT NULL
            AND (hr.gross_charge IS NULL OR hr.negotiated_percentage IS NULL)
            THEN hr.negotiated_dollar
        -- Some % TBC contracts use decimal values for the percentages, others
        -- use whole numbers. We want to convert them all to decimal values and
        -- cap the highest possible percentage
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_percentage < 1
            AND hr.gross_charge IS NOT NULL
            THEN hr.gross_charge * hr.negotiated_percentage
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_percentage >= 1
            AND hr.gross_charge IS NOT NULL
            THEN ROUND(CAST(
                hr.gross_charge * LEAST(hr.negotiated_percentage, 110) AS DOUBLE
            ) / 100)
        -- Use estimated allowed amount if nothing else is available
        WHEN hr.contract_methodology = 'other'
            AND hr.estimated_allowed_amount > 0
            AND hr.estimated_allowed_amount <= 10000000
            THEN hr.estimated_allowed_amount
        ELSE hr.negotiated_dollar
    END AS final_rate_amount,
    CASE
        WHEN hr.contract_methodology = 'per diem'
            AND (
                hr.negotiated_dollar < (hr.medicare_rate / drg.glos) * 3
                OR hr.medicare_rate IS NULL
            )
            AND drg.glos IS NOT NULL
            THEN 'per diem'
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_dollar IS NOT NULL
            AND (hr.gross_charge IS NULL OR hr.negotiated_percentage IS NULL)
            THEN 'percent of total billed charges'
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_percentage < 1
            AND hr.gross_charge IS NOT NULL
            THEN 'percent of total billed charges'
        WHEN hr.contract_methodology = 'percent of total billed charges'
            AND hr.negotiated_percentage >= 1
            AND hr.gross_charge IS NOT NULL
            THEN 'percent of total billed charges'
        WHEN hr.contract_methodology = 'other'
            AND hr.estimated_allowed_amount > 0
            AND hr.estimated_allowed_amount <= 10000000
            THEN 'estimated allowed amount'
        WHEN hr.contract_methodology = 'case rate'
            AND hr.negotiated_dollar IS NOT NULL
            THEN 'case rate'
        WHEN hr.contract_methodology = 'fee schedule'
            AND hr.negotiated_dollar IS NOT NULL
            THEN 'fee schedule'
        ELSE 'other'
    END AS final_rate_type,
    CAST(hr.negotiated_dollar AS DOUBLE) AS negotiated_dollar,
    CAST(hr.negotiated_percentage AS DOUBLE) AS negotiated_percentage,
    CAST(hr.gross_charge AS DOUBLE) AS gross_charge,
    CAST(hr.discounted_cash_rate AS DOUBLE) AS discounted_cash_rate,
    CAST(hr.medicare_rate AS DOUBLE) AS medicare_rate,
    hr.medicare_pricing_type,
    hr.negotiated_algorithm,
    CAST(hr.estimated_allowed_amount AS DOUBLE) AS estimated_allowed_amount,
    CAST(hr.min_standard_charge AS DOUBLE) AS min_standard_charge,
    CAST(hr.max_standard_charge AS DOUBLE) AS max_standard_charge,
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
    state.state_fips_code AS geoid_state,
    county.state_fips_code || county.county_fips_code AS geoid_county,
    cbsa.cbsa AS geoid_cbsa,
    hp.zip_code AS geoid_zcta,
    hp.total_beds,
    hp.hq_longitude AS lon,
    hp.hq_latitude AS lat,
    cmsq.hospital_overall_rating AS star_rating

-- Get hospital rates of 2025-05-06
FROM glue.hospital_data.hospital_rates AS hr
LEFT JOIN glue.hospital_data.hospital_provider AS hp
    ON hr.provider_id = hp.id
LEFT JOIN glue.hospital_data.price_transparency_state AS state
    ON hp.state = state.state_postal_abbreviation
LEFT JOIN glue.hospital_data.price_transparency_county AS county
    ON state.state_fips_code = county.state_fips_code
    AND hp.county = county.name
LEFT JOIN cbsa_xwalk AS cbsa
    ON hp.npi = cbsa.npi
LEFT JOIN redshift.reference.ref_cms_msdrg AS drg
    ON hr.billing_code = drg.msdrg
LEFT JOIN hive.labps.quality_cms_hospital_ratings_v0 AS cmsq
    ON hr.provider_id = CAST(cmsq.provider_id AS VARCHAR)
WHERE NOT hr.rate_is_outlier
    AND hr.provider_npi IS NOT NULL
    AND hr.payer_class_name = 'Commercial'
    AND hr.setting = 'Inpatient'
    AND COALESCE(
        hr.hospital_type IN (
            'Short Term Acute Care Hospital',
            'Critical Access Hospital'
            -- Technically, childen's hospitals can/do perform L&D, but not all
            -- of them. Easier to exclude these as a whole class
            -- 'Childrens Hospital'
        ),
        TRUE
    )
    -- Keep only the most common contracting methods
    AND (
        (
            hr.contract_methodology = 'other'
            AND (
                hr.negotiated_dollar IS NOT NULL
                OR hr.estimated_allowed_amount IS NOT NULL
            )
        ) OR hr.contract_methodology IN (
            'per diem',
            'percent of total billed charges',
            'case rate',
            'fee schedule'
        )
    )
    -- Drop crazy high gross charges for % TBC contracts
    AND NOT COALESCE(
        hr.gross_charge > 500000.0
        AND hr.contract_methodology = 'percent of total billed charges',
        FALSE
    )
    AND COALESCE(hr.negotiated_percentage <= 110, TRUE)
    -- Drop per diem rates that are significantly lower than the Medicare
    -- "day rate" for the same DRG
    AND NOT COALESCE(
        hr.contract_methodology = 'per diem'
        AND hr.negotiated_dollar < (hr.medicare_rate / drg.glos) * 0.5,
        FALSE
    )
    -- Drop rates where the negotiated value exceeds the list price,
    -- as long as the list price is reasonable (i.e. not super low or high)
    AND NOT COALESCE(
        hr.negotiated_dollar > hr.gross_charge * 1.1
        AND hr.gross_charge * 1.1
        BETWEEN hr.medicare_rate * 0.6 AND hr.medicare_rate * 10,
        FALSE
    )
    -- Get all delivery-related MS-DRGs, APR-DRGs, and revenue codes
    AND (
        (
            COALESCE(
                hr.billing_code_type IN ('MS-DRG', 'DRG', 'APR-DRG'),
                TRUE
            )
            AND SUBSTR(hr.billing_code, -3) IN (
                -- Cesarean Section with Sterilization
                '783', '784', '785',
                -- Cesarean Section without Sterilization
                '786', '787', '788',
                -- Vaginal Delivery with Sterilization and/or D&C
                '796', '797', '798',
                -- Vaginal Delivery without Sterilization/D&C
                '805', '806', '807'
            )
        )
        OR (
            COALESCE(
                hr.billing_code_type IN ('APR-DRG'),
                TRUE
            )
            AND hr.billing_code IN (
                -- Can't use these two since the severity isn't specified
                -- '560-', '560—',
                '560-1',
                '560-4',
                '5601',
                '5604'
            )
        )
    )
