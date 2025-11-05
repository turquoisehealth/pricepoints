WITH f5500 AS (
    SELECT f5500_by_date.*
    FROM (
        SELECT DISTINCT
            COALESCE(
                NULLIF(spons_dfe_dba_name, ''),
                NULLIF(sponsor_dfe_name, '')
            ) AS employer_name,
            spons_dfe_ein,
            spons_dfe_mail_us_address1 AS employer_address,
            spons_dfe_mail_us_city AS employer_city,
            spons_dfe_mail_us_state AS employer_state,
            spons_dfe_mail_us_zip AS employer_zip,
            COALESCE(type_welfare_bnft_code LIKE '%4A%', FALSE)
                AS has_health_benefit,
            COALESCE(CAST(benefit_gen_asset_ind AS BOOLEAN), FALSE)
            OR COALESCE(CAST(benefit_trust_ind AS BOOLEAN), FALSE)
            OR COALESCE(CAST(funding_gen_asset_ind AS BOOLEAN), FALSE)
            OR COALESCE(CAST(funding_trust_ind AS BOOLEAN), FALSE)
                AS has_non_insurance_arrangement,
            COALESCE(CAST(benefit_insurance_ind AS BOOLEAN), FALSE)
            OR COALESCE(CAST(funding_insurance_ind AS BOOLEAN), FALSE)
                AS has_insurance_arrangement,
            COALESCE(CAST(sch_a_attached_ind AS BOOLEAN), FALSE) AS has_sched_a,
            tot_active_partcp_cnt,
            ROW_NUMBER()
                OVER (
                    PARTITION BY spons_dfe_ein
                    ORDER BY form_tax_prd DESC
                )
                AS rn
        FROM
            redshift.reference.ref_form_5500
        WHERE tot_active_partcp_cnt >= 5000
    ) AS f5500_by_date
    -- Keep only the most recent filing for each EIN
    WHERE f5500_by_date.rn = 1
),

f5500_classified AS (
    SELECT
        *,
        CASE
            WHEN NOT has_health_benefit
                THEN 'not_health'
            -- Strong fully-funded signal if ONLY insurance
            -- is checked and a Schedule A exists
            WHEN has_insurance_arrangement
                AND NOT has_non_insurance_arrangement
                AND has_sched_a
                THEN 'fully_funded'
            -- Decent self-funded signal if trust/general-assets
            -- and NO insurance arrangement
            WHEN has_non_insurance_arrangement
                AND NOT has_insurance_arrangement
                THEN 'self_funded'
            -- Mixed checkboxes and schedule A means ambiguous
            ELSE 'ambiguous'
        END AS funding_type_inferred
    FROM f5500
)

SELECT DISTINCT
    f5500_classified.*,
    plans.reporting_entity_type,
    plans.plan_name,
    plans.plan_id,
    plans.plan_id_type,
    plans.plan_market_type,
    plans.payer_name,
    plans.payer_id
FROM
    hive.public_2025_07.compressed_idx_plan AS plans
INNER JOIN f5500_classified
    ON
    plans.plan_id = f5500_classified.spons_dfe_ein
WHERE
    LOWER(plans.plan_id_type) = 'ein'
    AND plans.file_hash IS NOT NULL
    AND plans.file_type = 'in-network-rates'
    AND plans.plan_market_type = 'group'
    AND plans.reporting_entity_type IN (
        -- 'third party administrator',
        -- 'self-insured plan',
        'health insurance issuer'
    )
    AND f5500_classified.funding_type_inferred IN (
        'self_funded',
        'fully_funded'
    )
