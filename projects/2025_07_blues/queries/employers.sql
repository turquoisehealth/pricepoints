WITH self_funded_employer_map AS (
    SELECT
        self_funded.spons_dfe_ein,
        self_funded.employer_name,
        self_funded.employer_address,
        self_funded.employer_city,
        self_funded.employer_state,
        self_funded.employer_zip,
        self_funded.tot_active_partcp_cnt
    FROM
        (
            SELECT DISTINCT
                COALESCE(
                    CASE
                        WHEN spons_dfe_dba_name = '' THEN NULL ELSE
                            spons_dfe_dba_name
                    END,
                    CASE
                        WHEN sponsor_dfe_name = '' THEN NULL ELSE
                            sponsor_dfe_name
                    END
                ) AS employer_name,
                spons_dfe_ein,
                spons_dfe_mail_us_address1 AS employer_address,
                spons_dfe_mail_us_city AS employer_city,
                spons_dfe_mail_us_state AS employer_state,
                spons_dfe_mail_us_zip AS employer_zip,
                tot_active_partcp_cnt,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY spons_dfe_ein
                        ORDER BY form_tax_prd DESC
                    )
                    AS rn
            FROM
                redshift.reference.ref_form_5500
            WHERE
                type_welfare_bnft_code LIKE '%4A%'
                AND tot_active_partcp_cnt > 10000
        ) AS self_funded
    WHERE
        self_funded.rn = 1
)

SELECT DISTINCT
    employers.*,
    plans.reporting_entity_type,
    plans.plan_name,
    plans.plan_id,
    plans.plan_id_type,
    plans.plan_market_type,
    plans.payer_name,
    plans.payer_id
FROM
    hive.public_2025_07.compressed_idx_plan AS plans
INNER JOIN self_funded_employer_map AS employers
    ON
    plans.plan_id = employers.spons_dfe_ein
WHERE
    LOWER(plans.plan_id_type) = 'ein'
    AND plans.file_hash IS NOT NULL
    AND plans.file_type = 'in-network-rates'
    AND plans.plan_market_type = 'group'
    AND plans.reporting_entity_type = 'health insurance issuer'
    AND plans.payer_id IN ( {{ blue_payer_ids }} )
ORDER BY employers.tot_active_partcp_cnt DESC
