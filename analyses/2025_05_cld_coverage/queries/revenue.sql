-- Grab all unique billing codes present in CLD
WITH cld AS (
    SELECT *
    FROM tq_dev.internal_dev_mnajarian_cld_v1_1.prod_combined_abridged
    WHERE canonical_rate IS NOT NULL
        AND network_class = 'Commercial'
        AND taxonomy_grouping = 'Hospitals'
),

codes AS (
    SELECT DISTINCT
        billing_code,
        billing_code_type,
        service_description,
        service_line
    FROM cld
),

-- Grab revenue by billing code. Yoinked from CLD code:
-- https://github.com/turquoisehealth/airflow_dags/blob/cld-v0.1/dags/core_licensable_data_sub_dag/sql/reference/rate_object_space.sql -- noqa
opps AS (
    SELECT DISTINCT
        hcpcs,
        payment_rate
    FROM redshift.reference.ref_cms_opps_addendum_b
    WHERE eff_start_dt = (
            SELECT MAX(add_b2.eff_start_dt)
            FROM redshift.reference.ref_cms_opps_addendum_b AS add_b2
        )
),

util AS (
    SELECT
        util.billing_code,
        util.billing_code_type,
        SUM(util.total_count_encounters) AS total_encounters
    FROM hive.claims_benchmarks.claims_benchmarks_utilization_national AS util
    WHERE util.taxonomy_grouping = 'Hospitals'
        AND util.claim_type_code = 'institutional'
        AND util.taxonomy_classification = 'General Acute Care Hospital'
    GROUP BY 1, 2, 3
),

allowed_amounts AS (
    SELECT
        billing_code_type,
        billing_code,
        AVG(median_allowed_amount) AS allowed_amount
    FROM
        hive.claims_benchmarks.claims_benchmarks_allowable_national_payerchannel
    WHERE payer_channel = 'Commercial'
        AND npi_source = 'hco'
        AND billing_code_modifier IS NULL
        AND claim_type_code IN ('institutional')
        AND median_allowed_amount IS NOT NULL
    GROUP BY 1, 2, 3
),

revenue_calculated AS (
    SELECT
        ut.billing_code_type,
        ut.billing_code,
        ut.total_encounters AS n_claims,
        aa.allowed_amount,
        ut.total_encounters * aa.allowed_amount AS revenue
    FROM util AS ut
    INNER JOIN allowed_amounts AS aa
        ON ut.billing_code_type = aa.billing_code_type
        AND ut.billing_code = aa.billing_code
    WHERE
        ut.total_encounters * aa.allowed_amount > 0
        AND ut.billing_code_type = 'MS-DRG'
),

filtered_revenue AS (
    SELECT
        spine.service_type AS billing_code_type,
        spine.service_code AS billing_code,
        rc.revenue
    FROM tq_production.spines.spines_services AS spine
    LEFT JOIN revenue_calculated AS rc
        ON spine.service_code = rc.billing_code
        AND rc.billing_code_type = 'MS-DRG'
    WHERE spine.service_type = 'MS-DRG'

    UNION ALL

    SELECT *
    FROM (
        SELECT DISTINCT
            'HCPCS' AS billing_code_type,
            util.billing_code,
            util.total_encounters * opps.payment_rate AS revenue
        FROM opps
        LEFT JOIN util
            ON opps.hcpcs = util.billing_code
            AND REGEXP_LIKE(util.billing_code, '^[0-9]')
            AND opps.payment_rate IS NOT NULL
        ORDER BY util.total_encounters * opps.payment_rate DESC
    )
)

SELECT
    fr.*,
    co.service_description,
    co.service_line
FROM filtered_revenue AS fr
INNER JOIN codes AS co
    ON fr.billing_code = co.billing_code
    AND fr.billing_code_type = co.billing_code_type
ORDER BY fr.billing_code_type ASC, fr.revenue DESC
