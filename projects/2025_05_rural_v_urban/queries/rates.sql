CREATE TABLE hive.tmp.{{ table_name }}
WITH (
    external_location = '{{ s3_location }}', format = 'parquet'
)
AS

SELECT
    cld.roid,
    cld.state,
    cld.county,
    cld.hq_latitude,
    cld.hq_longitude,
    cld.payer_id,
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
    cld.provider_type,
    cld.total_beds,
    cld.bill_type,
    cld.billing_code_type,
    cld.billing_code,
    cld.medicare_rate,
    cld.discounted_cash_rate,
    cld.canonical_gross_charge,
    cld.canonical_rate,
    cld.canonical_rate_source,
    cld.canonical_rate_type,
    cld.canonical_rate_category,
    cld.canonical_rate_class,
    cld.canonical_rate_percent_of_medicare,
    cld.canonical_rate_percent_of_list,
    cld.canonical_contract_methodology,
    cld.canonical_rate_score,
    cld.national_payer_covered_lives
FROM tq_dev.internal_dev_csong_cld_v2_1_1.prod_combined_abridged AS cld
WHERE cld.taxonomy_grouping = 'Hospitals'
    AND cld.network_type = 'PPO'
    AND cld.canonical_rate IS NOT NULL
    AND cld.canonical_rate_score >= 3
    AND NOT cld.is_drug_code
    AND (
        -- No device rates, drug rates, etc
        (
            cld.billing_code_type = 'HCPCS'
            AND REGEXP_LIKE(cld.billing_code, '^[0-9]{5}$')
        )
        OR cld.billing_code_type = 'MS-DRG'
    )
