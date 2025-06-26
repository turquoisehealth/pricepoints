WITH cld AS (
    SELECT DISTINCT
        provider_id,
        provider_is_340b_hospital,
        npi
    FROM tq_intermediate.cld_v1_1.prod_combined_abridged
    WHERE state = 'IL'
)

SELECT DISTINCT
    hp.id AS provider_id,
    hp.provider_name,
    hp.medicare_provider_id,
    hp.county,
    hp.city,
    hp.npi,
    hp.ein,
    hp.health_system_name,
    hp.hospital_type,
    hp.total_beds,
    hp.hq_longitude,
    hp.hq_latitude,
    cld.provider_is_340b_hospital AS cld_340b,
    cld.npi AS cld_npi
FROM glue.hospital_data.hospital_provider AS hp
LEFT JOIN cld
    ON hp.id = cld.provider_id
WHERE hp.state = 'IL'
