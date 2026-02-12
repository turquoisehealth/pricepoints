SELECT
    provider_id,
    provider_name,
    ccn,
    provider_latitude,
    provider_longitude
FROM tq_production.spines.spines_provider_hospitals
WHERE provider_id IN ( {{ provider_ids }} )
