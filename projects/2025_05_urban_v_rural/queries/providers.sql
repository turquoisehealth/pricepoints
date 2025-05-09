SELECT
    id,
    hp_longitude AS lon,
    hp_latitude AS lat
FROM glue.hospital_data.hospital_provider
WHERE hospital_type IN (
        'Short Term Acute Care Hospital',
        'Critical Access Hospital'
    )
