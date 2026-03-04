SELECT
    payer_id,
    COUNT(*) AS compressed_rates_record_count
FROM hive.public_2026_02.compressed_rates
GROUP BY payer_id;
