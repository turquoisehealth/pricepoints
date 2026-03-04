SELECT DISTINCT
    payer_id,
    compressed_rates_record_count
FROM hive.public_2025_12.meta_row_count_by_payer_id;
