SELECT
    payer_id,
    file_hash,
    compressed_rates_files_references_file_count,
    compressed_rates_files_references_record_count
FROM hive.public_2026_02.meta_row_count_by_file_hash;
