-- Taken from payer DSN status DAG
SELECT
    mrf.data_source_name,
    mrf.state,
    mrf.is_broken,
    COALESCE(meta.is_parsed, FALSE) AS is_parsed,
    COALESCE(mrf.is_duplicate, FALSE) AS is_skipped,
    CASE
        WHEN
            mrf.is_duplicate THEN
        CASE
            WHEN mrf.duplicate_reason = 'bcbs' THEN 'blue deduplication'
            WHEN mrf.duplicate_reason = 'filename' THEN 'same file name'
        END
    END AS skip_reason,
    CASE
        WHEN
            mrf.schema = 'in-network-rates'
            THEN JSON_EXTRACT_SCALAR(meta.s3_paths, '$.inr')
        WHEN
            mrf.schema = 'allowed-amounts'
            THEN JSON_EXTRACT_SCALAR(meta.s3_paths, '$.aa')
        WHEN
            mrf.schema = 'table-of-contents'
            THEN JSON_EXTRACT_SCALAR(meta.s3_paths, '$.idx')
    END AS s3_key,
    COALESCE(meta.has_error, FALSE) AS has_error,
    CASE WHEN meta.has_error THEN meta.process_type END AS error_process,
    CASE
        WHEN meta.has_error THEN COALESCE(meta.exception, meta.exception_info)
    END AS error_details,
    COALESCE(meta.is_parsed, FALSE) AS is_in_trino,
    mrf.schema,
    mrf.payer_id,
    payer.name AS payer_name,
    CAST(COUNT(loc.mrf_id) AS INTEGER) AS expected_location_files,
    CAST(COUNT(CASE WHEN loc.is_parsed THEN loc.mrf_id END) AS INTEGER)
        AS parsed_location_files,
    CASE
        WHEN COUNT(loc.mrf_id) > 0
            THEN CAST(
                100
                * COUNT(CASE WHEN loc.is_parsed THEN loc.mrf_id END)
                / COUNT(loc.mrf_id) AS INTEGER
            )
    END AS percent_parsed_location_files,
    meta.started_at AS parse_started_at,
    meta.finished_at AS parsed_finished_at,
    meta.duration_hr AS parse_duration,
    meta.file_size_raw AS raw_file_size_bytes,
    mrf.version
FROM aurora.data_bistro.payer_mrf AS mrf
LEFT JOIN aurora.data_bistro.payer_mrf_metrics_parser AS meta
    ON mrf.id = meta.mrf_id
LEFT JOIN aurora.data_bistro.payer_mrf_location AS loc
    ON mrf.id = loc.mrf_id
LEFT JOIN aurora.data_bistro.payer AS payer
    ON mrf.payer_id = payer.id
WHERE
    mrf.version = '2026-03'
    AND mrf.payer_id <> 510
GROUP BY
    mrf.data_source_name,
    mrf.state,
    mrf.is_broken,
    meta.is_parsed,
    mrf.is_duplicate,
    mrf.duplicate_reason,
    meta.s3_paths,
    meta.has_error,
    meta.process_type,
    meta.exception,
    meta.exception_info,
    mrf.schema, mrf.payer_id, payer.name,
    meta.started_at,
    meta.finished_at,
    meta.duration_hr,
    meta.file_size_raw,
    mrf.version
