SELECT
    payer_id,
    payer_name,
    parent_payer_name,
    payer_product_network,
    payer_class_id,
    payer_class_name,
    provider_name,
    provider_npi,
    provider_id,
    hospital_type,
    health_system_name,
    health_system_id,
    billing_code,
    billing_code_type,
    description,
    code_description,
    negotiated_dollar,
    estimated_allowed_amount,
    contract_methodology,
    additional_generic_notes,
    additional_payer_notes,
    CAST(ARRAY_MAX(
        REGEXP_EXTRACT_ALL(additional_generic_notes, '\d+')
    ) AS DOUBLE) AS generic_num,
    CAST(ARRAY_MAX(
        REGEXP_EXTRACT_ALL(additional_payer_notes, '\d+')
    ) AS DOUBLE) AS payer_num
FROM glue.hospital_data.hospital_rates
WHERE NOT (REGEXP_LIKE(
        additional_generic_notes,
        'Hospital System Supply Identifier.*Lawson ID$'
    ) AND additional_payer_notes IS NULL)
    AND NOT (REGEXP_LIKE(
        additional_generic_notes,
        'The contract rate provided is when this service is billed outpatient.'
    ) AND additional_payer_notes IS NULL)
    AND NOT (REGEXP_LIKE(
        additional_generic_notes,
        '^Re-evaluated: |^Lesser of|^\d|Rev \d+ Proc \d+|Term Line \d+$|real time' -- noqa
    ) AND additional_payer_notes IS NULL)
    AND NOT REGEXP_LIKE(additional_payer_notes, '^contract indicates payment')
    AND NOT REGEXP_LIKE(
        LOWER(additional_payer_notes),
        '\d+% of total billed charges|\d+% of charges|\d+% of total charges' -- noqa
    )
    AND (
        (additional_generic_notes IS NULL OR REGEXP_LIKE(
            LOWER(additional_generic_notes),
            'stop|loss|threshold|discount|charges|outlier'
        ))
        OR (additional_payer_notes IS NULL OR REGEXP_LIKE(
            LOWER(additional_payer_notes),
            'stop|loss|threshold|discount|charges|outlier'
        ))
    )
    AND payer_class_name = 'Commercial'
    AND payer_product_network = 'PPO'
    AND hospital_type IN (
        'Short Term Acute Care Hospital', 'Critical Access Hospital'
    )
    AND CAST(payer_id AS VARCHAR) IN ( {{ blue_payer_ids }} )
