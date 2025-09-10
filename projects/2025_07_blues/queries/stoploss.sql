SELECT
    provider_state,
    payer_id,
    payer_name,
    parent_payer_name,
    payer_product_network,
    payer_class_id,
    payer_class_name,
    provider_name,
    plan_name,
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
    negotiated_algorithm,
    additional_generic_notes,
    additional_payer_notes
FROM glue.hospital_data.hospital_rates
WHERE provider_name = 'Riverside Community Hospital'
    AND billing_code = '805'
    AND payer_class_name = 'Commercial'
    AND plan_name = 'COMM'
    AND CAST(payer_id AS VARCHAR) IN ( {{ blue_payer_ids }} )
ORDER BY payer_id
