WITH keytruda AS (
    SELECT *
    FROM redshift_datahouse.internal_reference.ref_medispan_wac_rx
    WHERE hcpc = 'J9271'
        AND ndc = '00006302601'
        AND rate_type = 'WAC'
),

max_date AS (
    SELECT MAX(price_effective_date) AS price_effective_date
    FROM keytruda
),

key_wac AS (
    SELECT
        ks.hcpc,
        ks.package_price_clean / CAST(ks.strength AS DOUBLE) AS wac
    FROM keytruda AS ks
    INNER JOIN max_date
        ON ks.price_effective_date = max_date.price_effective_date
)

SELECT
    pca.provider_id,
    pca.provider_name,
    AVG(pca.canonical_rate) AS avg_comm_rate,
    ARBITRARY(pca.medicare_rate) AS medicare_rate,
    ARBITRARY(pca.canonical_gross_charge) AS gross_rate,
    ARBITRARY(pca.discounted_cash_rate) / 200 AS cash_rate,
    ARBITRARY(pca.asp_payment_limit) / 1.06 AS asp,
    (ARBITRARY(pca.asp_payment_limit) / 1.06)
    - ((ARBITRARY(pca.asp_payment_limit) / 1.06) * 0.231) AS _340b_rate,
    ARBITRARY(key_wac.wac) AS wac
FROM tq_dev.internal_dev_csong_cld_v1_2_1.prod_combined_abridged AS pca
LEFT JOIN key_wac
    ON pca.billing_code = key_wac.hcpc
WHERE pca.billing_code = 'J9271'
    AND pca.state = 'IL'
    AND pca.canonical_rate IS NOT NULL
    AND pca.provider_name = 'Javon Bea Hospital'
GROUP BY pca.provider_id, pca.provider_name
