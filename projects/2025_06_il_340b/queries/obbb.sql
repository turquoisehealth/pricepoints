SELECT
    hp.id AS provider_id,
    hp.state AS mcr_state,
    mcr.prvdr_num AS mcr_ccn,
    mcr.fy_end_dt AS mcr_fy_end_date,
    mcr.g3_c1_3 AS mcr_net_patient_revenue,
    mcr.s3_1_c2_14 AS mcr_total_beds,
    mcr.s2_1_c1_27 AS mcr_urban_rural,
    COALESCE(mcr.s2_1_c1_24, 0) + COALESCE(mcr.s2_1_c2_24, 0)
    + COALESCE(mcr.s2_1_c3_24, 0) + COALESCE(mcr.s2_1_c4_24, 0)
    + COALESCE(mcr.s2_1_c5_24, 0)
    + COALESCE(mcr.s2_1_c6_24, 0) AS mcr_medicaid_days,
    COALESCE(mcr.s3_1_c8_14, 0) + COALESCE(mcr.s3_1_c8_32, 0)
    - (COALESCE(mcr.s3_1_c8_5, 0) + COALESCE(mcr.s3_1_c8_6, 0))
    + COALESCE(mcr.s3_1_c8_30, 0) AS mcr_total_days,
    mcr.e_a_hos_c1_30 AS mcr_dpp_part1,
    mcr.e_a_hos_c1_31 AS mcr_dpp_part2,
    mcr.e_a_hos_c1_32 AS mcr_dpp,
    mcr.e_a_hos_c1_33 AS mcr_dsh_pct
FROM tq_dev.internal_dev_mnajarian.reference_medicare_cost_report_hosp10 AS mcr  -- noqa: LT05
LEFT JOIN glue.hospital_data.hospital_provider AS hp
    ON mcr.prvdr_num = hp.medicare_provider_id
WHERE YEAR(mcr.fy_end_dt) = 2023
