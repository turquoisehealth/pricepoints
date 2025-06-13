SELECT
    prvdr_num AS mcr_ccn,
    fy_end_dt AS mcr_fy_end_date,
    c_1_c5_73 AS mcr_drug_cost,
    c_1_c6_73 AS mcr_inpatient_drug_charged,
    c_1_c7_73 AS mcr_outpatient_drug_charged,
    c_1_c6_73 + c_1_c7_73 AS mcr_drug_charged,
    c_1_c5_73 / (c_1_c6_73 + c_1_c7_73) AS mcr_drug_ccr,
    c_1_c7_73 / (c_1_c6_73 + c_1_c7_73) AS mcr_pct_outpatient,
    g3_c1_3 AS mcr_net_patient_revenue,
    e_a_hos_c1_33 AS mcr_dsh_pct
FROM tq_dev.internal_dev_mnajarian.reference_medicare_cost_report_hosp10
WHERE prvdr_num IN ( {{ ccn_values }} )
