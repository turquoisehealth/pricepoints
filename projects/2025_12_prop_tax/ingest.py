import os

import polars as pl
import requests
from dotenv import load_dotenv
from polars_readstat import scan_readstat
from tq.connectors import get_trino_connection

load_dotenv()
SC_HEADERS = {"X-App-Token": os.getenv("SOCRATA_APP_TOKEN")}

trino_conn = get_trino_connection()


##### Load acquisitions data ###################################################

# Manually gathered data, combo of Google site search (Beckers) + Deep Research
hospital_acquisitions_df = pl.read_csv(
    "data/input/hospital_acquisitions.csv",
    schema_overrides={"hospital_id": pl.String},
)

# Grab extra data from TQ database and attach to the manually gathered data
with open("queries/hospital_info.sql", "r") as query:
    provider_ids = ",".join(
        f"'{val}'"
        for val in hospital_acquisitions_df["hospital_id"].unique().to_list()
    )
    sql_tempalte = query.read()
    sql = sql_tempalte.replace("{{ provider_ids }}", provider_ids)
    hospital_info_df = pl.read_database(sql, trino_conn)

hospital_acquisitions_df = hospital_acquisitions_df.join(
    hospital_info_df, how="left", left_on="hospital_id", right_on="provider_id"
)

hospital_acquisitions_df.write_parquet("data/output/hospital_info.parquet")


##### Cook PIN processing ######################################################

# Ingest a list of possible MacNeal hospital pins pulled
# from the Cook County web map
macneal_pins_df_possible = pl.read_csv(
    "data/input/macneal_pins_possible.csv", schema_overrides={"pin": pl.String}
)
macneal_pins_str_possible = "', '".join(
    macneal_pins_df_possible["pin"].to_list()
)

# Use the Cook County mailing address data to grab the mailing address for each
# of the possible PINs
cook_address_endpoint = (
    "https://datacatalog.cookcountyil.gov/api/v3/views/3723-97qp/query.json"
)
cook_address_query = {
    "query": "SELECT DISTINCT pin, mail_address_name"
    + f" WHERE pin IN ('{macneal_pins_str_possible}')"
}
cook_address_response = requests.post(
    cook_address_endpoint, headers=SC_HEADERS, json=cook_address_query
)

# Keep only PINs with a mailing address related to Loyola or MacNeal hospital
macneal_pins_df_confirmed = (
    pl.read_json(cook_address_response.content)
    .filter(
        pl.col("mail_address_name").str.contains_any(
            ["MAC", "LOYOLA", "EXEMPT"]
        )
    )
    .unique("pin")
)
macneal_pins_df_confirmed.write_csv(
    "data/intermediate/macneal_pins_confirmed.csv"
)


##### Load CMS cost reports ####################################################

# Load all cost report data from the SAS files at:
# https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/hospital-2552-2010-form
cost_reports_dir = "data/input/cost_reports"
cost_reports_dfs = []
for file in sorted(os.listdir(cost_reports_dir)):
    if file.endswith(".sas7bdat"):
        year = int(file.replace("prds_hosp10_yr", "").replace(".sas7bdat", ""))
        df = scan_readstat(f"{cost_reports_dir}/{file}")
        df = df.rename(lambda x: x.lower())
        df = df.with_columns(pl.lit(year).alias("year"))
        cost_reports_dfs.append(df)

cost_reports_df = (
    pl.concat(cost_reports_dfs, how="diagonal")
    .select(
        pl.col("prvdr_num").alias("mcr_ccn"),
        pl.col("fy_end_dt").alias("mcr_fy_end_date"),
        pl.col("c_1_c5_73").alias("mcr_drug_cost"),
        pl.col("c_1_c6_73").alias("mcr_inpatient_drug_charged"),
        pl.col("c_1_c7_73").alias("mcr_outpatient_drug_charged"),
        (pl.col("c_1_c6_73") + pl.col("c_1_c7_73")).alias("mcr_drug_charged"),
        (
            pl.col("c_1_c5_73") / (pl.col("c_1_c6_73") + pl.col("c_1_c7_73"))
        ).alias("mcr_drug_ccr"),
        (
            pl.col("c_1_c7_73") / (pl.col("c_1_c6_73") + pl.col("c_1_c7_73"))
        ).alias("mcr_pct_outpatient"),
        pl.col("g3_c1_3").alias("mcr_net_patient_revenue"),
        (pl.col("c_1_c6_202") + pl.col("c_1_c7_202")).alias(
            "mcr_gross_charges"
        ),
        (
            pl.col("g3_c1_3") / (pl.col("c_1_c6_202") + pl.col("c_1_c7_202"))
        ).alias("mcr_conv_factor"),
        pl.col("e_a_hos_c1_33").alias("mcr_dsh_pct"),
        pl.col("year"),
    )
    .collect()
)

cost_reports_fil_df = cost_reports_df.join(
    hospital_acquisitions_df.select(["hospital_id", "provider_name", "ccn"]),
    how="right",
    left_on="mcr_ccn",
    right_on="ccn",
).sort(["hospital_id", "year"])

cost_reports_fil_df.write_parquet("data/output/cost_reports.parquet")
