import polars as pl
import tq

trino_conn = tq.get_trino_connection()


###### Data loading ############################################################

# Grab professional/PG rates and NPIs from the files here:
# https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional
bswh_pro_df_dict = pl.read_excel(
    "data/input/CostPlusWellness.com_C010_RateSheet_BSWH_Professional.xlsx",
    sheet_id=0,
)
# Drop the free-standing imaging rates since they have a different file schema
bswh_pro_fsi_df = bswh_pro_df_dict.pop("FreeStandingImaging")

bswh_pro_df = pl.concat(
    [
        df.util.to_snake_case()
        .rename({"nonfacilty_amount": "nonfacility_amount"}, strict=False)
        .with_columns(type=pl.lit(k))
        for k, df in bswh_pro_df_dict.items()
    ]
)

bswh_pro_providers_df = pl.read_csv(
    "data/input/CostPlusWellness.com_C010_20251104_BaylorScottWhiteHealth_PhysicianGroup.csv"
).with_columns(ein=pl.col("ein").str.replace_all("-", ""))

bswh_pro_providers_npis = ",".join(
    f"'{val}'" for val in bswh_pro_providers_df["npi"].unique().to_list()
)

with open("queries/bswh_pro_rates.sql", "r") as query:
    bswh_pro_tq_rates_df = pl.read_database(
        query.read().replace("{{ bswh_npis }}", bswh_pro_providers_npis),
        trino_conn,
    )
