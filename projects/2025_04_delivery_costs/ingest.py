# type: ignore
# %% Import Python libraries and set up Trino
import logging

import duckdb
import polars as pl
import requests as r
from census import Census
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
logger = logging.getLogger(__name__)

# Get OpenTimes DuckDB database
duckdb_conn = duckdb.connect(database=":memory:")
duckdb_conn.execute("""
  INSTALL httpfs;
  LOAD httpfs;
  ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes;
""")


# %% Grab hospital delivery rate data from Turquoise Health
logger.info("Fetching raw data from Trino")

with open("queries/rates.sql", "r") as query:
    rates_df = pl.read_database(query.read(), trino_conn)

# Grab payer covered lives to use for weighting during aggregations
with open("queries/payer_stats.sql", "r") as query:
    payer_stats_df = pl.read_database(query.read(), trino_conn)

logger.info("Cleaning rates data")
# Join payer stats to the provider-level rates data, and fill any
# missing payers with super low market share i.e. assume they are small
rates_df = (
    rates_df.join(payer_stats_df, on=["payer_id", "geoid_state"], how="left")
    .with_columns(
        pl.col("state_market_share")
        .fill_null(0.005)
        .alias("state_market_share")
    )
    .with_columns(
        pl.col("state_market_share")
        .replace(0, 0.005)
        .alias("state_market_share"),
    )
)

# Replicate some Turquoise outlier trimming methods, since they aren't
# applied to the full set of rates
# https://turquoisehealth.zendesk.com/hc/en-us/articles/31190981752603-Outlier-Management-in-hospital-rates
rates_fil_df = rates_df.filter(
    (pl.col("final_rate_amount") / pl.col("medicare_rate") >= 0.6)
    & (pl.col("final_rate_amount") / pl.col("medicare_rate") <= 10.0)
    & pl.col("final_rate_amount").is_between(3000, 500_000)
    & pl.col("final_rate_amount").is_not_nan()
)

# If multiple rates exist for the same provider-payer-plan-code
# combination, then prioritize by most simple rate type
rates_sort_cols = [
    "provider_id",
    "payer_id",
    "plan_name",
    "payer_product_network",
    "billing_code_type",
    "billing_code",
    "revenue_code",
    "final_rate_type",
]
rates_type_rank = pl.Enum(
    [
        "case rate",
        "percent of total billed charges",
        "per diem",
        "estimated allowed amount",
        "fee schedule",
        "other",
    ]
)
rates_fil_df = (
    rates_fil_df.with_columns(pl.col("final_rate_type").cast(rates_type_rank))
    .sort(rates_sort_cols, nulls_last=True)
    .unique(rates_sort_cols, keep="first")
)

# Keep only revenue codes related to inpatient stays, null revenue codes,
# and revenue codes with more than 10 rates
rates_fil_df = rates_fil_df.filter(
    (
        (pl.col("revenue_code").str.contains("^[1-2][0-9]{2}$"))
        & (pl.col("revenue_code").count().over("revenue_code") > 10)
    )
    | (pl.col("revenue_code").is_null())
)

# Collapse the negotiated rates to the mean across all revenue codes
# by provider-payer-plan-code, prioritizing the mean of only NULL revenue
# code rates first (if there are any)
rates_fil_df = (
    rates_fil_df.group_by(rates_sort_cols[:-2], maintain_order=True)
    .agg(
        pl.all().exclude(rates_sort_cols[:-2] + ["final_rate_amount"]).first(),
        pl.col("final_rate_amount").mean().alias("final_rate_amount_all_rc"),
        pl.col("final_rate_amount")
        .filter(pl.col("revenue_code").is_null())
        .mean()
        .alias("final_rate_amount_null_rc"),
    )
    .with_columns(
        pl.when(pl.col("final_rate_amount_null_rc").is_not_null())
        .then(pl.col("final_rate_amount_null_rc"))
        .otherwise(pl.col("final_rate_amount_all_rc"))
        .round(2)
        .alias("final_rate_amount")
    )
)

# Drop per diem rates that seemingly apply to days after the standard
# length of stay (e.g. 1-2 days for inpatient stays)
rates_fil_df = rates_fil_df.filter(
    ~(
        (pl.col("final_rate_type") == "per diem")
        & (
            pl.col("additional_payer_notes").str.contains(
                "(?i)Days?\\s+[3-9]\\+.*"
            )
        )
    )
)


# %% Fetch Census population, housing, and income data
logger.info("Fetching Census data")
cen = Census(config.get("CENSUS_API_KEY"), year=2023)
cen_vars = {
    "B01001_001E": "total_pop",
    "B19013_001E": "median_hh_income",
    "C27013_003E": "ins_pri_work_full_time",
    "C27013_006E": "ins_pri_work_part_time",
    "C27013_009E": "ins_pri_work_none",
    "C27014_003E": "ins_pub_work_full_time",
    "C27014_006E": "ins_pub_work_part_time",
    "C27014_009E": "ins_pub_work_none",
}


# Mini-class for aggregating insurance columns
@pl.api.register_dataframe_namespace("cen")
class CenDataFrame:
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def ins_agg(self) -> pl.DataFrame:
        return self._df.with_columns(
            (
                pl.sum_horizontal(pl.col("^ins_pri.*$")) / pl.col("total_pop")
            ).alias("pct_ins_pri"),
            (
                pl.sum_horizontal(pl.col("^ins_pub.*$")) / pl.col("total_pop")
            ).alias("pct_ins_pub"),
        ).select(pl.all().exclude("^ins_p.._work.*$"))


# State
cen_df_state = (
    pl.DataFrame(cen.acs5.state(list(cen_vars.keys()), state_fips="*"))
    .rename({**cen_vars, "state": "geoid"})
    .cen.ins_agg()  # type: ignore
)

# County
cen_df_county = (
    pl.DataFrame(
        cen.acs5.state_county(
            list(cen_vars.keys()), state_fips="*", county_fips="*"
        )
    )
    .rename(cen_vars)
    .with_columns(
        pl.concat_str(pl.col("state"), pl.col("county")).alias("geoid")
    )
    .select(pl.exclude(["state", "county"]))
    .cen.ins_agg()
)

# CBSA (no helper method for this one, so use the Census API directly)
cen_request_cbsa = r.get(
    (
        "https://api.census.gov/data/2023/acs/acs5?get="
        f"{','.join(list(cen_vars.keys()))}&for=metropolitan%20statistical%20area/"
        f"micropolitan%20statistical%20area:*&key={config.get('CENSUS_API_KEY')}"
    )
).json()
cen_df_cbsa = (
    pl.DataFrame(cen_request_cbsa[1:])
    .transpose()
    .rename(
        {
            **{
                "column_" + str(i): list(cen_vars.values())[i]
                for i in range(0, len(cen_vars))
            },
            "column_" + str(len(cen_vars)): "geoid",
        }
    )
    .with_columns(
        pl.all().exclude("geoid").cast(pl.Float64),
    )
    .cen.ins_agg()
)

# ZCTA
cen_df_zcta = (
    pl.DataFrame(
        cen.acs5.state_zipcode(list(cen_vars.keys()), state_fips="*", zcta="*")
    )
    .rename({**cen_vars, "zip code tabulation area": "geoid"})
    .cen.ins_agg()
)


# %% Attach Census data to rates and keep only needed columns
logger.info("Joining Census data to rates data")
rates_clean_df = (
    (
        rates_fil_df.join(
            cen_df_state,
            left_on="geoid_state",
            right_on="geoid",
            how="left",
        )
        .join(
            cen_df_county,
            left_on="geoid_county",
            right_on="geoid",
            how="left",
            suffix="_county",
        )
        .join(
            cen_df_cbsa,
            left_on="geoid_cbsa",
            right_on="geoid",
            how="left",
            suffix="_cbsa",
        )
        .join(
            cen_df_zcta,
            left_on="geoid_zcta",
            right_on="geoid",
            how="left",
            suffix="_zcta",
        )
    )
    .rename(
        {
            "total_pop": "total_pop_state",
            "median_hh_income": "median_hh_income_state",
            "pct_ins_pri": "pct_ins_pri_state",
            "pct_ins_pub": "pct_ins_pub_state",
        }
    )
    .with_columns(
        # ZCTAs and counties are sometimes too small to have income data,
        # so use the state median income instead
        pl.when(
            pl.col("median_hh_income_zcta").is_null()
            | (pl.col("median_hh_income_zcta") <= 0)
        )
        .then(pl.col("median_hh_income_state"))
        .otherwise(pl.col("median_hh_income_zcta"))
        .alias("median_hh_income_zcta"),
        pl.when(
            pl.col("median_hh_income_county").is_null()
            | (pl.col("median_hh_income_county") <= 0)
        )
        .then(pl.col("median_hh_income_state"))
        .otherwise(pl.col("median_hh_income_county"))
        .alias("median_hh_income_county"),
    )
).select(
    [
        "provider_id",
        "provider_name",
        "hospital_type",
        "total_beds",
        "star_rating",
        "state_market_share",
        "provider_npi",
        "payer_id",
        "payer_name",
        "parent_payer_name",
        "plan_name",
        "payer_product_network",
        "health_system_name",
        "health_system_id",
        "billing_code_type",
        "billing_code",
        "code_description",
        "revenue_code",
        "billing_code_modifiers",
        "medicare_pricing_type",
        "medicare_rate",
        "final_rate_type",
        "final_rate_amount",
        "additional_generic_notes",
        "additional_payer_notes",
        pl.col("^geoid_.*$"),
        pl.col("^total_pop_.*$"),
        pl.col("^median_hh_income_.*$"),
        pl.col("^pct_ins_.*$"),
    ]
)

rates_clean_df.write_parquet("data/rates_clean.parquet")


# %% Fetch ZIP code travel time data

# Grab ZIP-to-ZIP driving travel times
times_zcta_df = pl.DataFrame(
    duckdb_conn.execute("""
  SELECT origin_id, destination_id, duration_sec
  FROM opentimes.public.times
  WHERE version = '0.0.1'
      AND mode = 'car'
      AND year = '2024'
      AND geography = 'zcta'
      AND duration_sec <= 14400
""").fetchdf()
)

# Get all providers within the catchment of each ZIP
times_provider_df = times_zcta_df.join(
    rates_clean_df.select(
        pl.col("geoid_zcta"),
        pl.col("provider_id"),
    ).unique(),
    left_on="destination_id",
    right_on="geoid_zcta",
    how="inner",
).write_parquet("data/zip_adj_matrix.parquet")
