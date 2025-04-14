# %% Import Python libraries and set up Trino
import logging

import polars as pl
import requests as r
from census import Census
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
logger = logging.getLogger(__name__)


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
        .cast(pl.Float64)
        .alias("state_market_share")
    )
    .with_columns(
        pl.when(
            (pl.col("state_market_share").is_null())
            | (pl.col("state_market_share") < 0.001)
        )
        .then(pl.col("state_market_share").fill_null(0.005))
        .otherwise(pl.col("state_market_share"))
        .alias("state_market_share")
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


# %% Fetch Census population, housing, and income data
logger.info("Fetching Census data")
cen = Census(config.get("CENSUS_API_KEY"), year=2023)
cen_vars = {"B01001_001E": "total_pop", "B19013_001E": "median_hh_income"}

# State
cen_df_state = pl.DataFrame(
    cen.acs5.state(list(cen_vars.keys()), state_fips="*")
).rename({**cen_vars, "state": "geoid"})

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
            "column_0": "total_pop",
            "column_1": "median_hh_income",
            "column_2": "geoid",
        }
    )
    .with_columns(
        pl.col("total_pop").cast(pl.Float64),
        pl.col("median_hh_income").cast(pl.Float64),
    )
)

# ZCTA
cen_df_zcta = pl.DataFrame(
    cen.acs5.state_zipcode(list(cen_vars.keys()), state_fips="*", zcta="*")
).rename({**cen_vars, "zip code tabulation area": "geoid"})


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
)
