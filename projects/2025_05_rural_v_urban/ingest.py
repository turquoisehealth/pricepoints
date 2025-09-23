import random
import string

import polars as pl
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
s3_base_uri = config.get("S3_BASE_URI", "")


###### Functions ###############################################################


@pl.api.register_expr_namespace("util")
class UtilExpr:
    def __init__(self, expr: pl.Expr) -> None:
        self._expr = expr

    def wmean(self, weight: str) -> pl.Expr:
        """
        Compute the weighted mean for an expression, given a weight column name.
        Usage: pl.col("value_col").util.weighted_mean("weight_col")
        """
        weights = pl.when(self._expr.is_not_null()).then(pl.col(weight))
        return weights.dot(self._expr).truediv(weights.sum()).fill_nan(None)


def load_table_with_ctas(
    sql_file: str, s3_location: str | None, table_name: str
) -> pl.DataFrame:
    """
    Load table to Polars by first saving a SQL query to S3 as a Parquet file.

    This is way faster than just directly loading large query results using the
    Trino client.
    """
    if not s3_location or s3_location == "":
        raise ValueError("s3_location must not be empty")

    s3_full_path = f"s3://{s3_location}/{table_name}"
    with open(sql_file, "r") as f:
        sql = (
            f.read()
            .replace("{{ table_name }}", table_name)
            .replace("{{ s3_location }}", s3_full_path)
        )

    with trino_conn.cursor() as cur:
        cur.execute(sql)

    return pl.read_parquet(s3_full_path + "/*")


###### Data loading ############################################################

# Grab basically all hospital rates for the entire United States
rates_df = load_table_with_ctas(
    sql_file="queries/rates.sql",
    s3_location=s3_base_uri,
    table_name="dsnow_tmp_cld_rates_subset"
    + "_"
    + "".join(random.choices(string.ascii_lowercase + string.digits, k=5)),
)

# Grab state-specific code utilization data (procedure counts)
with open("queries/code_utilization.sql", "r") as query:
    code_utilization_df = pl.read_database(query.read(), trino_conn)

# Read county-level crosswalk for translating TQ state-county names to GEOIDs
county_xwalk_df = pl.read_csv(
    source="data/input/county_crosswalk.csv",
    schema_overrides={
        "state": pl.String,
        "county": pl.String,
        "census_county_fips": pl.String,
    },
)

# Read NCHS urban-rural classification codes
nchs_df = (
    pl.read_csv(
        source="data/input/NCHSurb-rural-codes.csv",
        encoding="utf8-lossy",
        columns=["STFIPS", "CTYFIPS", "CODE2023"],
        schema_overrides={
            "STFIPS": pl.String,
            "CTYFIPS": pl.String,
            "CODE2023": pl.String,
        },
    )
    .with_columns(
        pl.col("STFIPS").str.pad_start(2, "0"),
        pl.col("CTYFIPS").str.pad_start(3, "0"),
    )
    .with_columns(
        pl.concat_str(pl.col("STFIPS"), pl.col("CTYFIPS")).alias("geoid")
    )
    .rename({"CODE2023": "nchs_code"})
    .select(["geoid", "nchs_code"])
    .with_columns(
        (
            pl.when(pl.col("nchs_code").is_in(["5", "6"]))
            .then(pl.lit("rural"))
            .otherwise(pl.lit("urban"))
        ).alias("nchs_class")
    )
)

# Read USDS rural-urban continuum codes
usds_df = (
    (
        pl.read_csv(
            source="data/input/Ruralurbancontinuumcodes2023.csv",
            encoding="utf8-lossy",
            schema_overrides={"FIPS": pl.String},
        )
    )
    .filter(pl.col("Attribute") == "RUCC_2023")
    .rename({"Value": "usds_code", "FIPS": "geoid"})
    .select(["geoid", "usds_code"])
    .with_columns(pl.col("geoid").str.pad_start(5, "0"))
    .with_columns(
        (
            pl.when(pl.col("usds_code") >= "6")
            .then(pl.lit("rural"))
            .otherwise(pl.lit("urban"))
        ).alias("usds_class")
    )
)


###### Data cleaning ###########################################################

# Add the payer percentile rank to use as weights. Some rows are missing payer
# info, so we just share the most common value across rows by payer
rates_clean_df = (
    rates_df.with_columns(
        pl.col("national_payer_covered_lives")
        .fill_null(pl.col("national_payer_covered_lives").mode())
        .over("payer_id")
    )
    .with_columns(
        (
            (pl.col("national_payer_covered_lives").rank(method="dense") - 1)
            / (pl.col("national_payer_covered_lives").unique().len() - 1)
        ).alias("national_payer_covered_lives_percentile")
    )
    .with_columns(pl.col("national_payer_covered_lives").fill_null(0.01))
)

# Attach state-specific code utilization data. If missing (fairly common),
# assume extremely low utilization (1% percentile rank)
rates_clean_df = rates_clean_df.join(
    code_utilization_df,
    on=["state", "billing_code_type", "billing_code"],
    how="left",
).with_columns(
    pl.col("state_claims_percentile_sep").fill_null(0.01),
    pl.col("state_claims_percentile_all").fill_null(0.01),
)

# Join county codes and rural-urban classifications
rates_clean_df = (
    rates_clean_df.join(
        county_xwalk_df,
        on=["state", "county"],
        how="left",
    )
    .join(
        nchs_df,
        left_on="census_county_fips",
        right_on="geoid",
        how="left",
    )
    .join(
        usds_df,
        left_on="census_county_fips",
        right_on="geoid",
        how="left",
    )
)

rates_clean_df.group_by(["payer_id", "provider_id"]).agg(
    pl.col("canonical_rate_percent_of_medicare")
    .util.wmean("state_claims_percentile_all")
    .alias("rate_mean"),
    pl.len().alias("num_rates"),
    pl.col("national_payer_covered_lives_percentile").first(),
    pl.col("total_beds").first(),
    pl.col("usds_class").first(),
).group_by(["provider_id"]).agg(
    pl.col("rate_mean")
    .util.wmean("national_payer_covered_lives_percentile")
    .alias("rate_mean"),
    pl.col("num_rates").sum().alias("num_rates"),
    pl.col("total_beds").first(),
    pl.col("usds_class").first(),
).group_by(["usds_class"]).agg(
    pl.col("num_rates").sum().alias("num_rates"), pl.col("rate_mean").mean()
)
