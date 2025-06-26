# type: ignore
# %% Import Python libraries and set up Trino
import re

import polars as pl
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()


# Mini-classes for attaching utility methods to Polars DataFrames
@pl.api.register_dataframe_namespace("util")
class UtilDataFrame:
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def to_snake_case(self: pl.DataFrame) -> pl.DataFrame:
        """Convert all column names to snake_case."""
        return self._df.rename(
            {
                col: re.sub(r"[^a-zA-Z0-9]", "_", col.lower())
                for col in self._df.columns
            }
        )

    def empty_strings_to_null(self: pl.DataFrame) -> pl.DataFrame:
        """Convert all empty string columns to null."""
        return self._df.with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )


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


###### Data loading ############################################################

# %% Grab Illinois hospital IDs, bed counts, locations from Turquoise
with open("queries/il_hospitals.sql", "r") as query:
    il_hospitals_df = pl.read_database(query.read(), trino_conn)

# %% Grab Keytruda drug pricing example
with open("queries/price_example.sql", "r") as query:
    price_example_df = pl.read_database(query.read(), trino_conn)

# %% Grab Keytruda drug pricing example
with open("queries/drug_rates.sql", "r") as query:
    drug_rates_df = pl.read_database(query.read(), trino_conn)

# %% Grab Medicare cost report data exported from CMS SAS files. Interpolate
# CCN values directly into the SQL query to avoid pulling all cost report data
with open("queries/medicare_cost_reports.sql", "r") as query:
    ccn_values = ",".join(
        f"'{val}'"
        for val in il_hospitals_df["medicare_provider_id"].unique().to_list()
    )
    sql_tempalte = query.read()
    sql = sql_tempalte.replace("{{ ccn_values }}", ccn_values)
    medicare_cost_df = pl.read_database(sql, trino_conn)
    medicare_cost_df = medicare_cost_df.sort(
        "mcr_ccn",
        "mcr_fy_end_date",
        descending=[False, True],
    )

# %% Grab Illinois hospitals profiles data from HSFRB:
# https://hfsrb.illinois.gov/inventories-data.html
# Saved Excel file was manually cleaned to make it parseable with polars
ahq_counts_df = (
    pl.read_excel(source="data/input/il_ahq_2023.xlsx", sheet_name="Counts")
    .util.to_snake_case()
    .util.empty_strings_to_null()
)
ahq_revenue_df = (
    pl.read_excel(source="data/input/il_ahq_2023.xlsx", sheet_name="Revenue")
    .util.to_snake_case()
    .util.empty_strings_to_null()
)

# %% Grab OPAIS data from exported "daily" file: https://340bopais.hrsa.gov/home
opais_ce_df = (
    pl.read_excel(
        source="data/input/340b_opais.xlsx",
        sheet_name="Covered Entities",
        read_options={"header_row": 3},
    )
    .util.to_snake_case()
    .util.empty_strings_to_null()
    .with_columns((pl.col("participating") == "TRUE").alias("participating"))
)
opais_cp_df = (
    pl.read_excel(
        source="data/input/340b_opais.xlsx",
        sheet_name="Contract Pharmacies",
        read_options={"header_row": 3},
    )
    .util.to_snake_case()
    .util.empty_strings_to_null()
    .with_columns((pl.col("participating") == "TRUE").alias("participating"))
)


###### Data cleaning ###########################################################

# %% Grab OPAIS parent hospitals to attach to IL hospital dataframe
opais_ce_parent_cols = {
    "medicare_provider_number": "medicare_provider_id",
    "340b_id": "340b_id",
    "participating_start_date": "340b_start_date",
    "termination_date": "340b_end_date",
    "entity_type": "340b_entity_type",
}
opais_ce_parent_df = (
    opais_ce_df.filter(
        pl.col("parent_340b_id").is_null()
        & pl.col("participating")
        & pl.col("medicare_provider_number").is_not_null()
    )
    .select(list(opais_ce_parent_cols.keys()))
    .rename(opais_ce_parent_cols)
    .unique()
)

# %% Merge OPAIS and Medicare data to IL hospital dataframe, keep only
# the latest report for each hospital
il_hospitals_merged_df = il_hospitals_df.join(
    opais_ce_parent_df,
    on="medicare_provider_id",
    how="left",
).join(
    medicare_cost_df.filter(
        (
            pl.col("mcr_fy_end_date")
            == pl.col("mcr_fy_end_date").max().over("mcr_ccn")
        )
        & (pl.col("mcr_fy_end_date") >= pl.date(2023, 1, 1))
    ),
    left_on="medicare_provider_id",
    right_on="mcr_ccn",
    how="left",
)

# %% Grab OPAIS child sites for all parent hospitals left in the merged data
opais_ce_child_df = opais_ce_df.filter(
    pl.col("parent_340b_id").is_in(set(il_hospitals_merged_df["340b_id"]))
).with_columns(
    pl.concat_str(
        [
            pl.col("street_address_1"),
            pl.col("street_address_2"),
            pl.col("street_address_3"),
            pl.lit(","),
            pl.col("street_city"),
            pl.col("street_state"),
            pl.lit(","),
            pl.col("street_zip"),
        ],
        separator=" ",
        ignore_nulls=True,
    ).alias("street_address_full"),
)

# Same for contract pharmacies, keepin
opais_cp_fil_df = opais_cp_df.filter(
    pl.col("340b_id").is_in(set(il_hospitals_merged_df["340b_id"]))
).with_columns(
    pl.concat_str(
        [
            pl.col("pharmacy_address_1"),
            pl.col("pharmacy_address_2"),
            pl.col("pharmacy_address_3"),
            pl.lit(","),
            pl.col("pharmacy_city"),
            pl.col("pharmacy_state"),
            pl.lit(","),
            pl.col("pharmacy_zip"),
        ],
        separator=" ",
        ignore_nulls=True,
    ).alias("street_address_full"),
)

# %% Add 340b child CE count and contract pharmacy count to the merged dataframe
il_hospitals_merged_df = il_hospitals_merged_df.join(
    opais_ce_child_df.group_by("parent_340b_id")
    .agg(pl.len())
    .rename({"len": "340b_child_count"}),
    left_on="340b_id",
    right_on="parent_340b_id",
    how="left",
).join(
    opais_cp_fil_df.group_by("340b_id")
    .agg(pl.len())
    .rename({"len": "340b_contract_pharma_count"}),
    on="340b_id",
    how="left",
)

# %% Merge Illinois AHQ data and with Turquoise hospital ID
ahq_merged_df = (
    il_hospitals_merged_df.select(["provider_id", "provider_name"])
    .join(
        pl.read_csv("data/input/tq_ahq_crosswalk.csv"),
        left_on="provider_name",
        right_on="tq_provider_name",
        how="left",
    )
    .join(
        ahq_counts_df.select(
            [
                "hospital",
                pl.selectors.starts_with("inpatient", "outpatient"),
            ]
        ).rename(lambda x: x + "_count"),
        left_on="ahq_provider_name",
        right_on="hospital_count",
        how="left",
        suffix="_count",
    )
    .join(
        ahq_counts_df.select(
            [
                "hospital",
                pl.selectors.starts_with("inpatient", "outpatient"),
            ]
        ).rename(lambda x: x + "_revenue"),
        left_on="ahq_provider_name",
        right_on="hospital_revenue",
        how="left",
        suffix="_count",
    )
)

# %% Aggregate drug rates to the provider + code level, weighting by payer
# market share
drug_rates_agg_df = (
    drug_rates_df.with_columns(
        (pl.col("canonical_rate") / pl.col("medicare_rate")).alias(
            "rate_pct_of_medicare"
        ),
        (pl.col("canonical_rate") / pl.col("asp")).alias("rate_pct_of_asp"),
        (pl.col("gross_charge_std") / pl.col("medicare_rate")).alias(
            "gross_pct_of_medicare"
        ),
        (pl.col("gross_charge_std") / pl.col("asp")).alias("gross_pct_of_asp"),
        pl.col("state_market_share")
        .fill_null(0.01)
        .alias("state_market_share"),
    )
    # Drop some remaining outliers that result from whack dose standardization
    .filter(
        (
            (pl.col("gross_pct_of_asp") <= 10)
            | (pl.col("gross_pct_of_asp").is_null())
        )
        & (
            (pl.col("rate_pct_of_medicare") <= 10)
            | (pl.col("rate_pct_of_medicare").is_null())
        )
    )
    # Drop super high and low rates since they mess up % over ASP aggregations
    .filter(
        (pl.col("canonical_rate") >= 1)
        & (pl.col("canonical_rate") <= 250_000)
        & pl.col("count_endc").is_not_null()
    )
    .group_by(["provider_id", "provider_name", "billing_code"])
    .agg(
        # Unweighted averages of rates, gross charges, rate % of ASP,
        # gross % of ASP, and rate % of Medicare
        pl.col("canonical_rate").count().alias("rate_count_uwtd"),
        pl.col("canonical_gross_charge").count().alias("gross_count_uwtd"),
        (
            pl.col("canonical_rate").is_not_null()
            & pl.col("count_enc").is_not_null()
        )
        .sum()
        .alias("rate_count_wtd"),
        (
            pl.col("canonical_gross_charge").is_not_null()
            & pl.col("count_enc").is_not_null()
        )
        .sum()
        .alias("gross_count_wtd"),
        pl.col("canonical_rate").mean().alias("rate_avg_uwtd"),
        pl.col("rate_pct_of_asp").mean().alias("rate_poa_avg_uwtd"),
        pl.col("rate_pct_of_medicare").mean().alias("rate_pom_avg_uwtd"),
        pl.col("canonical_gross_charge").mean().alias("gross_avg_uwtd"),
        pl.col("gross_pct_of_asp").mean().alias("gross_poa_avg_uwtd"),
        pl.col("gross_pct_of_medicare").mean().alias("gross_pom_avg_uwtd"),
        # Same as above, but now weighted by state market share (biggest payer)
        pl.col("canonical_rate")
        .util.wmean("state_market_share")
        .alias("rate_avg_wtd"),
        pl.col("rate_pct_of_asp")
        .util.wmean("state_market_share")
        .alias("rate_poa_avg_wtd"),
        pl.col("rate_pct_of_medicare")
        .util.wmean("state_market_share")
        .alias("rate_pom_avg_wtd"),
        pl.col("canonical_gross_charge")
        .util.wmean("state_market_share")
        .alias("gross_avg_wtd"),
        pl.col("gross_pct_of_asp")
        .util.wmean("state_market_share")
        .alias("gross_poa_avg_wtd"),
        pl.col("gross_pct_of_medicare")
        .util.wmean("state_market_share")
        .alias("gross_pom_avg_wtd"),
        (pl.col("canonical_rate") / pl.col("canonical_gross_charge"))
        .mean()
        .alias("rate_to_gross_avg_uwtd"),
        (pl.col("canonical_rate") / pl.col("canonical_gross_charge"))
        .util.wmean("state_market_share")
        .alias("rate_to_gross_avg_wtd"),
        pl.col("count_enc").first().alias("count_enc"),
    )
)

drug_rates_cols = [
    "rate_avg_uwtd",
    "rate_poa_avg_uwtd",
    "rate_pom_avg_uwtd",
    "gross_avg_uwtd",
    "gross_poa_avg_uwtd",
    "gross_pom_avg_uwtd",
    "rate_avg_wtd",
    "rate_poa_avg_wtd",
    "rate_pom_avg_wtd",
    "gross_avg_wtd",
    "gross_poa_avg_wtd",
    "gross_pom_avg_wtd",
    "rate_to_gross_avg_uwtd",
    "rate_to_gross_avg_wtd",
]

# Replace 0 values in aggregates with nulls
drug_rates_agg_df = drug_rates_agg_df.with_columns(
    [
        pl.when(pl.col(c) == 0).then(None).otherwise(pl.col(c)).alias(c)
        for c in drug_rates_cols
    ]
)

# %% Collapse to the provider-level, weighting by state-level code utilization
drug_rates_provider_df = drug_rates_agg_df.group_by(
    ["provider_id", "provider_name"]
).agg(
    *[
        pl.col(c).sum().alias(c)
        for c in [
            "rate_count_uwtd",
            "gross_count_uwtd",
            "rate_count_wtd",
            "gross_count_wtd",
        ]
    ],
    *[pl.col(c).util.wmean("count_enc").alias(c) for c in drug_rates_cols],
)

##### Save data to file ########################################################

# %% Save finished dataframes to Parquet files
il_hospitals_merged_df.write_parquet("data/output/il_hospitals.parquet")
medicare_cost_df.write_parquet("data/output/medicare_cost_reports.parquet")
ahq_merged_df.write_parquet("data/output/ahq_hospital_stats.parquet")
price_example_df.write_parquet("data/output/price_example.parquet")
drug_rates_provider_df.write_parquet("data/output/drug_rates.parquet")

# %% Save OPAIS CE and contract pharma detailed data
opais_ce_child_df.write_parquet("data/intermediate/opais_ce_child.parquet")
opais_cp_fil_df.write_parquet(
    "data/intermediate/opais_contract_pharmacies.parquet"
)
