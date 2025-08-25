import polars as pl
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()


###### Data loading ############################################################

# Load flatfile of all Blues by state, then keep only states with 2+ Blues
blues_df = pl.read_csv(
    "data/blues.csv", schema_overrides={"state_fips": pl.String}
)
blues_twos_df = blues_df.filter(pl.len().over("state_fips") >= 2)
blues_twos_payer_ids = ",".join(
    f"'{val}'" for val in blues_twos_df["tq_payer_id"].unique().to_list()
)
blues_twos_states = ",".join(
    f"'{val}'" for val in blues_twos_df["state_name"].unique().to_list()
)

# Grab rates for states with multiple Blues
with open("queries/blue_rates.sql", "r") as query:
    sql_template = query.read()
    sql = sql_template.replace("{{ blue_payer_ids }}", blues_twos_payer_ids)
    sql = sql.replace("{{ blue_states }}", blues_twos_states)
    blue_rates_df = pl.read_database(sql, trino_conn)

with open("queries/blue_providers.sql", "r") as query:
    sql_template = query.read()
    sql = sql_template.replace("{{ blue_payer_ids }}", blues_twos_payer_ids)
    sql = sql.replace("{{ blue_states }}", blues_twos_states)
    blue_providers_df = pl.read_database(sql, trino_conn)


###### Data cleaning ###########################################################

# Drop rates that are from out-of-state Blues (rare, but it happens). Also
# deduplicate multiple networks from the same Blue payer in the same state
# i.e. Premera has a WA PPO and a OR PPO in Washington
blue_rates_df_clean = blue_rates_df.join(
    blues_df.select(["tq_payer_id", "state_name"])
    .with_columns(pl.col("tq_payer_id").cast(pl.String))
    .unique(),
    left_on=["payer_id", "state"],
    right_on=["tq_payer_id", "state_name"],
    how="inner",
).unique(
    [
        "state",
        "provider_id",
        "billing_code_type",
        "billing_code",
        "payer_id",
        "canonical_rate",
    ]
)

# Drop some outliers not caught by CLD using % of Medicare rules
blue_rates_df_clean = blue_rates_df_clean.filter(
    pl.col("canonical_rate_percent_of_medicare") >= 0.7,
    pl.col("canonical_rate_percent_of_medicare") <= 100.0,
)

# For each provider-state-code combination, keep the highest scored rate of
# each payer, then the min and max if any additional rows remain. Finally,
# keep only rates that have a provider-code pair
blue_rates_cols = ["state", "provider_id", "billing_code_type", "billing_code"]
blue_rates_df_clean = (
    blue_rates_df_clean.filter(
        pl.col("canonical_rate_score")
        == pl.col("canonical_rate_score")
        .max()
        .over(blue_rates_cols + ["payer_id"])
    )
    .filter(
        pl.all_horizontal(
            (pl.col("canonical_rate") == pl.col("canonical_rate").max())
            | (pl.col("canonical_rate") == pl.col("canonical_rate").min()),
            pl.col("canonical_rate").is_first_distinct(),
        ).over(blue_rates_cols)
    )
    .filter(pl.n_unique("payer_id").over(blue_rates_cols) >= 2)
).sort(blue_rates_cols)

# For each pair, calculate the absolute difference and absolute percent
# difference between the high and low rate
blue_rates_df_clean = blue_rates_df_clean.with_columns(
    pl.col("canonical_rate")
    .min()
    .over(blue_rates_cols)
    .alias("canonical_rate_min"),
    pl.col("canonical_rate")
    .max()
    .over(blue_rates_cols)
    .alias("canonical_rate_max"),
).with_columns(
    (pl.col("canonical_rate_min") - pl.col("canonical_rate_max"))
    .abs()
    .alias("canonical_rate_diff"),
    (pl.col("canonical_rate_max") / pl.col("canonical_rate_min"))
    .abs()
    .alias("canonical_rate_pct_diff"),
)

# Drop some outlier pairs with super high percentage diffs
blue_rates_df_clean = blue_rates_df_clean.filter(
    pl.col("canonical_rate_pct_diff") <= 10.0
)

# Save rates to Parquet for plotting in R
blue_rates_df_clean.write_parquet("data/blue_rates.parquet")

# Keep only providers with more than one Blue payer
blue_providers_df.filter(
    pl.n_unique("payer_id").over("provider_id") >= 2
).write_parquet("data/blue_providers.parquet")
