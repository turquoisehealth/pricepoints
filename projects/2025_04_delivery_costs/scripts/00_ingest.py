import polars as pl
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()

# Grab all delivery prices from providers (i.e. hospitals)
with open("queries/rates.sql", "r") as query:
    rates_df = pl.read_database(query.read(), trino_conn)

# Grab payer covered lives to use for weighting during aggregations
with open("queries/payer_stats.sql", "r") as query:
    payer_stats_df = pl.read_database(query.read(), trino_conn)

# Join payer stats to the provider-level rates data, and fill
# any missing payers with super low market share
rates_df = rates_df.join(
    payer_stats_df, on=["payer_id", "state"], how="left"
).with_columns(
    state_market_share=pl.when(
        (pl.col("state_market_share").is_null())
        | (pl.col("state_market_share") == 0)
    )
    .then(pl.col("state_market_share").fill_null(0.005))
    .otherwise(pl.col("state_market_share"))
)

# Replicate some Turquoise outlier trimming methods, since they aren't
# applied to the full set of rates
# https://turquoisehealth.zendesk.com/hc/en-us/articles/31190981752603-Outlier-Management-in-hospital-rates
rates_clean_df = rates_df.filter(
    (pl.col("final_rate_amount") / pl.col("medicare_rate") >= 0.6)
    & (pl.col("final_rate_amount") / pl.col("medicare_rate") <= 10.0)
    & pl.col("final_rate_amount").is_between(3000, 500_000)
    & pl.col("final_rate_amount").is_not_nan()
)

# If multiple rates with exist for the same provider-payer-plan-code
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
rates_clean_df = (
    rates_clean_df.with_columns(
        pl.col("final_rate_type").cast(rates_type_rank)
    )
    .sort(rates_sort_cols, nulls_last=True)
    .unique(rates_sort_cols, keep="first")
)

# Keep only common revenue codes related to inpatient stays
rates_clean_df = rates_clean_df.filter(
    (pl.col("revenue_code").str.contains("^[1-2][0-9]{2}$"))
    & (pl.col("revenue_code").count().over("revenue_code") > 10)
    | (pl.col("revenue_code").is_null())
)

# Collapse the negotiated rates to the mean across all revenue codes
# by provider-payer-plan-code, prioritizing the mean of only NULL revenue
# code rates (if there are any)
rates_clean_df = (
    rates_clean_df.group_by(rates_sort_cols[:-2], maintain_order=True)
    .agg(
        pl.all().exclude(rates_sort_cols[:-2] + ["final_rate_amount"]).first(),
        pl.col("final_rate_amount").mean().alias("final_rate_amount_all_rc"),
        pl.col("final_rate_amount")
        .filter(pl.col("revenue_code").is_null())
        .mean()
        .alias("final_rate_amount_null_rc"),
    )
    .with_columns(
        final_rate_amount=pl.when(
            pl.col("final_rate_amount_null_rc").is_not_null()
        )
        .then(pl.col("final_rate_amount_null_rc"))
        .otherwise(pl.col("final_rate_amount_all_rc"))
        .round(2)
    )
)
rates_clean_df.write_parquet("data/rates_clean.parquet")
