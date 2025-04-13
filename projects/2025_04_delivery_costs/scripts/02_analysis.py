# NOTE
# Many hospitals are missing DRGs. Most don't report
# Ignoring inpatient room and board rates
# Provider ID vs NPI?

# Ingest
# TODO: Grab census data for state, county, CBSA, ZCTA
# TODO: Grab CMS quality
# TODO: Grab maternal mortality

# Aggs
# TODO: Collapse plans by payer (no weight)
# TODO: Collapse payers to provider (by covered lives)
# TODO: Collapse providers to State, CBSA, ZIP, weighted by bed count

# Plots
# - Costs by top 10 CBSAs
# - Map of states, raw then hh income adjusted
# - Map of ZIPs, with travel times
# - Map of variance
# - Cost vs CMS quality
# - Cost vs maternal mortality

# %%
import polars as pl

rates_clean_df = pl.read_parquet("data/rates_clean.parquet")

rates_clean_df_group_cols = [
    "provider_id",
    "billing_code_type",
    "billing_code",
    "payer_id",
    "plan_name",
]
temp = (
    rates_clean_df.group_by(rates_clean_df_group_cols)
    .agg(
        pl.all().exclude(rates_clean_df_group_cols).first(),
        pl.count("final_rate_amount").alias("cnt_rate_payer_plan"),
        pl.mean("final_rate_amount").round(2).alias("avg_rate_payer_plan"),
    )
    .group_by(rates_clean_df_group_cols[:-1])
    .agg(
        pl.all().exclude(rates_clean_df_group_cols[:-1]).first(),
        pl.col("cnt_rate_payer_plan").sum().alias("cnt_rate_payer"),
        pl.mean("final_rate_amount").round(2).alias("avg_rate_payer"),
    )
    .group_by(rates_clean_df_group_cols[:-2])
    .agg(
        pl.all().exclude(rates_clean_df_group_cols[:-2]).first(),
        pl.col("cnt_rate_payer").sum().alias("cnt_rate_provider"),
        pl.mean("final_rate_amount").round(2).alias("avg_rate_provider"),
        pl.col("state_market_share")
        .dot("final_rate_amount")
        .truediv(pl.col("state_market_share").sum())
        .fill_nan(None)
        .round(2)
        .alias("avg_rate_provider_weighted"),
    )
)
