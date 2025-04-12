# %%
import polars as pl

# Load the raw providers data returned from SQL
providers_df = pl.read_parquet("data/providers.parquet")

# Replicate some Turquoise outlier trimming methods, since they aren't
# applied to the full set of rates
# https://turquoisehealth.zendesk.com/hc/en-us/articles/31190981752603-Outlier-Management-in-hospital-rates
providers_clean = providers_df.filter(
    (pl.col("final_rate_amount") / pl.col("medicare_rate") >= 0.6)
    & (pl.col("final_rate_amount") / pl.col("medicare_rate") <= 10.0)
    & pl.col("final_rate_amount").is_between(3000, 500_000)
    & pl.col("final_rate_amount").is_not_nan()
)

# If multiple rates with exist for the same provider-payer-plan-code
# combination, then prioritize by most simple rate type
provider_sort_cols = [
    "provider_id",
    "payer_id",
    "plan_name",
    "payer_product_network",
    "billing_code_type",
    "billing_code",
    "revenue_code",
    "final_rate_type",
]
provider_rate_types = pl.Enum(
    [
        "case rate",
        "percent of total billed charges",
        "per diem",
        "estimated allowed amount",
        "fee schedule",
        "other",
    ]
)
providers_clean = (
    providers_clean.with_columns(
        pl.col("final_rate_type").cast(provider_rate_types)
    )
    .sort(provider_sort_cols, nulls_last=True)
    .unique(provider_sort_cols, keep="first")
)

# Keep only common revenue codes related to inpatient stays
providers_clean = providers_clean.filter(
    (pl.col("revenue_code").str.contains("^[1-2][0-9]{2}$"))
    & (pl.col("revenue_code").count().over("revenue_code") > 10)
    | (pl.col("revenue_code").is_null())
)

# Collapse the negotiated rates to the mean across all revenue codes
# by provider-payer-plan-code, prioritizing the mean of only NULL revenue
# code rates (if there are any)
providers_clean = (
    providers_clean.group_by(provider_sort_cols[:-2], maintain_order=True)
    .agg(
        pl.all()
        .exclude(provider_sort_cols[:-2] + ["final_rate_amount"])
        .first(),
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

# TODO: Check against KFF cleanup report
