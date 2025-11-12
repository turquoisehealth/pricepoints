import polars as pl
import tq

trino_conn = tq.get_trino_connection()


###### Data loading ############################################################

## Grab BSW institutional rates based on the files here:
# https://www.costpluswellness.com/contracts/baylor-scott-and-white-facilities

# I manually extracted the CSV version of this PDF using Tabula + some light
# hand editing (e.g. to delete duplicate header rows)
bswh_inst_df = (
    (
        pl.read_csv(
            "data/input/CostPlusWellness.com_C010_RateSheet_BSWH_Institutional.csv",
            schema_overrides={"billing_code": pl.String},
        )
        .with_columns(rate=pl.col("rate").str.replace_all(",", ""))
        .with_columns(rate=pl.col("rate").cast(pl.Float64))
        .with_columns(
            billing_code_type=pl.col("billing_code_type").replace(
                ["CPT/HCP MCC", "CPT"], "HCPCS"
            ),
            setting=pl.col("setting").str.to_titlecase(),
            payer_id=pl.lit("0"),
            payer_name=pl.lit("CPW"),
        )
    )
    .select(pl.exclude("category"))
    .rename(
        {
            # Map column names to TQ equivalents
            "setting": "bill_type",
            "description": "service_description",
            "charge_type": "canonical_rate_contract_methodology",
            "rate": "canonical_rate",
        }
    )
)

# Grab the list of BSWH EIN's in CPW's contract, again parsed using Tabula
bswh_inst_providers_df = pl.read_csv(
    "data/input/CostPlusWellness.com_C010_20251104_BaylorScottWhiteHealth_Facility.csv"
).with_columns(ein=pl.col("ein").str.replace_all("-", ""))

bswh_inst_providers_eins = ",".join(
    f"'{val}'" for val in bswh_inst_providers_df["ein"].unique().to_list()
)

# Fetch all rates for the CPW EINs. See query for filter conditions
with open("queries/bswh_inst_rates.sql", "r") as query:
    bswh_inst_tq_rates_df = pl.read_database(
        query.read().replace("{{ bswh_eins }}", bswh_inst_providers_eins),
        trino_conn,
    )


###### Data aggregation ########################################################

# Attach third-party data to the CPW rates, taking the median across all
# providers. Also drop any CPW rates which are NULL or have no TQ-equivalent and
# therefore cannot be compared (most of these are obscure level 2 HCPCS)
bswh_inst_sub_df = (
    bswh_inst_df.join(
        bswh_inst_tq_rates_df.group_by(["billing_code", "billing_code_type"])
        .agg(
            pl.col("medicare_rate").median(),
            pl.col("service_line").mode(),
            pl.col("state_claims_percentile_sep").median(),
            pl.col("state_claims_percentile_all").median(),
        )
        .explode("service_line"),
        on=["billing_code", "billing_code_type"],
        how="inner",
    )
    .filter(pl.col("canonical_rate").is_not_null())
    # There are some rates with multiple settings in the CPW rate sheet (i.e.
    # OP + Lab, OP + ER). To simplify, keep only IP and OP rates
    .filter(pl.col("bill_type").is_in(["Outpatient", "Inpatient"]))
    .with_columns(
        pct_of_medicare=pl.col("canonical_rate") / pl.col("medicare_rate"),
    )
    .unique(["billing_code", "billing_code_type"])
)

# Keeping only the billing codes at the intersection of CPW x TQ rates,
# collapse TQ rates from BSWH providers to the payer-code level
bswh_inst_tq_payer_code_agg_df = (
    bswh_inst_tq_rates_df.filter(
        pl.col("billing_code").is_in(
            bswh_inst_sub_df["billing_code"].unique().to_list()
        )
    )
    .group_by(
        [
            "payer_id",
            "payer_name",
            "billing_code_type",
            "billing_code",
            "bill_type",
            "service_description",
            "service_line",
        ]
    )
    .agg(
        pl.len().alias("num_rates"),
        pl.n_unique("provider_id").alias("num_providers"),
        pl.col("canonical_rate").mean().alias("avg_canonical_rate"),
        pl.col("medicare_rate").mean().alias("avg_medicare_rate"),
        *[
            pl.col("canonical_rate")
            .quantile(q / 100)
            .alias(f"p{q}_canonical_rate")
            for q in [10, 25, 50, 75, 90]
        ],
        *[
            pl.col("medicare_rate")
            .quantile(q / 100)
            .alias(f"p{q}_medicare_rate")
            for q in [10, 25, 50, 75, 90]
        ],
        *[
            (pl.col("canonical_rate") / pl.col("medicare_rate"))
            .quantile(q / 100)
            .alias(f"p{q}_pct_of_medicare")
            for q in [10, 25, 50, 75, 90]
        ],
    )
)

# Same as above, but this time collapse all codes to the payer level
bswh_inst_tq_payer_agg_df = (
    bswh_inst_tq_rates_df.filter(
        pl.col("billing_code").is_in(
            bswh_inst_sub_df["billing_code"].unique().to_list()
        )
    )
    .group_by(["payer_name", "payer_id"])
    .agg(
        pl.len().alias("num_rates"),
        pl.n_unique("provider_id").alias("num_providers"),
        *[
            (pl.col("canonical_rate") / pl.col("medicare_rate"))
            .quantile(q / 100)
            .alias(f"p{q}_pct_of_medicare")
            for q in [10, 25, 50, 75, 90]
        ],
    )
)

bswh_inst_sub_df.write_parquet("data/output/bswh_inst_sub.parquet")
bswh_inst_tq_rates_df.write_parquet("data/output/bswh_inst_tq_rates.parquet")
bswh_inst_tq_payer_code_agg_df.write_parquet(
    "data/output/bswh_inst_tq_payer_code_agg.parquet"
)
bswh_inst_tq_payer_agg_df.write_parquet(
    "data/output/bswh_inst_tq_payer_agg.parquet"
)
