import re

import polars as pl
import polars.selectors as cs
import tq

trino_conn = tq.get_trino_connection()


# Mini-classes for attaching utility methods to Polars DataFrames, mostly for
# cleaning up OPAIS data
@pl.api.register_dataframe_namespace("util")
class UtilDataFrame:
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def to_snake_case(self) -> pl.DataFrame:
        """Convert all column names to snake_case."""
        return self._df.rename(
            {
                col: re.sub(r"[^a-zA-Z0-9]", "_", col.lower())
                for col in self._df.columns
            }
        )

    def empty_strings_to_null(self) -> pl.DataFrame:
        """Convert all empty string columns to null."""
        return self._df.with_columns(
            pl.when(pl.col(pl.String).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.String))
            .name.keep()
        )


###### Data loading ############################################################

# Grab professional/PG rates and NPIs from the files here:
# https://www.costpluswellness.com/contracts/baylor-scott-and-white-professional
bswh_pro_df_dict = pl.read_excel(
    "data/input/CostPlusWellness.com_C010_RateSheet_BSWH_Professional.xlsx",
    sheet_id=0,
)
# Drop the free-standing imaging rates since they have a different file schema
bswh_pro_fsi_df = bswh_pro_df_dict.pop("FreeStandingImaging")

bswh_pro_df = (
    pl.concat(
        [
            df.util.to_snake_case()
            .rename({"nonfacilty_amount": "nonfacility_amount"}, strict=False)
            .with_columns(type=pl.lit(k))
            for k, df in bswh_pro_df_dict.items()
        ]
    )
    .with_columns(
        billing_code_type=pl.lit("HCPCS"),
        payer_id=pl.lit("0"),
        payer_name=pl.lit("CPW"),
    )
    .select(pl.exclude("modifier"))
    .rename(
        {
            # Map column names to TQ equivalents
            "cpt_hcpcs": "billing_code",
            "cpt_hcpcs_description": "service_description",
        }
    )
    # Unpivot facility/nonfacility amount to match TQ structure (i.e. long
    # instead of wide)
    .unpivot(
        on=["facility_amount", "nonfacility_amount"],
        index=cs.exclude(["facility_amount", "nonfacility_amount"]),
        variable_name="facility",
        value_name="canonical_rate",
    )
    .with_columns(
        facility=pl.col("facility")
        .replace(["facility_amount", "nonfacility_amount"], [1, 0])
        .cast(pl.Int8)
        .cast(pl.Boolean)
    )
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


###### Data aggregation ########################################################

# Attach third-party data to the CPW rates, taking the median across all
# providers. Also drop any CPW rates which are NULL or have no TQ-equivalent and
# therefore cannot be compared (most of these are obscure level 2 HCPCS)
bswh_pro_sub_df = (
    bswh_pro_df.join(
        bswh_pro_tq_rates_df.group_by(
            ["billing_code", "billing_code_type", "facility"]
        )
        .agg(
            pl.col("medicare_rate").median(),
            pl.col("service_line").mode(),
            pl.col("state_claims_percentile_sep").median(),
            pl.col("state_claims_percentile_all").median(),
        )
        .explode("service_line"),
        on=["billing_code", "billing_code_type", "facility"],
        how="inner",
    )
    .filter(pl.col("canonical_rate").is_not_null())
    .with_columns(
        pct_of_medicare=pl.col("canonical_rate") / pl.col("medicare_rate"),
    )
    .unique(["billing_code", "billing_code_type", "facility"])
)

bswh_pro_sub_df.write_parquet("data/output/bswh_pro_sub.parquet")
bswh_pro_tq_rates_df.write_parquet("data/output/bswh_pro_tq_rates.parquet")
