# type: ignore
import re

import polars as pl
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()


# Mini-classes for attaching utility methods to Polars DataFrames, mostly for
# cleaning up OPAIS data
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


###### Data loading ############################################################

# Load the aggregated rates data via big ol' SQL query
with open("queries/rates.sql", "r") as query:
    rates_df = pl.read_database(query.read(), trino_conn)

# Load and cleanup the HRSA OPAIS data to get CAH status for each hospital
opais_ce_df = (
    pl.read_excel(
        source="data/input/340b_opais.xlsx",
        sheet_name="Covered Entities",
        read_options={"header_row": 3},
    )
    .util.to_snake_case()
    .util.empty_strings_to_null()
    .with_columns((pl.col("participating") == "TRUE").alias("participating"))
    .filter(pl.col("participating"))
    .select(["medicare_provider_number", "entity_type"])
    .rename({"entity_type": "opais_340b_entity_type"})
    .unique()
)

# Read county-level crosswalk for translating TQ state-county names to GEOIDs
# Created this manually with Claude + some hand matching
county_xwalk_df = pl.read_csv(
    source="data/input/county_crosswalk.csv",
    schema_overrides={
        "state": pl.String,
        "county": pl.String,
        "census_county_fips": pl.String,
    },
)

# Read NCHS urban-rural classification codes
# https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html
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
# https://www.ers.usda.gov/data-products/rural-urban-continuum-codes
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


# Join county codes, rural-urban classifications, and CAH status, then save
# to disk
(
    rates_df.join(
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
    .join(
        opais_ce_df,
        left_on="medicare_provider_id",
        right_on="medicare_provider_number",
        how="left",
    )
).write_parquet("data/output/rates_clean.parquet")
