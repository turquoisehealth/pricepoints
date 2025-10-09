import re

import polars as pl
from census import Census
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
cen = Census(config.get("CENSUS_API_KEY"), year=2023)


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

# Manual county matching per provider when the TQ county name doesn't match a
# current Census county. Most of these are in Connecticut since they
# (very annoyingly) remapped all their counties in 2022
missing_county_lookup_df = pl.read_csv(
    source="data/input/missing_county_lookup.csv",
    schema_overrides={
        "provider_id": pl.String,
        "missing_census_county_fips": pl.String,
    },
)

# Read NCHS urban-rural classification codes
# https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html
nchs_df = (
    pl.read_csv(
        source="data/input/NCHSurb-rural-codes.csv",
        encoding="utf8-lossy",
        columns=["STFIPS", "CTYFIPS", "CODE2023"],
        schema_overrides={"STFIPS": pl.String, "CTYFIPS": pl.String},
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
    # The 2023 can be null because each row represents a county, but some
    # counties existed in the past but no longer do in 2023
    .filter(pl.col("nchs_code").is_not_null())
    .with_columns(
        (
            pl.when(pl.col("nchs_code") >= 5)
            .then(pl.lit("rural"))
            .when(pl.col("nchs_code") < 5)
            .then(pl.lit("urban"))
            .otherwise(None)
        ).alias("nchs_class")
    )
)

# Read USDA rural-urban continuum codes
# https://www.ers.usda.gov/data-products/rural-urban-continuum-codes
rucc_df = (
    (
        pl.read_csv(
            source="data/input/Ruralurbancontinuumcodes2023.csv",
            encoding="utf8-lossy",
            schema_overrides={"FIPS": pl.String},
        )
    )
    .filter(pl.col("Attribute") == "RUCC_2023")
    .rename({"Value": "rucc_code", "FIPS": "geoid"})
    .select(["geoid", "rucc_code"])
    .with_columns(pl.col("geoid").str.pad_start(5, "0"))
    .with_columns(pl.col("rucc_code").cast(pl.Int64))
    .with_columns(
        (
            pl.when(pl.col("rucc_code") >= 4)
            .then(pl.lit("rural"))
            .when(pl.col("rucc_code") < 4)
            .then(pl.lit("urban"))
            .otherwise(None)
        ).alias("rucc_class")
    )
)

# Read UDSA urban-influence codes
# https://www.ers.usda.gov/data-products/urban-influence-codes
uic_df = (
    pl.read_csv(
        source="data/input/Urbaninfluencecodes2024.csv",
        encoding="utf8-lossy",
        schema_overrides={"FIPS-UIC": pl.String},
    )
    .filter(pl.col("Attribute") == "UIC_2024")
    .rename({"Value": "uic_code", "FIPS-UIC": "geoid"})
    .select(["geoid", "uic_code"])
    .with_columns(pl.col("geoid").str.pad_start(5, "0"))
    .with_columns(pl.col("uic_code").cast(pl.Int64))
    .with_columns(
        (
            pl.when(pl.col("uic_code") >= 3)
            .then(pl.lit("rural"))
            .when(pl.col("uic_code") < 3)
            .then(pl.lit("urban"))
            .otherwise(None)
        ).alias("uic_class")
    )
)

# Read USDA rural-urban commuting area codes by ZIP
ruca_df = (
    pl.read_csv(
        source="data/input/RUCA-codes-2020-zipcode.csv",
        encoding="utf8-lossy",
        schema_overrides={"ZIPCode": pl.String, "PrimaryRUCA": pl.String},
    )
    .rename({"ZIPCode": "zip_code", "PrimaryRUCA": "ruca_code"})
    .select(["zip_code", "ruca_code"])
    .with_columns(pl.col("zip_code").str.pad_start(5, "0"))
    .with_columns(pl.col("ruca_code").cast(pl.Int64))
    .with_columns(
        (
            pl.when(pl.col("ruca_code") >= 4)
            .then(pl.lit("rural"))
            .when(pl.col("ruca_code") < 4)
            .then(pl.lit("urban"))
            .otherwise(None)
        ).alias("ruca_class")
    )
)

# Grab Census population for each county. 2022 year since that's what Turquoise
# county IDs are based on
cen_vars = {"B01001_001E": "total_pop", "B19013_001E": "median_hh_income"}
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


###### Data joining ############################################################

# Join county codes, rural-urban classifications, and CAH status, then save
# to disk
rates_df_clean = (
    rates_df.join(
        county_xwalk_df,
        on=["state", "county"],
        how="left",
    )
    .join(missing_county_lookup_df, on="provider_id", how="left")
    .with_columns(
        pl.when(pl.col("census_county_fips").is_null())
        .then(pl.col("missing_census_county_fips"))
        .otherwise(pl.col("census_county_fips"))
        .alias("census_county_fips")
    )
    .join(ruca_df, on="zip_code", how="left")
    .join(
        nchs_df,
        left_on="census_county_fips",
        right_on="geoid",
        how="left",
    )
    .join(
        rucc_df,
        left_on="census_county_fips",
        right_on="geoid",
        how="left",
    )
    .join(
        uic_df,
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
    .join(
        cen_df_county,
        left_on="census_county_fips",
        right_on="geoid",
        how="left",
    )
)
rates_df_clean.write_parquet("data/output/rates_clean.parquet")
