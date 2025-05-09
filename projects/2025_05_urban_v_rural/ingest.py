# type: ignore
# %% Import Python libraries and set up Trino
import duckdb
import polars as pl
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())

# Get OpenTimes DuckDB database
duckdb_conn = duckdb.connect(database=":memory:")
duckdb_conn.execute("""
    INSTALL spatial;
    LOAD spatial;
    INSTALL httpfs;
    LOAD httpfs;
    ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes;
""")

# %% Load provider locations from Trino into local DuckDB
providers_df = pl.read_database(
    """
    SELECT
        id,
        hq_longitude AS lon,
        hq_latitude AS lat
    FROM glue.hospital_data.hospital_provider
    WHERE hospital_type IN (
        'Short Term Acute Care Hospital',
        'Critical Access Hospital'
    )
""",
    trino_conn,
)
duckdb_conn.execute("""
    CREATE OR REPLACE TABLE providers AS
    SELECT *, ST_Point(lon, lat) AS geometry
    FROM providers_df
""")

# %% Load tract geometries saved from TIGER/Line shapefiles into DuckDB
tracts_df = pl.read_csv("tracts.csv", schema_overrides={"GEOID": pl.String})
duckdb_conn.execute("""
    CREATE OR REPLACE TABLE tracts AS
    SELECT GEOID AS geoid, ST_GeomFromText(geometry) AS geometry
    FROM tracts_df
""")

# %% Add the current Census tract of each provider
duckdb_conn.execute("""
    CREATE OR REPLACE TABLE provider_tracts AS
    SELECT
        p.id AS provider_id,
        t.geoid
    FROM providers AS p
    INNER JOIN tracts AS t ON ST_Within(p.geometry, t.geometry)
""")

# %% Grab all U.S. Census tracts from OpenTimes
duckdb_conn.execute("""
    CREATE OR REPLACE TABLE times AS
    SELECT origin_id, destination_id, duration_sec
    FROM opentimes.public.times
    WHERE version = '0.0.1'
        AND mode = 'car'
        AND year = '2024'
        AND geography = 'tract'
        AND duration_sec <= 3600
""")

# %% Get all Census tracts with no provider within 60 minutes, then return
# a Polars dataframe and save to CSV
final_df = duckdb_conn.execute("""
    SELECT
        t.geoid,
        ST_AsText(t.geometry) AS geometry,
        np.origin_id IS NULL AS no_provider
    FROM tracts AS t
    LEFT JOIN (
        SELECT ti.origin_id
        FROM times AS ti
        LEFT JOIN provider_tracts AS pt
            ON ti.destination_id = pt.geoid
        GROUP BY ti.origin_id HAVING COUNT(DISTINCT pt.provider_id) = 0
    ) AS np
        ON t.geoid = np.origin_id
""").pl()
final_df.write_csv("final.csv", has_header=True)
