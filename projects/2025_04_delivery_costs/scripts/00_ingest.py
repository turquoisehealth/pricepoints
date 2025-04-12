import polars as pl
from tq.connectors import get_trino_connection

trino_conn = get_trino_connection()

# Grab all delivery prices from providers (i.e. hospitals)
with open("queries/providers.sql", "r") as query:
    providers_df = pl.read_database(query.read(), trino_conn)

providers_df.write_parquet("data/providers.parquet")
