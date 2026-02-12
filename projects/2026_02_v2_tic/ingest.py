import polars as pl
import tq

trino_conn = tq.get_trino_connection()


###### Data loading ############################################################

# Grab a proxy for number of facilities for each payer/plan in the DFW area
with open("queries/dfw_provider_counts.sql", "r") as query:
    dfw_provider_counts_df = pl.read_database(
        query.read(),
        trino_conn,
    )
