import polars as pl
import tq

trino_conn = tq.get_trino_connection()

with open("queries/self_v_fully_funded.sql", "r") as query:
    employers_df = pl.read_database(query.read(), trino_conn)

print(employers_df)


# Cigna tq_network_id
24593  # Gartner
33082  # Hilton
