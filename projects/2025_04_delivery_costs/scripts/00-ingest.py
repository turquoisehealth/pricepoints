import pandas as pd
import tq

trino_conn = tq.get_trino_connection()

# Grab all delivery prices from provider data (i.e. from hospitals)
with open("queries/providers.sql", "r") as query:
    providers_df = pd.read_sql_query(query.read(), trino_conn)

providers_df.to_parquet("data/providers.parquet", index=False)
