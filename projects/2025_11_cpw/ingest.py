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

# Count taken from CPW website here: https://www.costpluswellness.com/directory
cpw_provider_df = pl.DataFrame(
    {
        "payer_id": "0",
        "payer_name": "CostPlus",
        "payer_network_name": "CostPlus Wellness",
        "provider_count": 193,
    }
)

pl.concat([dfw_provider_counts_df, cpw_provider_df]).write_parquet(
    "data/output/dfw_provider_counts.parquet"
)
