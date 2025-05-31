# type: ignore
# %% Import Python libraries and set up Trino
import logging

import polars as pl
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# %% Grab hospital delivery rate data from Turquoise Health
logger.info("Fetching raw data from Trino")

with open("queries/rates.sql", "r") as query:
    rates_df = pl.read_database(query.read(), trino_conn)
    rates_df.write_parquet("data/rates.parquet")
