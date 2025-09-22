import random
import string

import polars as pl
from dotenv import dotenv_values
from tq.connectors import get_trino_connection
from tq.utils import get_env_file_path

trino_conn = get_trino_connection()
config = dotenv_values(get_env_file_path())
s3_base_uri = config.get("S3_BASE_URI", "")


def load_table_with_ctas(
    sql_file: str, s3_location: str | None, table_name: str
) -> pl.DataFrame:
    """Load table to Polars by first saving a SQL query to S3 as Parquet"""
    if not s3_location or s3_location == "":
        raise ValueError("s3_location must not be empty")

    s3_full_path = f"s3://{s3_location}/{table_name}"
    with open(sql_file, "r") as f:
        sql = (
            f.read()
            .replace("{{ table_name }}", table_name)
            .replace("{{ s3_location }}", s3_full_path)
        )

    with trino_conn.cursor() as cur:
        cur.execute(sql)

    return pl.read_parquet(s3_full_path + "/*")


all_rates_df = load_table_with_ctas(
    sql_file="queries/all_rates.sql",
    s3_location=s3_base_uri,
    table_name="dsnow_tmp_cld_rates_subset"
    + "_"
    + "".join(random.choices(string.ascii_lowercase + string.digits, k=5)),
)
