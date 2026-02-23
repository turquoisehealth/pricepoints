import logging
from datetime import date
from pathlib import Path

import polars as pl
import tq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

trino_conn = tq.get_trino_connection()

DATE_STAMP = date.today().strftime("%Y_%m_%d")
QUERY_DIR = Path("queries")
OUTPUT_DIR = Path("data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUERIES = [
    "ein_fill_rate_feb",
    "ein_names_sample_feb",
    "network_names_feb",
    "parsing_status_dec",
    "parsing_status_feb",
    "payer_files_dec",
    "payer_files_feb",
    "payer_lives",
    "provider_mode_dec",
    "provider_mode_feb",
]


###### Data loading ############################################################

for name in QUERIES:
    sql_path = QUERY_DIR / f"{name}.sql"
    out_path = OUTPUT_DIR / f"{DATE_STAMP}_{name}.parquet"

    logger.info("Running %s...", name)
    query = sql_path.read_text()

    # Strip trailing semicolons since pl.read_database doesn't want them
    query = query.rstrip().rstrip(";")

    df = pl.read_database(query, trino_conn)
    df.write_parquet(out_path)
    logger.info(
        "  -> %s (%s rows, %d cols)", out_path, f"{df.shape[0]:,}", df.shape[1]
    )

logger.info("Done.")
