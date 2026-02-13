from pathlib import Path

import polars as pl
import tq

trino_conn = tq.get_trino_connection()

QUERY_DIR = Path("queries")
OUTPUT_DIR = Path("data/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUERIES = [
    "payer_files_feb",
    "payer_files_dec",
    "network_names_feb",
    "network_names_sample",
    "provider_mode_feb",
    "provider_mode_dec",
    "file_sizes_feb",
    "ein_names_feb",
    "ein_names_agg_feb",
]


###### Data loading ############################################################

for name in QUERIES:
    sql_path = QUERY_DIR / f"{name}.sql"
    out_path = OUTPUT_DIR / f"{name}.parquet"

    print(f"Running {name}...")
    query = sql_path.read_text()

    # Strip trailing semicolons since pl.read_database doesn't want them
    query = query.rstrip().rstrip(";")

    df = pl.read_database(query, trino_conn)
    df.write_parquet(out_path)
    print(f"  -> {out_path} ({df.shape[0]:,} rows, {df.shape[1]} cols)")

print("\nDone.")
