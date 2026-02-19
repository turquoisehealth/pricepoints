import logging
import os
import re
from datetime import date
from pathlib import PurePosixPath

import boto3
import polars as pl
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("TQ_RAW_PAYER_BUCKET", "")
PREFIXES = {
    "file_sizes_dec": "2025-12/",
    "file_sizes_feb": "2026-02/",
}

HIVE_PATTERN = re.compile(
    r"payer_id=(?P<payer_id>[^/]+)/data_source_name=(?P<data_source_name>[^/]+)/"
)


def get_s3_client():
    session = boto3.Session()
    return session.client(
        "s3",
        config=Config(retries={"max_attempts": 5, "mode": "adaptive"}),
    )


def parse_hive_path(key: str) -> dict | None:
    match = HIVE_PATTERN.search(key)
    if not match:
        return None
    return match.groupdict()


def list_s3_files(client, prefix: str) -> list[dict]:
    files = []
    full_prefix = f"{prefix}type=in-network-rates/"
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=full_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parsed = parse_hive_path(key)
            if parsed is None:
                continue
            extension = "".join(PurePosixPath(key).suffixes)
            files.append(
                {
                    "payer_id": parsed["payer_id"],
                    "data_source_name": parsed["data_source_name"],
                    "extension": extension,
                    "file_size": obj["Size"],
                }
            )
    return files


def main():
    date_stamp = date.today().strftime("%Y_%m_%d")
    client = get_s3_client()
    os.makedirs("data/output", exist_ok=True)

    for name, prefix in PREFIXES.items():
        logger.info("Listing files under %s", prefix)
        files = list_s3_files(client, prefix)
        logger.info("Found %d files under %s", len(files), prefix)

        if not files:
            df = pl.DataFrame(
                schema={
                    "payer_id": pl.String,
                    "data_source_name": pl.String,
                    "extension": pl.String,
                    "n_files": pl.UInt32,
                    "total_size": pl.Int64,
                }
            )
        else:
            df = (
                pl.DataFrame(
                    files,
                    schema={
                        "payer_id": pl.String,
                        "data_source_name": pl.String,
                        "extension": pl.String,
                        "file_size": pl.Int64,
                    },
                )
                .group_by("payer_id", "data_source_name")
                .agg(
                    pl.col("extension").first(),
                    pl.len().cast(pl.UInt32).alias("n_files"),
                    pl.col("file_size").sum().alias("total_size"),
                )
            )

        out_path = f"data/output/{date_stamp}_{name}.parquet"
        df.write_parquet(out_path)
        logger.info("Wrote %d rows to %s", len(df), out_path)


if __name__ == "__main__":
    main()
