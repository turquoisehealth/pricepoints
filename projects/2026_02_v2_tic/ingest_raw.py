import gzip
import io
import logging
import os
import re
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import ijson
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
PREFIXES = ["2026-02/"]
MAX_WORKERS = 10

HIVE_PATTERN = re.compile(
    r"payer_id=(?P<payer_id>[^/]+)/data_source_name=(?P<data_source_name>[^/]+)/"
)


def get_s3_client():
    session = boto3.Session()
    return session.client(
        "s3",
        config=Config(
            max_pool_connections=MAX_WORKERS,
            retries={"max_attempts": 5, "mode": "adaptive"},
        ),
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
            files.append(
                {
                    "key": key,
                    "size": obj["Size"],
                    **parsed,
                }
            )
    return files


def extract_network_names(client, bucket: str, key: str) -> set[str]:
    resp = client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]

    if key.endswith(".gz"):
        stream = gzip.GzipFile(fileobj=body)
    elif key.endswith(".zip"):
        zip_bytes = io.BytesIO(body.read())
        zf = zipfile.ZipFile(zip_bytes)
        first_name = zf.namelist()[0]
        stream = zf.open(first_name)
    else:
        stream = body

    names = set()
    in_provider_refs = False

    for prefix, event, value in ijson.parse(stream):
        if prefix == "provider_references" and event == "start_array":
            in_provider_refs = True
        elif prefix == "provider_references" and event == "end_array":
            break
        elif (
            in_provider_refs
            and prefix == "provider_references.item.network_name.item"
            and event == "string"
        ):
            names.add(value)

    return names


def process_file(client, info: dict) -> dict:
    key = info["key"]
    prefix = key.split("/", 1)[0]

    try:
        names = extract_network_names(client, BUCKET, key)
    except Exception:
        logger.exception("Failed to process %s", key)
        return {
            "prefix": prefix,
            "payer_id": info["payer_id"],
            "data_source_name": info["data_source_name"],
            "network_names": None,
            "network_name_count": None,
            "file_size": info["size"],
        }

    return {
        "prefix": prefix,
        "payer_id": info["payer_id"],
        "data_source_name": info["data_source_name"],
        "network_names": sorted(names),
        "network_name_count": len(names),
        "file_size": info["size"],
    }


def main():
    client = get_s3_client()

    all_files = []
    for prefix in PREFIXES:
        logger.info("Listing files under %s", prefix)
        files = list_s3_files(client, prefix)
        logger.info("Found %d files under %s", len(files), prefix)
        all_files.extend(files)

    if not all_files:
        logger.warning("No files found. Exiting.")
        return

    results = []
    success = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_file, client, info): info
            for info in all_files
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if result["network_names"] is not None:
                success += 1

    logger.info("Processed %d/%d files successfully", success, len(all_files))

    df = pl.DataFrame(
        results,
        schema={
            "prefix": pl.String,
            "payer_id": pl.String,
            "data_source_name": pl.String,
            "network_names": pl.List(pl.String),
            "network_name_count": pl.Int64,
            "file_size": pl.Int64,
        },
    )

    df.write_parquet("data/output/network_names.parquet")
    logger.info("Wrote %d rows to data/output/network_names.parquet", len(df))


if __name__ == "__main__":
    main()
