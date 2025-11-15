import json
from pathlib import Path
from urllib.parse import urlencode

import polars as pl
import requests

# Available via devtools in any browser session :P
APP_ID = "2R6TWFHRPG"
API_KEY = "b9d2530b21f9d9fbbc910aa2f21a79da"
INDEX_NAME = "providers"

SEARCH_URL = f"https://{APP_ID}-dsn.algolia.net/1/indexes/{INDEX_NAME}/query"

HEADERS = {
    "Content-Type": "application/json",
    "x-algolia-application-id": APP_ID,
    "x-algolia-api-key": API_KEY,
    "Referer": "https://mishe.co/",
}

INTERMEDIATE_DIR = Path("data/intermediate")
FINAL_DIR = Path("data/input")
INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)


def normalize_hit(hit: dict) -> dict:
    """
    Normalize a single Algolia hit so Polars can infer a clean schema.

    Strategy: make EVERY non-null value a string.
    - dict / list  -> JSON string
    - scalar       -> str(...)
    - None stays None
    """
    out = {}
    for k, v in hit.items():
        if v is None:
            out[k] = None
        elif isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False)
        else:
            out[k] = str(v)
    return out


def search_letter_page(
    letter: str, page: int, hits_per_page: int = 1000
) -> dict:
    """
    Call Algolia search endpoint for a single page of results.
    """

    params_dict = {
        "query": letter,
        "hitsPerPage": hits_per_page,
        "page": page,
    }
    params_str = urlencode(params_dict)

    body = {"params": params_str}

    resp = requests.post(SEARCH_URL, headers=HEADERS, data=json.dumps(body))
    resp.raise_for_status()
    return resp.json()


def fetch_specialty_to_parquet(
    specialty: str,
    hits_per_page: int = 1000,
    max_pages_per_specialty: int | None = None,
) -> Path | None:
    """
    Fetch all providers for a given specialty,
    save them to a Parquet file under data/intermediate, and return the path.
    If there are no hits, returns None.
    """
    # Convert specialty to snake_case for filename
    specialty_snake = (
        specialty.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace("&", "and")
        .replace(",", "")
        .replace("(", "")
        .replace(")", "")
        .replace("'", "")
        .replace('"', "")
    )
    out_path = INTERMEDIATE_DIR / f"cpw_provider_{specialty_snake}.parquet"

    if out_path.exists():
        print(
            f"[{specialty}] Parquet already exists at {out_path}, skipping query."
        )
        return out_path

    print(f"[{specialty}] Querying Algolia...")

    all_hits = []

    first_page = search_letter_page(
        specialty, page=0, hits_per_page=hits_per_page
    )
    nb_pages = first_page.get("nbPages", 1)

    pages_for_specialty = nb_pages
    if max_pages_per_specialty is not None:
        pages_for_specialty = min(nb_pages, max_pages_per_specialty)

    for page in range(pages_for_specialty):
        if page > 0:
            page_json = search_letter_page(
                specialty, page=page, hits_per_page=hits_per_page
            )
        else:
            page_json = first_page

        hits = page_json.get("hits", [])
        if not hits:
            break

        for h in hits:
            all_hits.append(normalize_hit(h))

    if not all_hits:
        print(f"[{specialty}] No hits for this specialty, nothing to save.")
        return None

    df = pl.DataFrame(all_hits, infer_schema_length=1000)
    df.write_parquet(out_path)
    print(f"[{specialty}] Wrote {df.height} rows to {out_path}")
    return out_path


def main():
    specialties_df = pl.read_csv("data/input/cpw_specialties.csv")
    specialties = specialties_df["specialty"].drop_nulls().to_list()

    specialties = [s.strip() for s in specialties if s.strip()]

    print(f"Found {len(specialties)} specialties to query")

    for specialty in specialties:
        fetch_specialty_to_parquet(specialty, hits_per_page=1000)

    parquet_paths = sorted(INTERMEDIATE_DIR.glob("cpw_provider_*.parquet"))
    if not parquet_paths:
        print("No intermediate parquet files found; nothing to concatenate.")
        return

    dfs = []
    for path in parquet_paths:
        print(f"Loading {path}")
        dfs.append(pl.read_parquet(path))

    combined = pl.concat(dfs, how="diagonal_relaxed")
    final_path = FINAL_DIR / "cpw_providers.parquet"
    combined.write_parquet(final_path)
    print(f"Wrote combined {combined.height} rows to {final_path}")

    combined.select(pl.col("npi")).unique("npi").write_csv(
        "data/output/cpw_provider_npis.csv"
    )


if __name__ == "__main__":
    main()
