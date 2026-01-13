import os

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()
sc_headers = {"X-App-Token": os.getenv("SOCRATA_APP_TOKEN")}

# Ingest a list of possible MacNeal hospital pins pulled
# from the Cook County web map
possible_macneal_pins_df = pl.read_csv(
    "data/input/possible_macneal_pins.csv", schema_overrides={"pin": pl.String}
)
possible_macneal_pins_str = "', '".join(
    possible_macneal_pins_df["pin"].to_list()
)

# Use the Cook County mailing address data to grab the mailing address for each
# of the possible PINs
cook_address_endpoint = (
    "https://datacatalog.cookcountyil.gov/api/v3/views/3723-97qp/query.json"
)
cook_address_query = {
    "query": "SELECT DISTINCT pin, mail_address_name"
    + f" WHERE pin IN ('{possible_macneal_pins_str}')"
}
cook_address_response = requests.post(
    cook_address_endpoint, headers=sc_headers, json=cook_address_query
)

# Keep only PINs that are related to Loyola or MacNeal hospital
cook_pins_df = (
    pl.read_json(cook_address_response.content)
    .filter(
        pl.col("mail_address_name").str.contains_any(
            ["MAC", "LOYOLA", "EXEMPT"]
        )
    )
    .unique("pin")
)
cook_pins_df.write_csv("data/intermediate/cook_pins.csv")
