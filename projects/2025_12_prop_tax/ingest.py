import os

import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()
sc_headers = {"X-App-Token": os.getenv("SOCRATA_APP_TOKEN")}

# Ingest a list of possible MacNeal hospital pins pulled
# from the Cook County web map
macneal_pins_df_possible = pl.read_csv(
    "data/input/macneal_pins_possible.csv", schema_overrides={"pin": pl.String}
)
macneal_pins_str_possible = "', '".join(
    macneal_pins_df_possible["pin"].to_list()
)

# Use the Cook County mailing address data to grab the mailing address for each
# of the possible PINs
cook_address_endpoint = (
    "https://datacatalog.cookcountyil.gov/api/v3/views/3723-97qp/query.json"
)
cook_address_query = {
    "query": "SELECT DISTINCT pin, mail_address_name"
    + f" WHERE pin IN ('{macneal_pins_str_possible}')"
}
cook_address_response = requests.post(
    cook_address_endpoint, headers=sc_headers, json=cook_address_query
)

# Keep only PINs with a mailing address related to Loyola or MacNeal hospital
macneal_pins_df_confirmed = (
    pl.read_json(cook_address_response.content)
    .filter(
        pl.col("mail_address_name").str.contains_any(
            ["MAC", "LOYOLA", "EXEMPT"]
        )
    )
    .unique("pin")
)
macneal_pins_df_confirmed.write_csv(
    "data/intermediate/macneal_pins_confirmed.csv"
)
