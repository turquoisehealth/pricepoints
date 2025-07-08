# type: ignore
# %% Import Python libraries and set up geocoder
import polars as pl
from dotenv import dotenv_values
from geopy.geocoders import GoogleV3
from tq.utils import get_env_file_path

# Setup geocoding classes. Nominatim only allows 1 request per second
config = dotenv_values(get_env_file_path())
geocoder = GoogleV3(api_key=config.get("GOOGLE_API_KEY"))

# %% Grab the OPAIS detailed entity data for geocoding
opais_ce_child_df = pl.read_parquet("data/intermediate/opais_ce_child.parquet")
opais_cp_fil_df = pl.read_parquet(
    "data/intermediate/opais_contract_pharmacies.parquet"
)


##### Geocode CE sites #########################################################

# %% Geocode the covered entity child sites
opais_ce_unique_addresses = set(
    opais_ce_child_df["street_address_full"].to_list()
)
opais_ce_lon = []
opais_ce_lat = []

for address in opais_ce_unique_addresses:
    location = geocoder.geocode(address, exactly_one=True, timeout=5)
    if location:
        opais_ce_lon.append(location.longitude)
        opais_ce_lat.append(location.latitude)
    else:
        opais_ce_lon.append(None)
        opais_ce_lat.append(None)

# %% Construct a dataframe of the geocoding results and attach it
# back to the covered entity child dataframe
opais_ce_geocoded_df = pl.DataFrame(
    {
        "street_address_full": list(opais_ce_unique_addresses),
        "longitude": opais_ce_lon,
        "latitude": opais_ce_lat,
    }
)
opais_ce_child_df = opais_ce_child_df.join(
    opais_ce_geocoded_df,
    on="street_address_full",
    how="left",
)
opais_ce_child_df.write_parquet(
    "data/output/opais_ce_child_geocoded.parquet",
)


##### Geocode CP sites #########################################################

# %% Geocode the contract pharmacy sites
opais_cp_unique_addresses = set(
    opais_cp_fil_df["street_address_full"].to_list()
)
opais_cp_lon = []
opais_cp_lat = []

for address in opais_cp_unique_addresses:
    location = geocoder.geocode(address, exactly_one=True, timeout=10)
    if location:
        opais_cp_lon.append(location.longitude)
        opais_cp_lat.append(location.latitude)
    else:
        opais_cp_lon.append(None)
        opais_cp_lat.append(None)

# %% Construct a dataframe of the geocoding results and attach it
# back to the covered entity child dataframe
opais_cp_geocoded_df = pl.DataFrame(
    {
        "street_address_full": list(opais_cp_unique_addresses),
        "longitude": opais_cp_lon,
        "latitude": opais_cp_lat,
    }
)
opais_cp_fil_df = opais_cp_fil_df.join(
    opais_cp_geocoded_df,
    on="street_address_full",
    how="left",
)
opais_cp_fil_df.write_parquet(
    "data/output/opais_contract_pharmacies_geocoded.parquet",
)
