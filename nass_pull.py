import polars as pl
import requests as req
import os
from dotenv import load_dotenv
from io import StringIO
# import json

load_dotenv()
base_url = "https://quickstats.nass.usda.gov/api/"
api_key = os.getenv("NASS_api_key")
# rsc = "api_GET"
df = pl.read_csv("FedCounties.csv")
tuples = []

for dist in range(1, 13):
    filtered = df.filter(df["District"] == dist)
    tuples.extend(
        zip(
            filtered["District"].to_list(),
            filtered["STATEFP"].to_list(),
            filtered["COUNTYFP"].to_list(),
        )
    )
tuples = [(t[0], t[1].zfill(2), t[2].zfill(3)) for t in tuples]

district = 1

# def get_nass_quickstats(frb_dist, rsc, params):
#     url = f"{base_url}/{rsc}/?key={api_key}&{params}"
#     req.get(url).text

# frb = "kc"
state = "ME"
# get_nass_quickstats(frb, income)

# raw = req.get(
#     f"{base_url}/api_GET/?key={api_key}&agg_level_desc=COUNTY&commodity_desc=CATTLE&state_alpha={state}&source_desc=CENSUS&year=2022&format=CSV"
# )

# params = req.get(
#     f"{base_url}/get_param_values/?key={api_key}&param=short_desc"
# )

for i in tuples:
    if i[0] == district:

req.get()
# NASS accepts value=1&value=2, not value=1,2

# if raw.status_code == 200:
#     data = raw.text
#     df = pl.read_csv(StringIO(data))
#     print(df)
#     df.write_csv("NASS_df.csv")
# else:
#     print(f"Womp womp... {raw.status_code}")
