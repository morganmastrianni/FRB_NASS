import polars as pl
import requests as req
import os
from dotenv import load_dotenv
import json
from alive_progress import alive_bar
from time import sleep

fed_counties_df = pl.read_csv("FedCounties.csv")
tuples = []

for dist in range(1, 13):
    filtered = fed_counties_df.filter(fed_counties_df["District"] == dist)
    tuples.extend(
        zip(
            filtered["District"].to_list(),
            filtered["STATEFP"].to_list(),
            filtered["COUNTYFP"].to_list(),
        )
    )
tuples = [(t[0], str(t[1]).zfill(2), str(t[2]).zfill(3)) for t in tuples]

FedDist = 10  # kc fed (420 counties; 40:49.3 minutes)
pairs = [(state, county) for district, state, county in tuples if district == FedDist]

load_dotenv()
url = "https://quickstats.nass.usda.gov/api/api_GET"
api_key = os.getenv("NASS_api_key")

dfs = []
with alive_bar(len(pairs), title="Pairs") as bar:
    for state, county in pairs:
        bar()
        raw = req.get(
            url,
            params={
                "key": api_key,
                "state_fips_code": state,
                "county_code": county,
                "agg_level_desc": "COUNTY",
                "source_desc": "CENSUS",
                "year": 2022,
                "format": "json",
            },
        ).text
        sleep(2)

        try:
            content = json.loads(raw)
        except json.decoder.JSONDecodeError as e:
            print(raw)
            raise e

        if "error" in content:
            print(state, county)
            print(content["error"])
            continue

        df = pl.DataFrame(json.loads(raw)["data"])
        df = df.select([pl.col(c) for c in sorted(df.columns)])
        dfs.append(df)

df = pl.concat(dfs)
df.write_parquet("NASS_pull.parquet")
# df.write_csv("NASS_pull.csv")
