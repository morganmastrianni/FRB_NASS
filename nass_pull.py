import polars as pl
import requests as req
import os
from dotenv import load_dotenv
import json
from alive_progress import alive_bar
import csv

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

pairs_1 = [
    (state, county) for district, state, county in tuples if district == 1
]

load_dotenv()
url = "https://quickstats.nass.usda.gov/api/api_GET"
api_key = os.getenv("NASS_api_key")

from time import sleep

dfs = []
with alive_bar(len(pairs_1), title='Pairs') as bar:
    for state, county in pairs_1:
        bar()
        raw = req.get(
            url,
            params={
                "key": api_key,
                "state_fips_code": state,
                "county_code": county,
                "agg_level_desc": "COUNTY",
                "commodity_desc": "CATTLE",
                "source_desc": "CENSUS",
                "year": 2022,
                "format": "json"
            },
        ).text
        sleep(1)

        # if 'error' in content:
        #     sleep(1)

        # try:
        #     content = json.loads(raw)
        # except json.decoder.JSONDecodeError as e:
        #     print(raw)
        #     raise e
        
        if 'error' in content:
            print(state, county)
            print(content['error'])
            continue

        df = pl.DataFrame(json.loads(raw)['data'])
        df = df.select([pl.col(c) for c in sorted(df.columns)])
        dfs.append(df)

df = pl.concat(dfs)
df.write_parquet('NASS_pull.parquet')
df.write_csv("NASS_pull.csv")

# params = req.get(
#     f"{base_url}/get_param_values/?key={api_key}&param=short_desc"
# )

# if raw.status_code == 200:
#     data = raw.text
#     df = pl.read_csv(StringIO(data))
#     print(df)
#     df.write_csv("NASS_df.csv")
# else:
#     print(f"Womp womp... {raw.status_code}")

