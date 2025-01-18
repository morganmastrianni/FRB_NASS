import polars as pl
import requests as req
import os
from dotenv import load_dotenv
import json
from alive_progress import alive_bar

load_dotenv()
base_url = "https://quickstats.nass.usda.gov/api/"
api_key = os.getenv("NASS_api_key")
# rsc = "api_GET"
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

# def get_nass_quickstats(frb_dist, rsc, params):
#     url = f"{base_url}/{rsc}/?key={api_key}&{params}"
#     req.get(url).text

# frb = "kc"
# state = "ME"
# get_nass_quickstats(frb, income)

# raw = req.get(
#     f"{base_url}/api_GET/?key={api_key}&agg_level_desc=COUNTY&commodity_desc=CATTLE&state_alpha={state}&source_desc=CENSUS&year=2022&format=CSV"
# )

pairs_1 = [
    (state, county) for district, state, county in tuples if district == 1
]

dfs = []
with alive_bar(len(pairs_1), title='Pairs') as bar:
    for state, county in pairs_1:
        bar()
        url = f"{base_url}api_GET"
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

        content = json.loads(raw)
        if 'error' in content:
            print(state, county)
            print(content['error'])
            continue

        df = pl.DataFrame(json.loads(raw)['data'])
        df = df.select([pl.col(c) for c in sorted(df.columns)])
        dfs.append(df)

pl.concat(dfs).write_parquet('NASS_pull.parquet')


# params = req.get(
#     f"{base_url}/get_param_values/?key={api_key}&param=short_desc"
# )

# NASS accepts value=1&value=2, not value=1,2

# if raw.status_code == 200:
#     data = raw.text
#     df = pl.read_csv(StringIO(data))
#     print(df)
#     df.write_csv("NASS_df.csv")
# else:
#     print(f"Womp womp... {raw.status_code}")

# if raw.status_code == 200:
#     data = raw.text
#     df = pl.read_csv(StringIO(data))
#     print(df)
#     df.write_csv("NASS_df.csv")
# else:
#     print(f"Womp womp... {raw.status_code}")
