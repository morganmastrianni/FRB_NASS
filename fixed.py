import polars as pl
import requests as req
import os
import json
from alive_progress import alive_bar
import time

def pull_single(state_fips_code: str, county_code: str, year: int):
    from dotenv import load_dotenv
    load_dotenv()

    raw = req.get(
        "https://quickstats.nass.usda.gov/api/api_GET",
        params={
            "key": os.getenv("NASS_api_key"),
            "state_fips_code": state_fips_code,
            "county_code": county_code,
            "agg_level_desc": "COUNTY",
            "source_desc": "CENSUS",
            "year": year,
            "format": "json",
        },
    ).text

    try:
        content = json.loads(raw)
    except json.decoder.JSONDecodeError as e: # 403, forbidden
        if '403' in raw:
            raise e
        else:
            raise RuntimeError(f"Got something else?? {raw}")

    if "error" in content:
        print(content["error"])
        raise ValueError()

    county_df = pl.DataFrame(json.loads(raw)["data"])
    county_df = county_df.select(
        [pl.col(c) for c in sorted(county_df.columns)]
    )
    return county_df

fed_counties_df = pl.read_csv("FedCounties.csv").filter(~pl.col("STATEFP").is_in([78, 72, 69, 66, 60]))

year = 2022
for dist in range(1, 13):
    df_dist = fed_counties_df.filter(fed_counties_df["District"] == dist)

    with alive_bar(df_dist.height, title=f"District {dist}") as bar:
        for row in df_dist.iter_rows(named=True):
            state, county = str(row["STATEFP"]).zfill(2), str(row['COUNTYFP']).zfill(3)
            skip = False
            save_path = f'./data/{year}.parquet/state_fips_code={state}/county_code={county:03}/00000000.parquet'
            if os.path.exists(save_path):
                skip = True
            with open('./data/bad_combos.csv', 'r') as f:
                for line in f:
                    if f'{state},{county}' in line:
                        skip = True
                        break
            
            if skip:
                bar(skipped=True)
                continue

            try:
                county_df = pull_single(state, county, year=year)
                time.sleep(1.5)
            except json.JSONDecodeError:
                print('Error occured, lets try again later')
            except ValueError:
                with open('./data/bad_combos.csv', 'a') as f:
                    f.write(f'{state},{county}\n')
                continue

            county_df = county_df.with_columns(pl.lit(dist).alias("district"))
            if county_df.height > 50_000:
                raise ValueError('County DataFrame has max number of rows, data likely missing')
            county_df.write_parquet(f'./data/{year}.parquet', partition_by=('state_fips_code', 'county_code'))
            bar()