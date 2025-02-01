import polars as pl

# import matplotlib.pyplot as plt
import requests as req
import os
from dotenv import load_dotenv

load_dotenv()
url = "https://quickstats.nass.usda.gov/api/api_GET"
api_key = os.getenv("NASS_api_key")

cols_to_include = (
    "state_name",
    "state_fips_code",
    "county_name",
    "county_code",
    "asd_desc",
    "group_desc",
    "commodity_desc",
    "class_desc",
    "short_desc",
    "domain_desc",
    "domaincat_desc",
    "statisticcat_desc",
    "prodn_practice_desc",
    "util_practice_desc",
    "unit_desc",
    "District",
    "Value",
    "CV (%)",
)

# params = req.get(
#     f"{url}/get_param_values/?key={api_key}&param=short_desc"
# )
# print(params)

# df = pl.read_parquet("NASS_pull.parquet")
df = pl.read_parquet("NASS_pull.parquet")
df = df.select(cols_to_include)
print(df)

parameters = {col_name:df[col_name].unique() for col_name in df.columns[1:len(cols_to_include)-2]}



df_totals = df.filter(
    (pl.col("domain_desc") == "TOTAL")
    & (pl.col("statisticcat_desc") == "SALES")
    & (pl.col("unit_desc") == "HEAD")
    & (pl.col("commodity_desc") == "CATTLE")
)

# df_totals = df_totals.filter(~pl.col("Value").str.contains(r"\(D\)|\(Z\)"))
# print(df_totals)

print(
    (df_totals["Value"].str.replace_all(pattern=",", value="").cast(pl.Int64).sum())
    / 89400000000
)

df_totals = df_totals.with_columns(pl.lit(10).alias("District"))
print(df_totals)

# tenth district accounts for almost 1/3rd cash val of all cattle sales in the US
