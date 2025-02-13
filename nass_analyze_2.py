import polars as pl

df = pl.read_parquet("custom_df.parquet")

# find highest-value commodities per district, top 10 rows

district_dfs = []

for dist in df.partition_by("District"):
    district_df = dist.sort("District_Total", descending=True).head(10)
    district_dfs.append(district_df)

df = pl.concat(district_dfs)
print(df)

df.write_parquet("custom_df.parquet")