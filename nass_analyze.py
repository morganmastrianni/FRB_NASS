import polars as pl

df = pl.read_parquet("NASS_pull.parquet")
df = df.filter(~pl.col("Value").str.contains(r"\(D\)|\(Z\)"))
df = df.with_columns(pl.col("Value").str.replace_all(",", "").cast(pl.Float64))

district_dfs = []
# j = 0
for dist in df.partition_by("District"):
    district_df = dist.group_by("short_desc").agg(
        [pl.sum("Value").alias("District_Total"), pl.mean("District").cast(pl.Int32)]
    )
    # print(district_df["short_desc"].unique().count())
    # print(len(pl.Series(district_df["District_Totals"])))
    # j += district_df["short_desc"].unique().count()
    district_dfs.append(district_df)

df = pl.concat(district_dfs)
# print(df)
# print(j)
# print(len(pl.Series(df["District_Totals"])))

# keyword searching
keywords = ["acres", "irrigated", "cropland", "harvested"]
excl_keywords = ["excl", "non"]
districts = range(1, 13) # or [10, 11, 12]

custom = df.filter(
    *[
        pl.col("short_desc").str.to_lowercase().str.contains(k.lower())
        for k in keywords
    ],
    *[
        ~pl.col("short_desc").str.to_lowercase().str.contains(excl.lower())
        for excl in excl_keywords
    ],
    pl.col("District").is_in(districts),
)

print(custom.sort("District_Total", descending=True))
print(custom["short_desc"][0])
