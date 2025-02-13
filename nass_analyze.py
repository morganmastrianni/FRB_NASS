import polars as pl

df = pl.read_parquet("NASS_pull.parquet")
df = df.filter(~pl.col("Value").str.contains(r"\(D\)|\(Z\)"))
df = df.with_columns(pl.col("Value").str.replace_all(",", "").cast(pl.Float64))

district_dfs = []

for dist in df.partition_by("District"):
    district_df = dist.group_by("short_desc").agg(
        [pl.sum("Value").alias("District_Total"), pl.mean("District").cast(pl.Int32)]
    )
    district_dfs.append(district_df)

df = pl.concat(district_dfs)

districts = range(1, 13)
# districts = [10, 11, 12]

# keyword searching
keyword_list = []
excl_keyword_list = []

# farmland_keywords = ["acres", "irrigated", "cropland"]
# keyword_list.append(farmland_keywords)
# farmland_excl_keywords = []
# excl_keyword_list.append(farmland_excl_keywords)

# cattle_keywords = ["cattle", "\$", "sales"]
# keyword_list.append(cattle_keywords)
# cattle_excl_keywords = ["excl"]
# excl_keyword_list.append(cattle_excl_keywords)

commodity_keywords = ["sales, measured in \$"]
keyword_list.append(commodity_keywords)
commodity_excl_keywords = []
excl_keyword_list.append(commodity_excl_keywords)

dfs = []

for incl, excl in zip(keyword_list, excl_keyword_list):
    custom = df.filter(
        [pl.col("short_desc").str.to_lowercase().str.contains(k.lower()) for k in incl],
        *[
            ~pl.col("short_desc").str.to_lowercase().str.contains(s.lower())
            for s in excl
        ],
        pl.col("District").is_in(districts),
    )
    print(custom)
    dfs.append(custom)

custom = pl.concat(dfs)
print(custom)

custom.write_parquet("custom_df.parquet")

# if more sorting and trimming needs to be conducted (ex: find highest-val commodities in each dist), go to nass_analyze_2.py; otherwise, go to nass_tables to format output
