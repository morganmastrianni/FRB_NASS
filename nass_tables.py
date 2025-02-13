import polars as pl
import polars.selectors as cs

from great_tables import GT

dict = {
    "short_desc": "Description",
    "1": "Boston",
    "2": "New York (excl. PR and U.S. VI)",
    "3": "Philadelphia",
    "4": "Cleveland",
    "5": "Richmond",
    "6": "Atlanta",
    "7": "Chicago",
    "8": "St. Louis",
    "9": "Minneapolis",
    "10": "Kansas City",
    "11": "Dallas",
    "12": "San Francisco",
}

df = pl.read_parquet("custom_df.parquet")

df = df.pivot("District", values=cs.starts_with("District_Total"))

df = df.rename(dict)
print(df)

# refer to NASS for units
gt_df = GT(df)
gt_df.show()