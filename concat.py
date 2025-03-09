import polars as pl
pl.read_parquet('./data/2022.parquet').write_parquet('2022.parquet')