import polars as pl
import matplotlib.pyplot as plt

df = pl.read_csv("NASS_pull.csv")

# pt map of counties by fed district
# for d in df.partition_by("District"):
#     plt.scatter(d["INTPTLON"], d["INTPTLAT"], s=2, alpha=0.5, label=d["District"][0])
# plt.xlim(-126, -67)
# plt.ylim(24, 50)
# plt.grid()
# plt.legend()
# plt.show()

df_totals = df.filter(
    (pl.col("domain_desc") == "TOTAL")
    & (pl.col("statisticcat_desc") == "SALES")
    & (pl.col("unit_desc") == "$")
)
print(df_totals)
df_totals = df_totals.filter(~pl.col("Value").str.contains("(D)"))
print(
    (df_totals["Value"].str.replace_all(pattern=",", value="").cast(pl.Int64).sum())
    / 89400000000
)

# tenth district accounts for almost 1/3rd cash val of all cattle sales in the US
