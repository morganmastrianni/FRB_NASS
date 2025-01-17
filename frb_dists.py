import polars as pl
import matplotlib.pyplot as plt

df = pl.read_csv("FedCounties.csv")
# print(df)

for d in df.partition_by("District"):
    plt.scatter(d["INTPTLON"], d["INTPTLAT"], s=2, alpha=0.5, label=d["District"][0])
plt.xlim(-126, -67)
plt.ylim(24, 50)
plt.grid()
plt.legend()
plt.show()