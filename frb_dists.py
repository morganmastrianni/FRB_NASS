import polars as pl
import matplotlib.pyplot as plt

df = pl.read_csv("FedCounties.csv")

# pt map of counties by fed district
# for d in df.partition_by("District"):
#     plt.scatter(d["INTPTLON"], d["INTPTLAT"], s=2, alpha=0.5, label=d["District"][0])
# plt.xlim(-126, -67)
# plt.ylim(24, 50)
# plt.grid()
# plt.legend()
# plt.show()
tuples = []

for dist in range(1, 13):
    filtered = df.filter(df["District"] == dist)
    tuples.extend(
        zip(
            filtered["District"].to_list(),
            filtered["STATEFP"].to_list(),
            filtered["COUNTYFP"].to_list(),
        )
    )

district = 1

j = 0
for i in tuples:
    if i[0] == district:
        j += 1

print(j)
