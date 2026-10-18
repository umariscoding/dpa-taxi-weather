import matplotlib
matplotlib.use("Agg")  # no screen inside docker
import matplotlib.pyplot as plt
import pandas as pd

labels = {
    "trips_pct": "change in trips per day (%)",
    "duration_pct": "change in average trip time (%)",
    "fare_pct": "change in average fare (%)",
}

for factor in ["rain", "snow", "cold", "hot"]:
    data = pd.read_csv(f"outputs/{factor}_change.csv")
    for column, label in labels.items():
        df = data.dropna(subset=[column]).sort_values(column)
        picked = pd.concat([df.head(10), df.tail(10)])
        plt.figure(figsize=(9, 7))
        plt.barh(picked["Zone"], picked[column])
        plt.axvline(0, color="black", linewidth=0.8)
        plt.xlabel(label)
        plt.title(f"{factor}: 10 biggest drops and 10 biggest rises")
        plt.tight_layout()
        plt.savefig(f"outputs/{factor}_{column}.png")
        plt.close()
