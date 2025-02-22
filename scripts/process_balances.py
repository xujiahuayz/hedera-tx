import gzip
import json

import matplotlib.pyplot as plt
import pandas as pd

from tx_env.constants import DATA_PATH

# Read and process the data
with gzip.open(DATA_PATH / "balances_800.jsonl.gz") as f:
    data = [
        {"timestamp": w["timestamp"], **w["balances"][0]}
        for line in f
        if (w := json.loads(line))
    ]

# downsample for fast rendering
df = pd.DataFrame(data).iloc[::24, :]
df.sort_values("timestamp", inplace=True)

plt.figure(figsize=(12, 6))
# make timestamp human readable
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
plt.plot(df["timestamp"], df["balance"])
# log y-axis
plt.yscale("log")
# x-axis label

plt.show()
