import gzip
import json

from tx_env.constants import DATA_PATH


with gzip.open(DATA_PATH / "balances_800.jsonl.gz") as f:
    # iterate through each line in the file
    for line in f:
        # load the json line
        print(json.loads(line))
