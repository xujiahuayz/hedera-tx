import gzip
import json

from tx_env.constants import DATA_PATH

# identify all the files in the data folder that follows the format "transactions-*.jsonl.gz"
individual_tx_files = list(DATA_PATH.glob("transactions-*.jsonl.gz"))

# iterate through each file
for file in [
    "/Users/dsf-pro16-m3/projects/hedera-tx/data/transactions-1737072000-1737158400.jsonl.gz"
]:
    with gzip.open(file) as f:
        # iterate through each line in the file
        tx_this_file = []
        for line in f:
            # load the json line
            tx_this_file.extend(json.loads(line)["transactions"])
