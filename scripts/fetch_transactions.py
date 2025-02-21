import asyncio
from datetime import datetime, timedelta

import aiohttp
import pandas as pd

from tx_env.fetch import FetchData


async def fetch_main(time_intervals):
    async with aiohttp.ClientSession() as session:
        fetch = FetchData()
        tasks = [
            fetch.save_with_pagination(
                session=session,
                file_name=f"{fetch.q}-{greater_than}-{less_than}",
                query=fetch.construct_query(
                    limit=100,
                    account="0.0.800",
                    further_specs=f"timestamp=lt:{less_than}&timestamp=gt:{greater_than}",
                ),
            )
            for greater_than, less_than in time_intervals
        ]
        await asyncio.gather(*tasks)


# get timestamp of 2024-11-01 to 2025-02-01

start_time = datetime(2024, 11, 1)
end_time = datetime(2025, 2, 1)

# construct time intervals for 1 day intervals, starting from start_time, ending at end_time
time_intervals = [
    (int(start_time.timestamp()), int((start_time + timedelta(days=1)).timestamp()))
    for start_time in pd.date_range(start_time, end_time, freq="1D")
]

asyncio.run(fetch_main(time_intervals))
