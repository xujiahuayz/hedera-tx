import asyncio
import gzip
from datetime import datetime, timedelta

import aiohttp
import aiofiles

from tx_env.constants import DATA_PATH
from tx_env.fetch import FetchData

start_time = datetime(2023, 1, 1)
end_time = datetime(2025, 2, 21)

# Construct time intervals for array of timestamps at 1-hour steps
times = [
    int((start_time + timedelta(hours=i)).timestamp())
    for i in range(0, int((end_time - start_time).total_seconds() / 3600))
]

OUTPUT_FILE = DATA_PATH / "balances_800.jsonl.gz"
CONCURRENCY_LIMIT = 10  # Limit the number of concurrent requests


async def fetch_and_write(
    session: aiohttp.ClientSession,
    fetch: FetchData,
    timestamp: int,
    semaphore: asyncio.Semaphore,
    file_lock: asyncio.Lock,
):
    """Fetch balance data and write it immediately to file."""
    async with semaphore:  # Limit concurrent requests
        query = fetch.construct_query(
            limit=100,
            account="0.0.800",
            further_specs=f"timestamp={timestamp}",
        )
        result = await fetch.fetch_without_pagination(session=session, query=query)

        if result:
            async with file_lock:  # Prevent concurrent writes
                async with aiofiles.open(OUTPUT_FILE, "ab") as f:
                    with gzip.GzipFile(fileobj=f, mode="ab") as gz_file:
                        gz_file.write((result + "\n").encode())

            print(f"Written data for timestamp: {timestamp}")
        else:
            print(f"No data fetched for timestamp: {timestamp}")


async def fetch_balances(timestamps: list[int]) -> None:
    """Fetch balance data concurrently and write results as we go."""
    fetch = FetchData(q="balances")
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    file_lock = asyncio.Lock()  # Prevent multiple coroutines from writing at once

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_and_write(session, fetch, ts, semaphore, file_lock)
            for ts in timestamps
        ]
        await asyncio.gather(*tasks)  # Run all tasks concurrently


async def main():
    await fetch_balances(times)


if __name__ == "__main__":
    asyncio.run(main())
