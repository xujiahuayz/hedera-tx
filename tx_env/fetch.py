import asyncio
import gzip
import json
import logging
from typing import Any

import aiohttp

from tx_env.constants import DATA_PATH

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FetchData:
    def __init__(
        self,
        q: str = "transactions",
        suburl: str = "api/v1",
        rooturl: str = "https://mainnet-public.mirrornode.hedera.com",
    ):
        self.q = q
        self.suburl = suburl
        self.rooturl = rooturl

    def construct_query(
        self,
        limit: int = 100,
        account: str | None = "0.0.800",
        further_specs: str | None = None,
    ) -> str:
        """Constructs a query URL for fetching data from the Hedera mirror node."""
        query = f"/{self.suburl}/{self.q}?"
        if account:
            query += f"account.id={account}&"
        query += f"limit={limit}&order=asc"
        if further_specs:
            query += f"&{further_specs}"
        return query

    async def _fetch_data(
        self,
        session: aiohttp.ClientSession,
        request_url: str,
        max_retries: int = 13,
    ) -> dict[str, Any] | None:
        """Fetches data from the API with retry logic."""
        retries = 0
        while retries < max_retries:
            logger.info(
                f"Fetching: {request_url} (Attempt {retries + 1}/{max_retries})"
            )

            try:
                async with session.get(request_url) as response:
                    if response.status != 200:
                        logger.error(
                            f"Failed request {request_url} - HTTP {response.status}"
                        )
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                        )

                    response_text = await response.text()
                    response_data = json.loads(response_text)

                    if not response_data.get(self.q):
                        logger.warning(
                            f"Empty response at {request_url}, stopping fetch."
                        )
                        return None

                    return response_data

            except (aiohttp.ClientError, json.JSONDecodeError) as e:
                retries += 1
                wait_time = min(2**retries, 60)
                logger.error(
                    f"Error fetching {request_url}: {e}. Retrying in {wait_time} seconds."
                )
                await asyncio.sleep(wait_time)

        logger.error(f"Max retries reached for {request_url}, moving to next task.")
        return None

    async def save_with_pagination(
        self,
        session: aiohttp.ClientSession,
        file_name: str,
        query: str,
        max_retries: int = 13,
    ) -> None:
        """Fetches data from the API and saves it in a gzip-compressed JSONL format."""
        file_name_full = DATA_PATH / f"{file_name}.jsonl.gz"

        # Check if the file already exists and extract the last query
        if file_name_full.exists():
            logger.info(
                f"File {file_name_full} already exists, resuming from last entry."
            )
            with gzip.open(file_name_full, "rt") as f:
                last_line = None
                for line in f:
                    last_line = line
                if last_line:
                    try:
                        new_query = json.loads(last_line).get("links", {}).get("next")
                        if new_query:
                            query = new_query
                            logger.info(f"Resuming from: {query}")
                        else:
                            logger.info(f"No more data to fetch at {query}")
                            return
                    except json.JSONDecodeError:
                        logger.error(
                            f"Error parsing last line of {file_name_full}, starting fresh."
                        )

        while True:
            request_url = self.rooturl + query
            response_data = await self._fetch_data(session, request_url, max_retries)

            if not response_data:
                break

            # Append response data to file
            with gzip.open(file_name_full, "at") as f:
                f.write(json.dumps(response_data) + "\n")

            # Get the next query if available
            query = response_data.get("links", {}).get("next")
            if not query:
                logger.info(f"No more pages after {request_url}")
                break

    async def fetch_without_pagination(
        self,
        session: aiohttp.ClientSession,
        query: str,
        max_retries: int = 13,
    ) -> dict[str, Any] | None:
        """Fetches data from the API without pagination and returns it as a JSON string."""
        request_url = self.rooturl + query
        response_data = await self._fetch_data(session, request_url, max_retries)

        if response_data:
            return json.dumps(response_data)
        return None
