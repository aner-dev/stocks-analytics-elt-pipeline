from datetime import datetime
import os
import time
from typing import Any
import requests

import structlog

log = structlog.get_logger()

# FIXME: ty lsp doesn't work; replace with pyright (create lspconfig.lua)
BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")

symbols_list = [
    "AAPL",
    "IBM",
]


# extract data with retry logic, error handling and logging
def extract_stocks_data(BASE_URL: str, api_params: dict[str, str]) -> dict[str, Any]:
    try:
        log.info(f"Making API request to {BASE_URL}")
        response = requests.get(BASE_URL, params=api_params, timeout=30, verify=True)
        response.raise_for_status()

        data = response.json()
        if "Error Message" in data or "Note" in data:
            log.error(f"API returned an error message {data}")
            raise ValueError(f"API business error: {data}")

        log.info(f"data extraction complete - {len(data)} records")
        return data

    except requests.JSONDecodeError as e:
        log.error(f"Invalid JSON response from API: {e}")
        raise
    except requests.RequestException as e:
        log.error(f"other API request error: {e}")
        raise


if __name__ == "__main__":
    if BASE_URL is None:
        log.error("Error: BASE_URL environment variable is missing. Check .env file.")
        sys.exit(1)

    if API_KEY is None:
        log.error("Error: API_KEY environment variable is missing. Check .env file.")
        sys.exit(1)

    base_api_params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "datatype": "json",
        "apikey": API_KEY,
    }

    symbols(symbols_list, BASE_URL, base_api_params)
