from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

from dotenv import load_dotenv
from loguru import logger
import requests

logger.remove(0)
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

load_dir = Path(__file__).parent.parent / "load"
sys.path.append(str(load_dir))

DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / ".airflow.env"
load_dotenv(dotenv_path=DOTENV_PATH)

BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")


# extract data with retry logic, error handling and logging
def extract_data(BASE_URL: str, api_params: dict[str, str]) -> dict[str, Any]:
    try:
        logger.info(f"Making API request to {BASE_URL}")
        response = requests.get(BASE_URL, params=api_params, timeout=30, verify=True)
        response.raise_for_status()

        data = response.json()
        if "Error Message" in data or "Note" in data:
            logger.error(f"API returned an error message {data}")
            raise ValueError(f"API business error: {data}")

        logger.info(f"data extraction complete - {len(data)} records")
        return data

    except requests.JSONDecodeError as e:
        logger.error(f"Invalid JSON response from API: {e}")
        raise
    except requests.RequestException as e:
        logger.error(f"other API request error: {e}")
        raise


def symbols(symbols: list[str], BASE_URL: str, base_api_params: dict):
    """wrapper to call extract_data for each symbol"""
    from minio_client import load_to_minio  # type: ignore

    extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_date = datetime.now().strftime("%Y-%m-%d")

    for i, symbol in enumerate(symbols):
        symbol_params = base_api_params.copy()
        symbol_params["symbol"] = symbol

        try:
            data = extract_data(BASE_URL, symbol_params)
            if not data:
                continue
            # data lineage and enrichment with metadata
            enriched_data = {
                "metadata": {
                    "extraction_timestamp": extraction_timestamp,
                    "extraction_date": extraction_date,
                    "symbol": symbol,
                    "api_function": base_api_params["function"],
                    "total_symbols": len(symbols),
                    "current_symbol_index": i + 1,
                    "source_system": "alpha_vantage",
                },
                "api_data": data,
            }

            json_str = json.dumps(enriched_data)
            # handling duplicates in storage
            object_name = f"alpha_vantage/{symbol}/weekly_{extraction_timestamp}.json"
            file_key = load_to_minio(
                data=json_str.encode("utf-8"),
                bucket="stocks-raw",
                object_name=object_name,
            )
            logger.info(
                f"Succesfully extracted and loaded {symbol} ({i + 1}/{len(symbols)})"
            )
            logger.info(f"saved to: {file_key}")

        except Exception as e:
            logger.error(f"extract {symbol} failed: {e}")

        if (i + 1) % 5 == 0 and i < len(symbols) - 1:
            logger.info(
                "reached 5 requests. Waiting 60 seconds for rate limit reset..."
            )
            time.sleep(60)
        else:
            logger.info("small delay between requests...")
            time.sleep(2)

    logger.info("symbols processing complete")


if __name__ == "__main__":
    if BASE_URL is None:
        logger.error(
            "Error: BASE_URL environment variable is missing. Check .env file."
        )
        sys.exit(1)

    if API_KEY is None:
        logger.error("Error: API_KEY environment variable is missing. Check .env file.")
        sys.exit(1)

    symbols_list = [
        "AAPL",
        "IBM",
        "MSFT",
        "GOOGL",
        "TSLA",
        "NVDA",
        "META",
        "AMZN",
        "JPM",
        "V",
        "MA",
        "JNJ",
        "PFE",
        "UNH",
        "XOM",
        "CVX",
        "WMT",
        "PG",
        "KO",
        "CAT",
        "BA",
    ]
    base_api_params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "datatype": "json",
        "apikey": API_KEY,
    }

    symbols(symbols_list, BASE_URL, base_api_params)
