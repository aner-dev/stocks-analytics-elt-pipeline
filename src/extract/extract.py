import sys
from typing import Any
import requests
from loguru import logger
import os
from dotenv import load_dotenv
from pathlib import Path

logger.remove(0)
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "api.env"
print(f"DEBUG: Intentando cargar .env desde: {DOTENV_PATH}")
# Verifica si la ruta existe. DEBE imprimir True.
print(f"DEBUG: ¿Existe el archivo en esta ruta? {DOTENV_PATH.exists()}")

load_dotenv(dotenv_path=DOTENV_PATH)

BASE_URL = os.getenv("BASE_URL")
API_KEY = os.getenv("API_KEY")

# isolate API parameters for flexibility
# use a dict for performance optimization (hash table O(1))
api_params = {
    "function": "TIME_SERIES_WEEKLY_ADJUSTED",
    "symbol": "IBM",
    "datatype": "json",
    "apikey": API_KEY,
}


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


if __name__ == "__main__":
    if BASE_URL is None:
        logger.error(
            "Error: BASE_URL environment variable is missing. Check .env file."
        )
        sys.exit(1)

    if API_KEY is None:
        logger.error("Error: API_KEY environment variable is missing. Check .env file.")
        sys.exit(1)

    data = extract_data(BASE_URL, api_params)
    if data:
        logger.info("Succesfully extracted data")
    else:
        logger.error("Failed to extract data")
