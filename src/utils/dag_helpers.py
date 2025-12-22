# src/utils/dag_helpers.py
from airflow.models.variable import Variable
from typing import List, Dict


def define_symbols_and_parameters() -> List[Dict[str, str]]:
    # List of symbols (obtained from an Airflow Variable, for example)
    api_key = Variable.get("alpha_vantage_api_key")

    SYMBOLS_TO_PROCESS = [
        "IBM",
        "MSFT",
        "NVDA",
        "GOOGL",
        "AAPL",
        "AMZN",
        "V",
        "MA",
        "TSCO.LON",
        "SHOP.TRTO",
    ]
    BASE_URL = "https://www.alphavantage.co/query"

    symbol_payloads = []
    for symbol in SYMBOLS_TO_PROCESS:
        symbol_payloads.append(
            {
                "symbol": symbol,
                "base_url": BASE_URL,
                "api_params": {
                    "function": "TIME_SERIES_WEEKLY_ADJUSTED",
                    "apikey": api_key,
                    "symbol": symbol,
                },
            }
        )
    return symbol_payloads
