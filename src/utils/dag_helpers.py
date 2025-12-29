# src/utils/dag_helpers.py
import json
from typing import Dict, List

from airflow.models.variable import Variable


def define_symbols_and_parameters() -> List[Dict[str, str]]:
    # List of symbols (obtained from an Airflow Variable, for example)
    api_key = Variable.get("alpha_vantage_api_key")
    symbols_json = Variable.get("symbols_to_track", default_var='["IBM", "MSFT"]')
    symbols_to_process = json.loads(symbols_json)

    base_url = "https://www.alphavantage.co/query"
    symbol_payloads = []

    for symbol in symbols_to_process:
        symbol_payloads.append(
            {
                "symbol": symbol,
                "base_url": base_url,
                "api_params": {
                    "function": "TIME_SERIES_WEEKLY_ADJUSTED",
                    "apikey": api_key,
                    "symbol": symbol,
                },
            }
        )
    return symbol_payloads
