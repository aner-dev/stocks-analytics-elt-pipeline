# debug_alpha.py
import requests
import os
from dotenv import load_dotenv
from pathlib import Path

DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "api.env"
load_dotenv(dotenv_path=DOTENV_PATH)

API_KEY = os.getenv("API_KEY")

url = "https://www.alphavantage.co/query"
params = {"function": "TIME_SERIES_WEEKLY_ADJUSTED", "symbol": "IBM", "apikey": API_KEY}

print("=== LLAMANDO ALPHA VANTAGE ===")
response = requests.get(url, params=params)
data = response.json()

print("=== KEYS EN LA RESPUESTA ===")
print(data.keys())

print("=== PRIMER ELEMENTO DE TIME SERIES ===")
if "Weekly Adjusted Time Series" in data:
    first_date = list(data["Weekly Adjusted Time Series"].keys())[0]
    print(f"Fecha: {first_date}")
    print(data["Weekly Adjusted Time Series"][first_date])
else:
    print("NO HAY 'Weekly Adjusted Time Series' en la respuesta")
    print("Respuesta completa:", data)
