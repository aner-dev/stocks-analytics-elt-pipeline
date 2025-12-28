import polars as pl
import pandas as pd
import os
import sys
from typing import cast  # Importante para el LSP
from dotenv import load_dotenv

load_dotenv()

# Obtenemos el valor
raw_url = os.getenv("DB_URL")

# Validación para ejecución
if raw_url is None:
    print("❌ Error: DB_URL no encontrada.")
    sys.exit(1)

# Forzamos al LSP a entender que esto es un string sí o sí
DB_URL = cast(str, raw_url)

# Ahora el linter verá 'str' y no 'str | None'
df_pl = pl.read_database_uri(
    query="SELECT * FROM public.fact_adjusted_prices",
    uri=DB_URL,
    engine="connectorx",
)

df_pd = df_pl.to_pandas()

print("-" * 30)
print("📊 MEMORY AUDIT RESULTS")
print("-" * 30)
print(f"Polars (Arrow-native): {df_pl.estimated_size('mb'):.2f} MB")
print(
    f"Pandas (Python-objects): {df_pd.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
)
print("-" * 30)
