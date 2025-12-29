import time

import duckdb
import pandas as pd
import polars as pl


def benchmark(tool, load_func):
    start = time.time()
    result = load_func()
    end = time.time()
    print(f"{tool}: {end-start:.2f}s - Rows: {len(result)}")


print("benchmark time")
df_pandas = benchmark("Pandas", lambda: pd.read_csv("extracted_data.csv"))
df_polars = benchmark(
    "Polars", lambda: pl.read_csv("extracted_data.csv", ignore_errors=True)
)
df_duckdb = benchmark("Duckdb", lambda: duckdb.read_csv("extracted_data.csv").df())
