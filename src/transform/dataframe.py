import sys
from pathlib import Path

load_dir = Path(__file__).parent.parent / "load"
sys.path.append(str(load_dir))

from minio_client import load_raw_stock
import polars as pl

raw_data = load_raw_stock("stocks-raw")
print("data extracted! keys:", list(raw_data.keys()))

if "Weekly Adjusted Time Series" in raw_data:
    time_series = raw_data["Weekly Adjusted Time Series"]
    print(f"📊 Número de registros: {len(time_series)}")
    print(f"types of registers: {type(time_series)}")
    print("📅 Primeras fechas:", list(time_series.keys())[:5])

records = [{"timestamp": date, **values} for date, values in time_series.items()]
df = pl.DataFrame(records)
print("shape of the dataset:", df.shape)


def transformation(raw_data: dict) -> pl.DataFrame:
    # data cleaning
    df_clean = (
        df.with_columns(
            [
                pl.col("1. open").cast(pl.Float64).alias("open"),
                pl.col("2. high").cast(pl.Float64).alias("high"),
                pl.col("3. low").cast(pl.Float64).alias("low"),
                pl.col("4. close").cast(pl.Float64).alias("close"),
                pl.col("5. adjusted close").cast(pl.Float64).alias("adjusted_close"),
                pl.col("6. volume").cast(pl.Float64).alias("volume"),
                pl.col("timestamp")
                .str.strptime(pl.Date, "%Y-%m-%d")
                .alias("timestamp"),
            ]
        )
        .filter()
        # Cleanup - drop duplicates
        .drop(
            [
                "1. open",
                "2. high",
                "3. low",
                "4. close",
                "5. adjusted close",
                "6. volume",
            ]
        )
    )


print("transformation complete")
print(df_clean.head(3))
