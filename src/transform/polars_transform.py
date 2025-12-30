# src/transform/polars_transform.py

from datetime import datetime, timezone
from typing import Tuple

import polars as pl


def standardize_and_clean_df(
    raw_data_json: dict, symbol: str, ingestion_timestamp: datetime
) -> Tuple[pl.DataFrame, int]:
    """
    Takes the raw JSON data and applies all Polars transformation logic
    (flattening, casting, filtering, auditing).

    Returns the cleaned DataFrame and the initial row count.
    """

    time_series_key = "Weekly Adjusted Time Series"
    time_series = raw_data_json.get(time_series_key, {})

    # 1. Extraction and Flattening
    records = [{"timestamp": date, **values} for date, values in time_series.items()]
    df = pl.DataFrame(records)
    initial_rows = df.height

    if initial_rows == 0:
        return pl.DataFrame(), 0

    # 2. Rename Alpha Vantage columns (CRITICAL STEP)
    # improve legibility and avoid unnecesary verbosity on syntax & code
    df = df.rename(
        {
            "timestamp": "price_date",
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. adjusted close": "adjusted_close",
            "6. volume": "volume",
        }
    )

    # 3. Auditing & Metadata
    df = df.with_columns(
        [
            pl.lit(symbol).alias("symbol"),
            pl.lit(ingestion_timestamp).alias("_ingestion_timestamp"),
            pl.lit("alpha_vantage").alias("_data_source"),
            pl.lit(datetime.now(timezone.utc).date()).alias("_processing_date"),
        ]
    )

    # 4. Transformations (NO regex, NO duplicate risk)
    df_clean = (
        df.with_columns(
            [
                pl.col("open").cast(pl.Float64, strict=False),
                pl.col("high").cast(pl.Float64, strict=False),
                pl.col("low").cast(pl.Float64, strict=False),
                pl.col("close").cast(pl.Float64, strict=False),
                pl.col("adjusted_close").cast(pl.Float64, strict=False),
                pl.col("volume").cast(pl.Int64, strict=False),
                pl.col("price_date").str.strptime(pl.Date, "%Y-%m-%d"),
            ]
        )
        .filter(
            pl.all_horizontal(pl.col(["open", "close"]).is_finite()),
            pl.col("close") >= 0,
            pl.col("volume") >= 0,
        )
        .select(
            [
                "price_date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "adjusted_close",
                "volume",
                "_ingestion_timestamp",
                "_data_source",
                "_processing_date",
            ]
        )
        .sort("price_date")
    )

    return df_clean, initial_rows
