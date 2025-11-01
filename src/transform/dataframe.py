from pathlib import Path
import sys

from loguru import logger
import polars as pl


load_dir = Path(__file__).parent.parent / "load"
sys.path.append(str(load_dir))

logger.remove(0)
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")


def create_dataframe(raw_data: dict) -> pl.DataFrame:
    """Transform raw_data into a clean and validated DataFrame"""

    if "api_data" not in raw_data:
        raise ValueError("api_data key not found in raw_data")

    api_data = raw_data["api_data"]
    time_series_key = "Weekly Adjusted Time Series"

    if time_series_key not in api_data:
        available_keys = list(api_data.keys())
        raise ValueError(
            f"'{time_series_key} not found in api_data param. Available keys: {available_keys}"
        )

    time_series = api_data[time_series_key]
    logger.info(f"number of registers: {len(time_series)}")

    records = [{"timestamp": date, **values} for date, values in time_series.items()]
    df = pl.DataFrame(records)
    logger.info(f"initial shape of dataset: {df.shape}")

    # data transform
    df_clean = (
        df.with_columns(
            [
                pl.col("1. open").cast(pl.Float64),
                pl.col("2. high").cast(pl.Float64),
                pl.col("3. low").cast(pl.Float64),
                pl.col("4. close").cast(pl.Float64),
                pl.col("5. adjusted close").cast(pl.Float64),
                # volume of transactions tipically its a integer (previous type: Float64)
                pl.col("6. volume").cast(pl.Int64),
                pl.col("timestamp").str.strptime(pl.Date, "%Y-%m-%d"),
            ]
        )
        .filter(
            # filtering nulls and finites
            pl.col("1. open").is_finite(),
            pl.col("2. high").is_finite(),
            pl.col("3. low").is_finite(),
            pl.col("4. close").is_finite(),
            pl.col("5. adjusted close").is_finite(),
            pl.col("6. volume").is_finite(),
            # business logic
            pl.col("4. close") > 0,
            pl.col("6. volume") > 0,
            pl.col("2. high") >= pl.col("3. low"),
            pl.col("2. high") >= pl.col("4. close"),
            pl.col("3. low") <= pl.col("4. close"),
        )
        # 3. casting, renaming and final selection
        .select(
            [
                pl.col("timestamp"),
                pl.col("1. open").alias("open"),
                pl.col("2. high").alias("high"),
                pl.col("3. low").alias("low"),
                pl.col("4. close").alias("close"),
                pl.col("5. adjusted close").alias("adjusted_close"),
                pl.col("6. volume").alias("volume"),
            ]
        )
        .sort("timestamp")
    )
    logger.info(f"Transformation complete. Final shape: {df_clean.shape}")
    return df_clean


if __name__ == "__main__":
    try:
        from minio_client import load_raw_stock

        raw_data = load_raw_stock("stocks-raw")
        df_clean = create_dataframe(raw_data)

        logger.info("transformation complete.")
        logger.info(f"dataframe: {df_clean}")
        logger.info(f"estimated size: {df_clean.estimated_size()}")

        logger.info(f"Summary: {df_clean.describe()}")
        logger.info(f"final DataFrame shape: {df_clean.shape}")

    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
