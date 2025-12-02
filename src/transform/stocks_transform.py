from datetime import datetime, timezone
import io


import polars as pl

import structlog

log = structlog.get_logger()


# added modularity on validation:
# refactorize validation logic creating a helper function that handles it
# dataframe_transform only is responsable for transformation
def _validate_time_series(raw_data: dict) -> dict:
    """validating stocks symbols"""

    if "api_data" not in raw_data:
        raise ValueError("api_data key not found in raw_data")

    api_data = raw_data["api_data"]
    time_series_key = "Weekly Adjusted Time Series"

    if time_series_key not in api_data:
        available_keys = list(api_data.keys())
        raise ValueError(
            f"'{time_series_key} not found in api_data param. Available keys: {available_keys}"
        )

    return api_data[time_series_key]


def dataframe_transform(raw_data: dict, symbol: str) -> pl.DataFrame:
    """Transform raw_data into a clean and validated DataFrame"""

    ingestion_timestamp = datetime.now(timezone.utc)
    log.info(f" Data ingestion started at: {ingestion_timestamp}")

    time_series = _validate_time_series(raw_data)
    log.info(f" number of registers: {len(time_series)}")

    records = [{"timestamp": date, **values} for date, values in time_series.items()]
    df = pl.DataFrame(records)
    # auditory metadata
    df = df.with_columns(
        [
            pl.lit(symbol).alias("symbol"),
            pl.lit(ingestion_timestamp).alias("_ingestion_timestamp"),
            pl.lit("alpha_vantage").alias("_data_source"),
            pl.lit(datetime.now(timezone.utc).date()).alias("_processing_date"),
        ]
    )
    initial_shape = df.shape
    initial_rows = initial_shape[0]
    initial_cols = initial_shape[1]

    log.info(f"initial shape of dataset: {initial_shape}")

    # transformation
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
                pl.col("symbol"),
                pl.col("1. open").alias("open"),
                pl.col("2. high").alias("high"),
                pl.col("3. low").alias("low"),
                pl.col("4. close").alias("close"),
                pl.col("5. adjusted close").alias("adjusted_close"),
                pl.col("6. volume").alias("volume"),
                pl.col("_ingestion_timestamp"),
                pl.col("_data_source"),
                pl.col("_processing_date"),
            ]
        )
        .sort("timestamp")
    )
    final_rows, final_cols = df_clean.shape
    rejected_rows = initial_rows - final_rows
    rejection_rate = (rejected_rows / initial_rows) * 100 if initial_rows > 0 else 0

    processing_duration = (
        datetime.now(tz=timezone.utc) - ingestion_timestamp
    ).total_seconds()

    audit_metrics = {
        "timing_metrics": {
            "ingestion_timestamp": ingestion_timestamp.isoformat(),
            "processing_duration_seconds": round(processing_duration, 2),
            "records_per_second": round(final_rows / processing_duration, 2)
            if processing_duration > 0
            else 0,
        },
        "volume_metrics": {
            "initial_records": initial_rows,
            "final_records": final_rows,
            "rejected_records": rejected_rows,
            "rejection_rate_percent": round(rejection_rate, 2),
        },
        "schema_metrics": {
            "initial_columns": initial_cols,
            "final_columns": final_cols,
            "columns_renamed": True,
        },
        "quality_metrics": {
            "data_retention_rate": round((final_rows / initial_rows) * 100, 2),
            "completeness_score": "100%" if final_rows > 0 else "0%",
            "data_freshness_days": 0,
        },
    }

    log.info(f"Transformation complete. Final shape: {df_clean.shape}")
    log.info("AUDIT REPORT:")
    log.info(f"  - Records: {initial_rows} → {final_rows} (rejected: {rejected_rows})")
    log.info(
        f"  - Rejection rate: {audit_metrics['volume_metrics']['rejection_rate_percent']}%"
    )
    log.info(
        f"  - Data retention: {audit_metrics['quality_metrics']['data_retention_rate']}%"
    )
    log.info(
        f"  - Processing time: {audit_metrics['timing_metrics']['processing_duration_seconds']}s"
    )

    # for detailed debugging
    log.debug(f"Full audit metrics: {audit_metrics}")

    return df_clean


def read_parquet_by_key(bucket: str, object_name: str) -> pl.DataFrame:
    """Lee un archivo Parquet de MinIO y lo carga como DataFrame"""
    bytes_data = download_from_minio(bucket, object_name)
    return pl.read_parquet(io.BytesIO(bytes_data))
