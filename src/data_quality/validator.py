import polars as pl
from polars import exceptions
from typing import Dict, Any

import structlog

log = structlog.get_logger()


def validate_raw_payload(extracted_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    """
    Validates the structure and minimum quality of the raw data (JSON)
    before loading it to the Bronze layer.

    If validation fails (e.g., critical missing data), an exception is raised.
    """

    raw_json_data = extracted_data["raw_json"]
    symbol = extracted_data["symbol"]

    # --- CONFIGURATION ---
    KEY_TIME_SERIES = "Weekly Adjusted Time Series"

    # --- 1. API ERROR CHECK ---
    if "Error Message" in raw_json_data:
        log.error(f"API Error detected: {raw_json_data['Error Message']}")
        raise ValueError("API returned an error message. Check limits or symbol.")

    # --- 2. STRUCTURE CHECK (Time Series Key) ---
    if KEY_TIME_SERIES not in raw_json_data:
        log.error("Missing time series key in JSON response.")
        raise KeyError(
            f"JSON response is incomplete. Key '{KEY_TIME_SERIES}' not found."
        )

    time_series = raw_json_data[KEY_TIME_SERIES]

    # 3. Flatten and create Polars DataFrame
    # Use the API key as the column name (e.g., "1. open")
    records = [{"timestamp": date, **values} for date, values in time_series.items()]
    df_for_validation = pl.DataFrame(records)

    log.info(f"Starting validation for {symbol}. Rows: {df_for_validation.height}")

    # --- 4. CRITICAL VALIDATION (STRUCTURE AND QUALITY CHECKS) ---

    # Check 4.1: Is data empty?
    if df_for_validation.height == 0:
        raise ValueError("DataFrame is empty. No time series data found.")

    # Check 4.2: Can dates be converted?
    try:
        df_for_validation = df_for_validation.with_columns(
            pl.col("timestamp").str.to_date("%Y-%m-%d").alias("date_check")
        )
        null_dates = df_for_validation.select(pl.col("date_check").null_count()).item()
        if null_dates > 0:
            raise ValueError(f"{null_dates} null dates for {symbol}")
    except pl.exceptions.ComputeError as e:
        log.error(f"Error in date casting: {e}")
        raise ValueError("Invalid dates detected in raw data.") from e

    # Check 4.3: Are there critical nulls in the date column? (Date must never be missing)
    if df_for_validation.select(pl.col("date_check").null_count()).item() > 0:
        raise ValueError("Critical null values detected in the date column.")

    # Check 4.4: Are all expected columns present? (Basic integrity check)
    required_cols = ["1. open", "4. close", "6. volume"]
    for col in required_cols:
        if col not in df_for_validation.columns:
            raise KeyError(f"Critical column '{col}' missing in the API response.")

    # Check 4.5: Can numeric data be cast? (Attempt a simple casting)
    try:
        df_for_validation.select(
            [pl.col("^\\d\\.\\s.*$").cast(pl.Float64, strict=True)]
        )
    except pl.exceptions.ComputeError as e:
        raise ValueError(
            f"Numeric casting failed. Corrupt or non-numeric data: {e}"
        ) from e

    log.info("✔️ Data Quality and structure validation successful.")
    # If successful, return the original JSON

    return extracted_data
