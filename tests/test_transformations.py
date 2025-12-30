from datetime import datetime

import polars as pl

from src.transform.polars_transform import standardize_and_clean_df


def test_resilience_against_negative_prices():
    """Verify that the cleaning logic removes records with negative prices."""
    # 1. Simulate dirty JSON coming from the API
    raw_data = {
        "Weekly Adjusted Time Series": {
            "2023-10-20": {
                "1. open": "150.0",
                "2. high": "155.0",
                "3. low": "149.0",
                "4. close": "-10.0",
                "5. adjusted close": "148.0",
                "6. volume": "1000",
            },
            "2023-10-27": {
                "1. open": "152.0",
                "2. high": "158.0",
                "3. low": "151.0",
                "4. close": "155.0",
                "5. adjusted close": "154.0",
                "6. volume": "2000",
            },
        }
    }

    # 2. Execute the actual function
    df_clean, initial_count = standardize_and_clean_df(raw_data, "AAPL", datetime.now())

    # 3. Validations
    assert initial_count == 2
    assert df_clean.height == 1  # Only the positive record should remain
    assert (df_clean["close"] >= 0).all()


def test_schema_integrity():
    """Verify that the final data types are correct for the database."""
    raw_data = {
        "Weekly Adjusted Time Series": {
            "2023-10-27": {
                "1. open": "152.0",
                "2. high": "158.0",
                "3. low": "151.0",
                "4. close": "155.0",
                "5. adjusted close": "154.0",
                "6. volume": "2000",
            }
        }
    }

    df_clean, _ = standardize_and_clean_df(raw_data, "AAPL", datetime.now())

    assert df_clean.schema["close"] == pl.Float64
    assert df_clean.schema["volume"] == pl.Int64
    assert df_clean.schema["price_date"] == pl.Date
