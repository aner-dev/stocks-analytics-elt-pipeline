-- models/staging/stg_weekly_adjusted_prices.sql

WITH source_data AS (
    SELECT *
    FROM {{ source('stocks_silver_layer', 'weekly_adjusted_prices') }}
)

-- Final selection for the staging model
SELECT
    -- Rename columns for dimensional clarity
    TRIM(UPPER(symbol)) AS stock_symbol,
    price_date AS trade_date,
    open AS open_price,
    high AS high_price,
    low AS low_price,
    close AS close_price,
    adjusted_close AS adjusted_close_price,
    volume AS trade_volume,

    _ingestion_timestamp,
    _processing_date,
    -- Apply business logic (e.g., calculate a timestamp field for auditing)
    CURRENT_TIMESTAMP AS dbt_load_timestamp

FROM source_data
WHERE adjusted_close IS NOT NULL
-- Basic data filters can be added here if needed
