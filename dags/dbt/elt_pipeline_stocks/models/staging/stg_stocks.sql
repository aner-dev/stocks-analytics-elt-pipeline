-- models/staging/stg_stocks.sql
-- Declares that you are using the source defined in sources.yml
WITH source_data AS (
    SELECT *
    -- The source(source_name, table_name) macros are the canonical dbt way
    FROM {{ source('stocks_silver_layer', 'weekly_adjusted_stocks') }}
)

-- Final selection for the staging model
SELECT
    -- Rename columns for dimensional clarity
    symbol AS stock_symbol,
    price_date AS effective_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    -- Apply business logic (e.g., calculate a timestamp field for auditing)
    CURRENT_TIMESTAMP() AS dbt_load_timestamp
FROM source_data
WHERE 1 = 1
-- Basic data filters can be added here if needed
