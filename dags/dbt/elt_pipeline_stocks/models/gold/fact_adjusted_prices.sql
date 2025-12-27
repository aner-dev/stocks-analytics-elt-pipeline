{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_weekly_adjusted_prices') }}
),

dim_stock AS (
    SELECT * FROM {{ ref('dim_stock') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    -- Keys
    s.stock_id,
    d.date_id,
    
    -- Denormalized Attributes
    sd.stock_symbol AS symbol,
    s.company_name,
    sd.trade_date,

    -- Measures
    ROUND(sd.open_price::numeric, 4) AS open_price,
    ROUND(sd.high_price::numeric, 4) AS high_price,
    ROUND(sd.low_price::numeric, 4) AS low_price,
    ROUND(sd.adjusted_close_price::numeric, 4) AS adjusted_close_price,
    sd.trade_volume,
    
    -- Calculated Metrics
    ROUND((sd.adjusted_close_price - sd.open_price)::numeric, 4) AS weekly_return_abs,
    ROUND(((sd.adjusted_close_price / NULLIF(sd.open_price, 0)) - 1)::numeric, 4) AS weekly_return_pct,
    ROUND((sd.high_price - sd.low_price)::numeric, 4) AS trading_range_abs,
    ROUND(((sd.high_price / NULLIF(sd.low_price, 0)) - 1)::numeric, 4) AS volatility_pct,
    
    -- Metadata
    sd._ingestion_timestamp AS load_timestamp,
    'alpha_vantage' AS source_model

FROM source_data sd
JOIN dim_stock s ON sd.stock_symbol = s.symbol
JOIN dim_date d ON sd.trade_date = d.week_ending
