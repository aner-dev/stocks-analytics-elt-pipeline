-- fact_adjusted_prices.sql 
{{ config(
  materialized='incremental', 
  unique_key=['stock_id', 'date_id'],
  incremental_strategy='merge',
  tags=['gold']
) }}

WITH stg_data AS (
    -- 1. Select the clean data source (Silver Layer)
    -- This uses the alias defined in your sources.yml: stocks_silver_layer.weekly_adjusted_prices
    SELECT
        stock_symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        adjusted_close_price,
        trade_volume
    FROM {{ ref('stg_weekly_adjusted_prices') }} -- use 'ref' instead of 'source' because stg_stocks is a dbt model
    {% if is_incremental() %}
    WHERE trade_date > (SELECT MAX(trade_date) FROM {{ this }})
    {% endif %}
),

-- 2. Calculate the derived facts
calculated_facts AS (
    SELECT
        *,
        -- Derived Fact Calculation: Absolute Weekly Return
        (adjusted_close_price - open_price) AS weekly_return_abs,
        
        -- Derived Fact Calculation: Percentage Weekly Return
        -- NULLIF is used to prevent division by zero
        (adjusted_close_price - open_price) / NULLIF(open_price, 0) AS weekly_return_pct,

        -- Derived Fact Calculation: Absolute Trading Range
        (high_price - low_price) AS trading_range_abs,
        
        -- Derived Fact Calculation: Volume in USD (adjusted price * volume)
        (adjusted_close_price * trade_volume) AS volume_usd

    FROM stg_data
)

-- 3. Join the data with the dimensions to get the Foreign Keys (FKs)
SELECT
    -- Foreign Keys (FKs)
    dim_s.stock_id,
    dim_d.date_id,
    
    -- Transactional Facts (API)
    cf.open_price,
    cf.high_price,
    cf.low_price,
    cf.adjusted_close_price,
    cf.trade_volume,

    -- Derived Facts (Calculated)
    cf.weekly_return_abs,
    cf.weekly_return_pct,
    cf.trading_range_abs,
    cf.volume_usd,
    
    -- Metadata
    CURRENT_TIMESTAMP AS load_timestamp,
    '{{ this.name }}' AS source_model, -- Uses the dbt model name as the source
    '{{ invocation_id }}' AS execution_batch_id -- Unique ID of the dbt run 

FROM calculated_facts cf

-- Get the Foreign Key from the Stock Dimension (Join by natural key: symbol)
INNER JOIN {{ ref('dim_stock') }} dim_s
    ON cf.stock_symbol = dim_s.symbol

-- Get the Foreign Key from the Date Dimension (Join by natural key: trade_date)
INNER JOIN {{ ref('dim_date') }} dim_d
    ON cf.trade_date = dim_d.week_ending
