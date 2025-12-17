-- models/dim_stock.sql
{{ config(
    materialized='table',
    unique_key='symbol'
) }}

WITH unique_symbols AS (
    SELECT DISTINCT
        stock_symbol AS symbol
    FROM {{ ref('stg_stocks') }} -- Get all unique symbols from the prices
)

-- NOTE: If I had a Silver table with metadata (name, sector), I would do a JOIN here.
SELECT
    {{ dbt_utils.surrogate_key(['symbol']) }} AS stock_id, -- Generate surrogate key
    symbol,
    -- NOTE: These columns must be populated. For now, they will be constants or nulls.
    'Unknown Company' AS company_name, 
    'Unknown Sector' AS sector,
    'Unknown Industry' AS industry,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM unique_symbols
