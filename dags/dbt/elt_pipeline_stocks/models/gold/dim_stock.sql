{{ config(
    materialized='table'
) }}

WITH unique_symbols AS (
    SELECT DISTINCT
        stock_symbol AS symbol
    FROM {{ ref('stg_weekly_adjusted_prices') }}
),

metadata AS (
    SELECT * FROM {{ ref('stock_metadata') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.symbol']) }} AS stock_id,
    s.symbol,
    COALESCE(m.company_name, 'Unknown Company') AS company_name,
    COALESCE(m.sector, 'Unknown Sector') AS sector,
    COALESCE(m.industry, 'Unknown Industry') AS industry,
    TRUE AS is_active,
    CURRENT_TIMESTAMP AS created_at
FROM unique_symbols s
LEFT JOIN metadata m ON s.symbol = m.symbol
