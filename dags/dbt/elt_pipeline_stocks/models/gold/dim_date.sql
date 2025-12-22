{{ config(
    materialized='table',
    unlogged=True,
    indexes=[
      {'columns': ['week_ending'], 'unique': True},
      {'columns': ['date_id'], 'unique': True}
    ]
) }}

WITH unique_dates AS (
    SELECT DISTINCT
        trade_date AS week_ending
    FROM {{ ref('stg_weekly_adjusted_prices') }}
    WHERE trade_date IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['week_ending']) }} AS date_id,
    week_ending,
    EXTRACT(YEAR FROM week_ending) AS year,
    EXTRACT(WEEK FROM week_ending) AS week_number,
    EXTRACT(QUARTER FROM week_ending) AS quarter,
    CURRENT_TIMESTAMP AS created_at
FROM unique_dates
ORDER BY week_ending DESC
