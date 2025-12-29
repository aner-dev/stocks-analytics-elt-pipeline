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
  TRIM(TO_CHAR(week_ending, 'Month')) AS month_name,
    CASE WHEN EXTRACT(MONTH FROM week_ending) IN (3, 6, 9, 12) THEN TRUE ELSE FALSE END AS is_quarter_reporting_month,
    EXTRACT(DOW FROM week_ending) IN (0, 6) AS is_weekend,
    CURRENT_TIMESTAMP AS created_at
FROM unique_dates
