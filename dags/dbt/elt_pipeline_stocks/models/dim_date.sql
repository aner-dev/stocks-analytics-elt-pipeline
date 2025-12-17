-- models/dim_date.sql
{{ config(
    materialized='table',
    unique_key='week_ending'
) }}

WITH unique_dates AS (
    SELECT DISTINCT
        effective_date AS week_ending -- The date from stg_stocks.sql
    FROM {{ ref('stg_stocks') }} -- Use the staging model as the source
    WHERE effective_date IS NOT NULL
)

SELECT
    {{ dbt_utils.surrogate_key(['week_ending']) }} AS date_id, -- Generate surrogate key
    week_ending,
    EXTRACT(YEAR FROM week_ending) AS year,
    EXTRACT(WEEK FROM week_ending) AS week_number,
    EXTRACT(QUARTER FROM week_ending) AS quarter,
    CURRENT_TIMESTAMP() AS created_at
FROM unique_dates
