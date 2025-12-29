{{ config(materialized='table') }}

WITH source_definition AS (
    SELECT 
        'alpha_vantage' AS source_name,
        'Weekly Adjusted Prices' AS data_content,
        'Gold' AS source_tier,
        'Weekly' AS update_frequency,
        'v1' AS api_version,
        'https://www.alphavantage.co/documentation/' AS documentation_url,
        'REST API' AS extraction_method
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['source_name', 'api_version']) }} AS source_id,
    source_name,
    data_content,
    source_tier,
    update_frequency,
    api_version,
    documentation_url,
    extraction_method,
    CURRENT_TIMESTAMP AS created_at
FROM source_definition
