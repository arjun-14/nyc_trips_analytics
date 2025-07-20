/*
Dataset: gold
Table: ratecode_dim
Description: This dimension table provides descriptive mappings for rate codes used in NYC taxi trips, 
             enabling better interpretation and analysis of fare rate types

Version      Last Modified             Changes 
1.0          2025-07-13                Initial creation of the table
2.0          2025-07-19                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'ratecode_dim'
) }}

SELECT
    ratecode_id,
    ratecode_desc,
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM
    {{ ref('ratecodes') }}