/*
Dataset: gold
Table: payment_dim
Description: This dimension table maps numeric payment type codes to human-readable descriptions for NYC taxi trips

Version      Last Modified             Changes 
1.0          2025-07-13                Initial creation of the table
2.0          2025-07-19                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'payment_dim'
) }}

SELECT
    payment_type_id,
    payment_type_desc,
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM
    {{ ref('payment_types') }}