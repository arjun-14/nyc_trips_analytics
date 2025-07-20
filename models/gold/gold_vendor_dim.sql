/*
Dataset: gold
Table: vendor_dim
Description: This dimension table stores metadata for vendor identifiers used across Yellow Taxi, Green Taxi, 
                and High Volume For-Hire Vehicle services in NYC

Version      Last Modified             Changes 
1.0          2025-07-13                Initial creation of the table
2.0          2025-07-19                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'vendor_dim'
) }}

SELECT
    vendor_id,
    vendor_desc,
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM
    {{ ref('vendors') }}