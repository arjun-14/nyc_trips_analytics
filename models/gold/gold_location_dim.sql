/*
Dataset: gold
Table: location_dim
Description: This dimension table provides reference data mapping Location IDs to NYC boroughs, 
                zones, and service zones. It is used to enrich trip data with geographic context in the gold layer.

Version      Last Modified             Changes 
1.0          2025-07-13                Initial creation of the table
2.0          2025-07-19                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'gold',
    alias = 'location_dim'
) }}

SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM
    {{ ref('silver_taxi_zone_lookup') }}