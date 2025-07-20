/*
Dataset: silver
Table: taxi_zone_lookup
Description: This table stores reference data mapping taxi zone Location IDs to boroughs, zones, and service zones in NYC.

Version      Last Modified             Changes 
1.0          2025-07-13                Initial creation of the table with basic fields and derived columns
*/

{{ config(
    materialized = 'table',
    schema = 'silver',
    alias = 'taxi_zone_lookup'
) }}

SELECT
    location_id,
    borough,
    zone,
    service_zone,

    -- housekeeping columns
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM
    {{ ref('bronze_taxi_zone_lookup') }}