/*
Dataset: bronze
Table: taxi_zone_lookup
Description: This table stores reference data mapping taxi zone Location IDs to boroughs, zones, and service zones in NYC.

Version      Last Modified             Changes 
1.0          2025-07-03                Initial creation of the table
2.0          2025-07-17                Migrated to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'bronze',
    alias = 'taxi_zone_lookup'
) }}

SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone,
    service_zone
FROM
    {{ source('bronze', 'ext_taxi_zone_lookup') }}
