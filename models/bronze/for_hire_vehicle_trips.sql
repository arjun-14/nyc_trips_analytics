/*
Dataset: bronze
Table: for_hire_vehicle_trips
Description: This table stores raw trip-level data for traditional for-hire vehicles in NYC for the year 2024.

Version      Last Modified             Changes 
1.0          2025-06-27                Initial creation of the table
2.0          2025-07-17                Migrated to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'bronze',
    alias = 'for_hire_vehicle_trips'
) }}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime AS dropoff_datetime,
    PUlocationID AS pulocation_id,
    DOlocationID AS dolocation_id,
    SR_Flag AS sr_flag,
    Affiliated_base_number AS affiliated_base_number 
FROM
    {{ source('bronze', 'ext_for_hire_vehicle_trips') }}
