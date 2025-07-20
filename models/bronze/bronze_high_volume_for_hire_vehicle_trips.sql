/*
Dataset: bronze
Table: high_volume_for_hire_vehicle_trips
Description: This table stores raw trip-level data for high volume for-hire vehicles (e.g., Uber, Lyft) in NYC for the year 2024.

Version      Last Modified             Changes 
1.0          2025-06-27                Initial creation of the table
2.0          2025-07-17                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'bronze',
    alias = 'high_volume_for_hire_vehicle_trips'
) }}

SELECT
    hvfhs_license_num,
    dispatching_base_num,
    originating_base_num,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime,
    PULocationID AS pulocation_id,
    DOLocationID AS dolocation_id,
    trip_miles,
    trip_time,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tips,
    driver_pay,
    shared_request_flag,
    shared_match_flag,
    access_a_ride_flag,
    wav_request_flag,
    wav_match_flag
FROM
    {{ source('bronze', 'ext_high_volume_for_hire_vehicle_trips') }}
