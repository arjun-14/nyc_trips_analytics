/*
Dataset: silver
Table: high_volume_for_hire_vehicle_trips
Description: This table stores cleansed and enriched trip-level data for high volume for-hire vehicles 
             (e.g., Uber, Lyft) in NYC for the year 2024.
             Includes derived fields such as total amount, fare per mile, trip duration, time-of-day 
             classification, tip percentage and data quality flags for fare amount and trip duration.          

Version      Last Modified             Changes 
1.0          2025-07-04                Initial creation of the table with basic fields and derived columns
1.1          2025-07-14                Update the primary key to include trip type
1.2          2025-07-15                Remove redundant column trip_duration_seconds
2.0          2025-07-18                Migrate to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'silver',
    alias = 'high_volume_for_hire_vehicle_trips'
) }}

WITH high_volume_for_hire_vehicle AS(
SELECT
    TO_HEX(SHA256(CONCAT(
        CAST(request_datetime AS STRING), '_',
        CAST(pickup_datetime AS STRING), '_',
        CAST(dropoff_datetime AS STRING), '_',
        CAST(pulocation_id AS STRING), '_',
        CAST(dolocation_id AS STRING), '_',
        'high volume for hire vehicle'
    ))) AS trip_id,
    hvfhs_license_num,
    dispatching_base_num,
    originating_base_num,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime,
    pulocation_id,
    dolocation_id,
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
    wav_match_flag,

    --derived columns
    base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + airport_fee AS total_amount,
    CASE 
        WHEN trip_miles > 0 THEN ROUND(base_passenger_fare / trip_miles, 2)
        ELSE NULL
    END AS fare_per_mile,
    CASE 
        WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 18 THEN 'Day'
        ELSE 'Night'
    END AS time_of_day_bucket,
    FORMAT_TIMESTAMP('%A', pickup_datetime) AS pickup_day_of_week,
    CASE 
        WHEN base_passenger_fare > 0 THEN ROUND(tips / base_passenger_fare * 100, 2)
        ELSE NULL
    END AS tip_percentage,

    -- data quality flags
    CASE
        WHEN base_passenger_fare < 0 THEN 'INVALID'
        ELSE 'VALID'
    END AS base_passenger_fare_flag,
    CASE
        WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) < 0 THEN 'INVALID'
        ELSE 'VALID'
    END AS trip_duration_seconds_flag,

    -- housekeeping columns
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp

FROM
    {{ ref('bronze_high_volume_for_hire_vehicle_trips') }}
)
SELECT
    trip_id,
    hvfhs_license_num,
    dispatching_base_num,
    originating_base_num,
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime,
    pulocation_id,
    dolocation_id,
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
    wav_match_flag,
    total_amount,
    fare_per_mile,
    time_of_day_bucket,
    pickup_day_of_week,
    tip_percentage,
    base_passenger_fare_flag,
    trip_duration_seconds_flag,
    insert_timestamp,
    update_timestamp
FROM
    high_volume_for_hire_vehicle
QUALIFY ROW_NUMBER() OVER(PARTITION BY trip_id) = 1