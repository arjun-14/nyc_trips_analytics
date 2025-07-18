/*
Dataset: bronze
Table: green_taxi_trips
Description: This table stores raw trip-level data for green taxis in NYC for the year 2024.

Version      Last Modified             Changes 
1.0          2025-06-27                Initial creation of the table
2.0          2025-07-17                Migrated to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'bronze',
    alias = 'green_taxi_trips'
) }}

SELECT
    VendorID AS vendor_id,
    lpep_pickup_datetime ,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID AS ratecode_id,
    PULocationID AS pulocation_id,
    DOLocationID AS dolocation_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax ,
    tip_amount ,
    tolls_amount ,
    ehail_fee,
    improvement_surcharge,
    total_amount ,
    payment_type ,
    trip_type,
    congestion_surcharge 
FROM
    {{ source('bronze', 'ext_green_taxi_trips') }}