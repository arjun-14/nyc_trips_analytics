/*
Dataset: bronze
Table: yellow_taxi_trips
Description: This table stores raw trip-level data for yellow taxis in NYC for the year 2024.

Version      Last Modified             Changes 
1.0          2025-06-27                Initial creation of the table
2.0          2025-07-17                Migrated to dbt
*/

{{ config(
    materialized = 'table',
    schema = 'bronze',
    alias = 'yellow_taxi_trips'
) }}

SELECT
    VendorID AS vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID AS ratecode_id,
    store_and_fwd_flag,
    PULocationID AS pulocation_id,
    DOLocationID AS dolocation_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee AS airport_fee
FROM
    {{ source('bronze', 'ext_yellow_taxi_trips') }}
