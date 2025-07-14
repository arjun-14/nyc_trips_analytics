/*
-- Script: create_silver_green_taxi_trips.sql
-- Dataset: silver
-- Table: green_taxi_trips
-- Source Table: nyc-trips-analytics.bronze.green_taxi_trips
-- Description: This table stores cleansed and enriched trip-level data for green taxis in NYC for the year 2024.
--              Includes derived fields such as trip duration, time-of-day classification, tip percentage,
--              and data quality flags for passenger count, fare amount, and trip duration.
-- Version: 1.0 
-- Last Modified: 2025-07-04
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.silver.green_taxi_trips` AS
WITH green_taxi AS (
SELECT
  TO_HEX(SHA256(CONCAT(
    CAST(vendor_id AS STRING), '_',
    CAST(lpep_pickup_datetime AS STRING), '_',
    CAST(lpep_dropoff_datetime AS STRING), '_',
    CAST(pulocation_id AS STRING), '_',
    CAST(dolocation_id AS STRING), '_',
    CAST(total_amount AS STRING), '_',
    'green taxi'
  ))) AS trip_id,
  vendor_id,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  store_and_fwd_flag,
  ratecode_id,
  pulocation_id,
  dolocation_id,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge,
  
  -- derived fields                              
    TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, SECOND)  AS trip_duration_seconds, 
    CASE 
      WHEN EXTRACT(HOUR FROM lpep_pickup_datetime) BETWEEN 6 AND 18 THEN 'Day'
      ELSE 'Night'
    END AS time_of_day_bucket,
    FORMAT_TIMESTAMP('%A', lpep_pickup_datetime) AS pickup_day_of_week,
    CASE 
      WHEN fare_amount > 0 THEN ROUND(tip_amount / fare_amount * 100, 2)
      ELSE NULL
    END AS tip_percentage,

    -- data quality flags
    CASE
      WHEN passenger_count IS NULL THEN 'MISSING'
      WHEN passenger_count = 0 THEN 'ZERO'
      ELSE 'VALID'
    END AS passenger_count_flag,
    CASE
      WHEN total_amount < 0 THEN 'INVALID'
      ELSE 'VALID'
    END AS total_amount_flag,
    CASE
      WHEN TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, SECOND) < 0 THEN 'INVALID'
      ELSE 'VALID'
    END AS trip_duration_seconds_flag,
    
    -- housekeeping columns
    CURRENT_TIMESTAMP() AS insert_timestamp,
    CURRENT_TIMESTAMP() AS update_timestamp
FROM
  `nyc-trips-analytics.bronze.green_taxi_trips`
)
SELECT
  trip_id,
  vendor_id,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pulocation_id,
  dolocation_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  trip_type,
  congestion_surcharge,
  trip_duration_seconds,
  time_of_day_bucket,
  pickup_day_of_week,
  tip_percentage,
  passenger_count_flag,
  total_amount_flag,
  trip_duration_seconds_flag,
  insert_timestamp,
  update_timestamp
FROM
  green_taxi
QUALIFY ROW_NUMBER() OVER(PARTITION BY trip_id) = 1;