/*
-- Script: create_silver_for_hire_vehicle_trips.sql
-- Dataset: silver
-- Table: for_hire_vehicle_trips
-- Source Table: nyc-trips-analytics.bronze.for_hire_vehicle_trips
-- Description: This table stores cleansed and enriched trip-level data for for-hire vehicles in NYC for the year 2024.
--              Includes derived fields such as trip duration, time-of-day classification
--              and data quality flags for trip duration.
-- Version: 1.0 
-- Last Modified: 2025-07-05
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.silver.for_hire_vehicle_trips` AS
WITH for_hire_vehicle AS (
SELECT
  TO_HEX(SHA256(CONCAT(
      CAST(dispatching_base_num AS STRING), '_',
      CAST(pickup_datetime AS STRING), '_',
      CAST(dropoff_datetime AS STRING), '_',
      'for hire vehicle'
    ))) AS trip_id,
  dispatching_base_num,
  pickup_datetime,
  dropoff_datetime,
  pulocation_id,
  dolocation_id,
  sr_flag,
  affiliated_base_number,

  --derived fields
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND)  AS trip_duration_seconds, 
  CASE 
    WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 6 AND 18 THEN 'Day'
    ELSE 'Night'
  END AS time_of_day_bucket,
  FORMAT_TIMESTAMP('%A', pickup_datetime) AS pickup_day_of_week,
  
  -- data quality flags
  CASE
    WHEN TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) < 0 THEN 'INVALID'
    ELSE 'VALID'
  END AS trip_duration_seconds_flag,
  
  -- housekeeping columns
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp
    
FROM
  `nyc-trips-analytics.bronze.for_hire_vehicle_trips`
) 
SELECT
  trip_id,
  dispatching_base_num,
  pickup_datetime,
  dropoff_datetime,
  pulocation_id,
  dolocation_id,
  sr_flag,
  affiliated_base_number,
  trip_duration_seconds,
  time_of_day_bucket,
  pickup_day_of_week,
  trip_duration_seconds_flag,
  insert_timestamp,
  update_timestamp
FROM
  for_hire_vehicle
QUALIFY ROW_NUMBER() OVER(PARTITION BY trip_id) = 1