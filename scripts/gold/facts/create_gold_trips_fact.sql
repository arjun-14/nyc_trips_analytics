/*
-- Script: create_gold_trips_fact.sql
-- Dataset: gold
-- Table: trips_fact
-- Description: This fact table consolidates cleaned and enriched trip-level data across yellow taxis, green taxis, 
                and high-volume for-hire vehicles in NYC for the year 2024. It includes relevant metrics for revenue, 
                trip duration, fare components, and links to corresponding dimension tables.
-- Primary Key: trip_id
-- Related Dimensions: date_dim, location_dim, vendor_dim, payment_dim, ratecode_dim

-- Version      Last Modified             Changes 
-- 1.0          2025-07-15                Initial creation of the table
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.trips_fact` AS
SELECT
  trip_id, 
  'Yellow taxi' AS trip_type,
  CAST(vendor_id AS STRING) AS vendor_id, 
  pulocation_id AS pickup_location_id, 
  dolocation_id AS dropoff_location_id,
  CAST(FORMAT_DATE('%Y%m%d', tpep_pickup_datetime) AS INT64) AS pickup_date_key,
  CAST(FORMAT_DATE('%Y%m%d', tpep_dropoff_datetime) AS INT64) AS dropoff_date_key,
  payment_type AS payment_type_id,
  ratecode_id, 
  NULL AS request_datetime,
  NULL AS on_scene_datetime,
  tpep_pickup_datetime AS pickup_datetime, 
  tpep_dropoff_datetime AS dropoff_datetime,
  passenger_count, 
  trip_distance, 
  trip_duration_seconds,
  time_of_day_bucket,
  fare_amount, 
  extra AS extra_amount,
  NULL AS bcf_amount,
  mta_tax, 
  NULL AS sales_tax,
  tolls_amount, 
  improvement_surcharge, 
  congestion_surcharge, 
  airport_fee, 
  total_amount, 
  tip_amount, 
  tip_percentage,
  NULL AS driver_pay,
  NULL AS fare_per_mile,
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp
FROM
  `nyc-trips-analytics.silver.yellow_taxi_trips`
WHERE 
  passenger_count_flag = 'VALID'
  AND total_amount_flag = 'VALID'
  AND trip_duration_seconds_flag = 'VALID'

UNION ALL
SELECT 
  trip_id, 
  'Green taxi' AS trip_type,
  CAST(vendor_id AS STRING) AS vendor_id,
  pulocation_id AS pickup_location_id, 
  dolocation_id AS dropoff_location_id,
  CAST(FORMAT_DATE('%Y%m%d', lpep_pickup_datetime) AS INT64) AS pickup_date_key,
  CAST(FORMAT_DATE('%Y%m%d', lpep_dropoff_datetime) AS INT64) AS dropoff_date_key, 
  payment_type AS payment_type_id, 
  ratecode_id, 
  NULL AS request_datetime,
  NULL AS on_scene_datetime,
  lpep_pickup_datetime AS pickup_datetime, 
  lpep_dropoff_datetime AS dropoff_datetime, 
  passenger_count, 
  trip_distance, 
  trip_duration_seconds, 
  time_of_day_bucket, 
  fare_amount, 
  extra AS extra_amount,
  NULL AS bcf_amount,
  mta_tax, 
  NULL AS sales_tax,
  tolls_amount, 
  improvement_surcharge, 
  congestion_surcharge, 
  NULL AS airport_fee,
  total_amount, 
  tip_amount, 
  tip_percentage,
  NULL AS driver_pay,
  NULL AS fare_per_mile,
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp
FROM 
  `nyc-trips-analytics.silver.green_taxi_trips`
WHERE 
  passenger_count_flag = 'VALID'
  AND total_amount_flag = 'VALID'
  AND trip_duration_seconds_flag = 'VALID'

UNION ALL
SELECT 
  trip_id,
  'High volume for hire vehicle' AS trip_type,
  hvfhs_license_num AS vendor_id, 
  pulocation_id AS pickup_location_id, 
  dolocation_id AS dropoff_location_id, 
  CAST(FORMAT_DATE('%Y%m%d', pickup_datetime) AS INT64) AS pickup_date_key,
  CAST(FORMAT_DATE('%Y%m%d', dropoff_datetime) AS INT64) AS dropoff_date_key,
  1 AS payment_type_id,
  99 AS ratecode_id,
  request_datetime, 
  on_scene_datetime, 
  pickup_datetime, 
  dropoff_datetime,
  NULL AS passenger_count,
  trip_miles AS trip_distance, 
  trip_time AS trip_duration_seconds, 
  time_of_day_bucket, 
  base_passenger_fare AS fare_amount,
  NULL AS extra_amount,
  bcf AS bcf_amount,
  NULL AS mta_tax, 
  sales_tax,
  tolls AS tolls_amount, 
  NULL AS improvement_surcharge, 
  congestion_surcharge, 
  airport_fee,
  total_amount, 
  tips AS tip_amount, 
  tip_percentage,
  driver_pay,
  fare_per_mile,
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp 
FROM `nyc-trips-analytics.silver.high_volume_for_hire_vehicle_trips`
WHERE
  base_passenger_fare_flag = 'VALID'
  AND trip_duration_seconds_flag = 'VALID'