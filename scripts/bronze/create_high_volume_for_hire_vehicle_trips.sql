/*
-- Script: create_high_volume_for_hire_vehicle_trips.sql
-- Dataset: bronze
-- Table: high_volume_for_hire_vehicle_trips
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores raw trip-level data for high volume for-hire vehicles (e.g., Uber, Lyft) in NYC for the year 2024.
-- Version: 1.0 
-- Last Modified: 2025-06-27
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.bronze.high_volume_for_hire_vehicle_trips`
(
  hvfhs_license_num	STRING,
  dispatching_base_num	STRING,
  originating_base_num	STRING,
  request_datetime	TIMESTAMP,
  on_scene_datetime	TIMESTAMP,
  pickup_datetime	TIMESTAMP,
  dropoff_datetime	TIMESTAMP,
  pulocation_id	INT64,
  dolocation_id	INT64,
  trip_miles	FLOAT64,
  trip_time	INT64,
  base_passenger_fare	FLOAT64,
  tolls	FLOAT64,
  bcf	FLOAT64,
  sales_tax	FLOAT64,
  congestion_surcharge	FLOAT64,
  airport_fee	FLOAT64,
  tips	FLOAT64,
  driver_pay	FLOAT64,
  shared_request_flag	STRING,
  shared_match_flag	STRING,
  access_a_ride_flag	STRING,
  wav_request_flag	STRING,
  wav_match_flag	STRING
);
