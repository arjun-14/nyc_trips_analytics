/*
-- Script: create_for_hire_vehicle_trips.sql
-- Dataset: bronze
-- Table: for_hire_vehicle_trips
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores raw trip-level data for traditional for-hire vehicles in NYC for the year 2024.
-- Version: 1.0 
-- Last Modified: 2025-06-27
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.bronze.for_hire_vehicle_trips`
(
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  pulocation_id	INT64,
  dolocation_id	INT64,
  sr_flag	INT64,
  affiliated_base_number STRING
);
