/*
-- Script: create_green_taxi_trips.sql
-- Dataset: bronze
-- Table: green_taxi_trips
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores raw trip-level data for green taxis in NYC for the year 2024.
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.bronze.green_taxi_trips`
(
  vendor_id	INT64,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime	TIMESTAMP,
  store_and_fwd_flag STRING,
  ratecode_id	INT64,
  pulocation_id	INT64,
  dolocation_id	INT64,
  passenger_count	INT64,
  trip_distance	FLOAT64,
  fare_amount	FLOAT64,
  extra	FLOAT64,
  mta_tax FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  ehail_fee	FLOAT64,
  improvement_surcharge	FLOAT64,
  total_amount FLOAT64,
  payment_type INT64,
  trip_type	INT64,
  congestion_surcharge FLOAT64
);
