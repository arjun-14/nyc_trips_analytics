/*
-- Script: create_bronze_yellow_taxi_trips.sql
-- Dataset: bronze
-- Table: yellow_taxi_trips
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores raw trip-level data for yellow taxis in NYC for the year 2024.
-- Version: 1.0 
-- Last Modified: 2025-06-27
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.bronze.yellow_taxi_trips`
(
  vendor_id	INT64,
  tpep_pickup_datetime	TIMESTAMP,
  tpep_dropoff_datetime	TIMESTAMP,
  passenger_count	INT64,
  trip_distance	FLOAT64,
  ratecode_id	INT64,
  store_and_fwd_flag	STRING,
  pulocation_id	INT64,
  dolocation_id	INT64,
  payment_type	INT64,
  fare_amount	FLOAT64,
  extra	FLOAT64,
  mta_tax	FLOAT64,
  tip_amount	FLOAT64,
  tolls_amount	FLOAT64,
  improvement_surcharge	FLOAT64,
  total_amount	FLOAT64,
  congestion_surcharge	FLOAT64,
  airport_fee	FLOAT64
);
