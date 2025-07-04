/*
-- Script: create_taxi_zone_lookup.sql
-- Dataset: bronze
-- Table: taxi_zone_lookup
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores reference data mapping taxi zone Location IDs to boroughs, zones, and service zones in NYC.
-- Version: 1.0 
-- Last Modified: 2025-07-03
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.bronze.taxi_zone_lookup`
(
  location_id INT64,
  borough STRING,
  zone STRING,
  service_zone STRING
);
