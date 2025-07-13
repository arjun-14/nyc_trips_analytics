/*
-- Script: create_silver_taxi_zone_lookup.sql
-- Dataset: silver
-- Table: taxi_zone_lookup
-- Source URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
-- Description: This table stores reference data mapping taxi zone Location IDs to boroughs, zones, and service zones in NYC.
-- Version: 1.0 
-- Last Modified: 2025-07-13
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.silver.taxi_zone_lookup` AS
SELECT
  location_id,
  borough,
  zone,
  service_zone
FROM
  `nyc-trips-analytics.bronze.taxi_zone_lookup`;