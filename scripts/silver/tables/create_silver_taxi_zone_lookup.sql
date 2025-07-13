/*
-- Script: create_silver_taxi_zone_lookup.sql
-- Dataset: silver
-- Table: taxi_zone_lookup
-- Source Table: nyc-trips-analytics.bronze.taxi_zone_lookup
-- Description: This table stores reference data mapping taxi zone Location IDs to boroughs, zones, and service zones in NYC.
-- Version: 1.0 
-- Last Modified: 2025-07-13
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.silver.taxi_zone_lookup` AS
SELECT
  location_id,
  borough,
  zone,
  service_zone,
  
  -- housekeeping columns
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp
FROM
  `nyc-trips-analytics.bronze.taxi_zone_lookup`;