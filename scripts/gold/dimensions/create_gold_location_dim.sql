/*
-- Script: create_gold_location_dim.sql
-- Dataset: gold
-- Table: location_dim
-- Source Table: nyc-trips-analytics.silver.taxi_zone_lookup
-- Description: This dimension table provides reference data mapping Location IDs to NYC boroughs, 
                zones, and service zones. It is used to enrich trip data with geographic context in the gold layer.
-- Version: 1.0 
-- Last Modified: 2025-07-13
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.location_dim` AS
SELECT
  location_id,
  borough,
  zone,
  service_zone
FROM
  `nyc-trips-analytics.silver.taxi_zone_lookup`;