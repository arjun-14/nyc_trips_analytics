/*
-- Script: create_gold_trip_type_dim.sql
-- Dataset: gold
-- Table: trip_type_dim
-- Description: This dimension table stores a list of trip types (e.g., Yellow Taxi, Green Taxi, FHV, HVFHV) 
    to categorize trips in the NYC taxi dataset.
-- Version: 1.0
-- Last Modified: 2025-07-13
*/

-- Creating the trip_type_dim table
CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.trip_type_dim`
(
  trip_type_id INT64,
  trip_type_desc STRING,   
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- -- Populating the table with known trip types
INSERT INTO `nyc-trips-analytics.gold.trip_type_dim` (trip_type_id, trip_type_desc)
VALUES
  (1, 'Yellow taxi'),
  (2, 'Green taxi'),
  (3, 'For hire vehicle'),
  (4, 'High volume for hire vehicle');
