/*
-- Script: create_ratecode_dim.sql
-- Dataset: gold
-- Table: ratecode_dim
-- Description: This dimension table provides descriptive mappings for rate codes used in NYC taxi trips, 
                enabling better interpretation and analysis of fare rate types.
-- Version: 1.0
-- Last Modified: 2025-07-13
*/

-- Creating the ratecode_dim table
CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.ratecode_dim`
(
  ratecode_id INT64,
  ratecode_desc STRING,   
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Populating the table with known ratecodes
INSERT INTO `nyc-trips-analytics.gold.ratecode_dim` (ratecode_id, ratecode_desc)
VALUES
  (1, 'Standard rate'),
  (2, 'JFK'),
  (3, 'Newark'),
  (4, 'Nassau or Westchester'),
  (5, 'Negotiated fare'),
  (6, 'Group ride'),
  (99, 'Null/unknown');