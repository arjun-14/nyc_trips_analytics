/*
-- Script: create_gold_vendor_dim.sql
-- Dataset: gold
-- Table: vendor_dim
-- Description: This dimension table stores metadata for vendor identifiers used across Yellow Taxi, Green Taxi, 
                and High Volume For-Hire Vehicle services in NYC. It provides descriptions for each vendor ID and is 
                used to enrich fact tables for reporting and analytics.
-- Version: 1.0
-- Last Modified: 2025-07-13
*/

-- Creating the vendor_dim table
CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.vendor_dim`
(
  vendor_id STRING,
  vendor_desc STRING,   
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Populating the table with known vendors
INSERT INTO `nyc-trips-analytics.gold.vendor_dim` (vendor_id, vendor_desc)
VALUES
  ('1', 'Creative Mobile Technologies, LLC'),
  ('2', 'Curb Mobility, LLC'),
  ('6', 'Myle Technologies Inc'),
  ('7', 'Helix'),
  ('HV0002', 'Juno'),
  ('HV0003', 'Uber'),
  ('HV0004', 'Via'),
  ('HV0005', 'Lyft');
