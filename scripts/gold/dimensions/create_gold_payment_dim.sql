/*
-- Script: create_gold_payment_dim.sql
-- Dataset: gold
-- Table: payment_dim
-- Description: This dimension table maps numeric payment type codes to human-readable descriptions for NYC taxi trips.
-- Version: 1.0
-- Last Modified: 2025-07-13
*/

-- Creating the payment_dim table
CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.payment_dim`
(
  payment_type_id INT64,                
  payment_type_desc STRING,             
  insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), 
  update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Populating the table with known payment types
INSERT INTO `nyc-trips-analytics.gold.payment_dim` (payment_type_id, payment_type_desc) 
VALUES
  (0, 'Flex Fare trip'),
  (1, 'Credit card'),
  (2, 'Cash'),
  (3, 'No charge'),
  (4, 'Dispute'),
  (5, 'Unknown'),
  (6, 'Voided trip');