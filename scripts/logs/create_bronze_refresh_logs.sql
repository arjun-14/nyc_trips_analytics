/*
-- Script: create_bronze_refresh_logs.sql
-- Dataset: logs
-- Table: bronze_refresh_logs
-- Description: Stores execution logs for the bronze layer refresh procedure. 
--              Includes status updates and error messages with timestamps.
-- Version: 1.0 
-- Last Modified: 2025-06-28
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.logs.bronze_refresh_logs`
(
  run_id STRING,
  log_timestamp TIMESTAMP,
  status STRING,
  message STRING
);
