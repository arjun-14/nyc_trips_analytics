/*
-- Script: create_bronze_refresh_logs.sql
-- Dataset: logs
-- Table: bronze_refresh_logs
-- Description: Stores execution logs for the bronze layer refresh procedure. 
--              Includes status updates and error messages with timestamps.
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.logs.bronze_refresh_logs`
(
  run_id STRING,
  log_timestamp TIMESTAMP,
  status STRING,
  message STRING
);
