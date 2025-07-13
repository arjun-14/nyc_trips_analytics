/*
-- Script: create_gold_date_dim.sql
-- Dataset: gold
-- Table: date_dim
-- Description: This dimension table stores calendar date attributes for the year 2024,
--              used to support time-based analyses in the NYC taxi trips data model.
-- Version: 1.0
-- Last Modified: 2025-07-13
*/

CREATE OR REPLACE TABLE `nyc-trips-analytics.gold.date_dim` AS
WITH dates AS (
  SELECT
    CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_key,
    d AS full_date,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(MONTH FROM d) AS month,
    FORMAT_DATE('%B', d) AS month_name,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
    FORMAT_DATE('%A', d) AS day_name,
    EXTRACT(WEEK FROM d) AS week_of_year,
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM d) IN (1, 7) 
      THEN TRUE 
      ELSE FALSE 
    END AS is_weekend
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31', INTERVAL 1 DAY)) AS d
)
SELECT 
  date_key,
  full_date,
  day,
  month,
  month_name,
  year,
  quarter,
  day_of_week,
  day_name,
  week_of_year,
  is_weekend,
  CURRENT_TIMESTAMP() AS insert_timestamp,
  CURRENT_TIMESTAMP() AS update_timestamp
FROM dates;