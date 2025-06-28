/*
----------------------------------------------------------------------------------
Procedure:    proc_refresh_bronze_tables  
Load Type:    Full Load  

Description:
  Loads raw 2024 NYC taxi trip data from Google Cloud Storage into BigQuery bronze
  tables via temporary external tables. Supports Yellow, Green, FHV, and HVFHV data.

Steps:
  - Generate run_id and log START
  - Truncate target bronze tables
  - Create external tables from GCS URIs
  - Insert into native tables
  - Drop external tables
  - Log SUCCESS or FAILURE

Usage:
  CALL `nyc-trips-analytics.bronze.refresh_bronze_tables`();
----------------------------------------------------------------------------------
*/

CREATE OR REPLACE PROCEDURE `nyc-trips-analytics.bronze.proc_refresh_bronze_tables`()
BEGIN
  -- Declaring variables
  DECLARE run_id STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP());

  -- Log start
  INSERT INTO `nyc-trips-analytics.logs.bronze_refresh_logs`
  VALUES (run_id, current_timestamp(), 'STARTED', 'Procedure execution started');

  -- Truncate existing tables
  TRUNCATE TABLE `nyc-trips-analytics.bronze.yellow_taxi_trips`;
  TRUNCATE TABLE `nyc-trips-analytics.bronze.green_taxi_trips`;
  TRUNCATE TABLE `nyc-trips-analytics.bronze.for_hire_vehicle_trips`;
  TRUNCATE TABLE `nyc-trips-analytics.bronze.high_volume_for_hire_vehicle_trips`;

  -- Load Yellow Taxi data
  CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_yellow_taxi_trips`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_trips_records/yellow_taxi_trips_records/*.parquet']
  );

  INSERT INTO `nyc-trips-analytics.bronze.yellow_taxi_trips`
  SELECT 
    VendorID, 
    tpep_pickup_datetime, 
    tpep_dropoff_datetime, 
    passenger_count, 
    trip_distance, 
    RatecodeID, 
    store_and_fwd_flag, 
    PULocationID, 
    DOLocationID, 
    payment_type, 
    fare_amount, 
    extra, 
    mta_tax, 
    tip_amount, 
    tolls_amount, 
    improvement_surcharge, 
    total_amount, 
    congestion_surcharge, 
    Airport_fee 
  FROM `nyc-trips-analytics.bronze.ext_yellow_taxi_trips`;

  -- Load Green Taxi data
  CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_green_taxi_trips`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_trips_records/green_taxi_trips_records/*.parquet']
  );

  INSERT INTO `nyc-trips-analytics.bronze.green_taxi_trips`
  SELECT 
    VendorID, 
    lpep_pickup_datetime, 
    lpep_dropoff_datetime, 
    store_and_fwd_flag, 
    RatecodeID, 
    PULocationID, 
    DOLocationID, 
    passenger_count, 
    trip_distance, 
    fare_amount, 
    extra, 
    mta_tax, 
    tip_amount, 
    tolls_amount, 
    ehail_fee, 
    improvement_surcharge, 
    total_amount,
    payment_type, 
    trip_type, 
    congestion_surcharge
  FROM `nyc-trips-analytics.bronze.ext_green_taxi_trips`;

  -- Load For-Hire Vehicle data
  CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_for_hire_vehicle_trips`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_trips_records/for_hire_vehicle_trips_records/*.parquet']
  );

  INSERT INTO `nyc-trips-analytics.bronze.for_hire_vehicle_trips`
  SELECT 
    dispatching_base_num, 
    pickup_datetime, 
    dropOff_datetime, 
    PUlocationID, 
    DOlocationID, 
    SR_Flag, 
    Affiliated_base_number
  FROM `nyc-trips-analytics.bronze.ext_for_hire_vehicle_trips`;

  -- Load High Volume For-Hire Vehicle data
  CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_high_volume_for_hire_vehicle_trips`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nyc_trips_records/high_volume_for_hire_vehicle_trips_records/*.parquet']
  );

  INSERT INTO `nyc-trips-analytics.bronze.high_volume_for_hire_vehicle_trips`
  SELECT 
    hvfhs_license_num, 
    dispatching_base_num, 
    originating_base_num, 
    request_datetime, 
    on_scene_datetime, 
    pickup_datetime, 
    dropoff_datetime, 
    PULocationID, 
    DOLocationID, 
    trip_miles, 
    trip_time, 
    base_passenger_fare, 
    tolls, 
    bcf, 
    sales_tax, 
    congestion_surcharge, 
    airport_fee, 
    tips, 
    driver_pay, 
    shared_request_flag, 
    shared_match_flag, 
    access_a_ride_flag, 
    wav_request_flag, 
    wav_match_flag 
  FROM `nyc-trips-analytics.bronze.ext_high_volume_for_hire_vehicle_trips`;

  -- Clean up external tables
  DROP EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_yellow_taxi_trips`;
  DROP EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_green_taxi_trips`;
  DROP EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_for_hire_vehicle_trips`;
  DROP EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_high_volume_for_hire_vehicle_trips`;

  -- Log success
  INSERT INTO `nyc-trips-analytics.logs.bronze_refresh_logs`
  VALUES (run_id, current_timestamp(), 'SUCCESS', 'All data loaded successfully');

  EXCEPTION WHEN ERROR THEN
    BEGIN
      -- Getting run_id
      DECLARE run_id STRING;
      SET run_id = (SELECT MAX(run_id) FROM `nyc-trips-analytics.logs.bronze_refresh_logs`);

      -- Log failure
      INSERT INTO `nyc-trips-analytics.logs.bronze_refresh_logs`
      VALUES (run_id, current_timestamp(), 'FAILED', @@error.message);
    END;
END;
