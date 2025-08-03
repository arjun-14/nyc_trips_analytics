/*
Script:      bronze_external_tables.sql
Purpose:     Creates external tables in the Bronze layer for 2024 NYC taxi datasets from Parquet/CSV files stored in Google Cloud Storage.

Version      Last Modified             Changes 
1.0          2025-08-03                Initial creation of external tables
*/

CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_yellow_taxi_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_trips_records/yellow_taxi_trips_records/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_green_taxi_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_trips_records/green_taxi_trips_records/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_for_hire_vehicle_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_trips_records/for_hire_vehicle_trips_records/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_high_volume_for_hire_vehicle_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc_trips_records/high_volume_for_hire_vehicle_trips_records/*.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `nyc-trips-analytics.bronze.ext_taxi_zone_lookup`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc_trips_records/taxi_zone_lookup/taxi_zone_lookup.csv']
);