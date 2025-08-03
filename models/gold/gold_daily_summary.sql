/*
Dataset: gold
View: daily_summary
Description: Summarizes daily ride-hailing metrics including trip counts, fare amounts, driver pay, platform earnings, 
            distances, and durations—grouped by date, vendor, and trip type for trend and performance analysis.

Version      Last Modified             Changes 
1.0          2025-07-28                Initial creation of the view
*/

{{ config(
    materialized = 'view',
    schema = 'gold',
    alias = 'daily_summary'
) }}

WITH daily_summary AS (
SELECT
    DATE(trips.pickup_datetime) AS pickup_date,
    trips.trip_type,
    vendor.vendor_desc AS vendor_name,
    date_dim.month_name,
    date_dim.day_name,
    date_dim.is_weekend,
    COUNT(*) AS total_trips,
    
    ROUND(AVG(trips.fare_amount), 2) AS avg_fare_amount,
    ROUND(AVG(trips.total_amount), 2) AS avg_total_amount,
    ROUND(AVG(trips.tip_amount), 2) AS avg_trip_amount,
    ROUND(AVG(trips.tip_percentage), 2) AS avg_tip_percentage,
    ROUND(AVG(trips.driver_pay), 2) AS avg_driver_pay,
    ROUND(AVG(trips.platform_earnings), 2) AS avg_platform_earnings,
    ROUND(AVG(trips.platform_share), 2) AS avg_platform_share,

    ROUND(SUM(trips.fare_amount), 2) AS gross_fare_amount,
    ROUND(SUM(trips.total_amount), 2) AS gross_total_amount,
    ROUND(SUM(trips.platform_earnings), 2) AS gross_platform_earnings,
    
    SUM(trips.passenger_count) AS total_passengers,
    ROUND(AVG(trips.trip_distance), 2) AS avg_total_distance,
    ROUND(SUM(trips.trip_distance), 2) AS total_distance,
    ROUND(AVG(trips.trip_duration_seconds), 2) AS avg_trip_duration_seconds,
    SUM(trips.trip_duration_seconds) AS total_trip_duration
    
FROM 
    {{ ref('gold_trips_fact') }} trips
LEFT JOIN {{ ref('gold_vendor_dim') }} vendor 
    ON trips.vendor_id = vendor.vendor_id
LEFT JOIN {{ ref('gold_date_dim') }} date_dim 
    ON trips.pickup_date_key = date_dim.date_key
GROUP BY
    DATE(trips.pickup_datetime),
    trips.trip_type,
    vendor.vendor_desc,
    date_dim.month_name,
    date_dim.day_name,
    date_dim.is_weekend
ORDER BY
    DATE(trips.pickup_datetime),
    trips.trip_type,
    vendor.vendor_desc
)
SELECT
    pickup_date,
    trip_type,
    vendor_name,
    month_name,
    day_name,
    is_weekend,
    total_trips,
    avg_fare_amount,
    avg_total_amount,
    avg_trip_amount,
    avg_tip_percentage,
    avg_driver_pay,
    avg_platform_earnings,
    avg_platform_share,
    gross_fare_amount,
    gross_total_amount,
    gross_platform_earnings,
    total_passengers,
    avg_total_distance,
    total_distance,
    avg_trip_duration_seconds,
    total_trip_duration
FROM
    daily_summary