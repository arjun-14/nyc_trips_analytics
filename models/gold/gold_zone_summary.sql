/*
Dataset: gold
View: zone_summary
Description: Summarizes total trips and revenue between pickup and dropoff zones, enabling borough and zone-level trip pattern analysis

Version      Last Modified             Changes 
1.0          2025-08-01                Initial creation of the view
*/

{{ config(
    materialized = 'view',
    schema = 'gold',
    alias = 'zone_summary'
) }}

WITH zone_summary AS(
SELECT
    pickup_location.borough pickup_borough,
    pickup_location.zone pickup_zone,
    dropoff_location.borough dropoff_borough,
    dropoff_location.zone dropoff_zone,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM
    {{ ref('gold_trips_fact') }} trips
LEFT JOIN {{ ref('gold_location_dim') }} pickup_location
    ON trips.pickup_location_id = pickup_location.location_id
LEFT JOIN {{ ref('gold_location_dim') }} dropoff_location
    ON trips.dropoff_location_id = dropoff_location.location_id
WHERE
    pickup_location.borough NOT IN ('Unknown', 'N/A')
GROUP BY
    pickup_location.borough,
    pickup_location.zone,
    dropoff_location.borough,
    dropoff_location.zone
)
SELECT
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    total_trips,
    total_revenue
FROM
    zone_summary