{{ config(
    materialized='materialized_view',
    schema='gold'
) }}

SELECT
    pickup_year_month as month_yyyymm,
    vendor_id,
    COUNT(1) as total_rides,
    SUM(CASE WHEN is_valid_trip = true THEN total_amount ELSE 0.0 END) as total_valid_amount,
    COALESCE(AVG(CASE WHEN is_valid_trip = true THEN total_amount ELSE NULL END), 0.0) as avg_ticket_amount,
    COALESCE(AVG(trip_distance), 0.0) as avg_distance
FROM {{ ref('fct_trips') }}
GROUP BY pickup_year_month, vendor_id
