{{ config(
    materialized='materialized_view',
    schema='gold'
) }}

SELECT
    f.pickup_year_month as month_yyyymm,
    f.vendor_id,
    -- total de corridas do mes
    COUNT(1) as total_rides,
    -- receita valida (is_valid_payment = true)
    SUM(CASE WHEN p.is_valid_payment = true THEN f.total_amount ELSE 0.0 END) as total_valid_amount,
    -- media do valor apenas das corridas com pagamento valido
    COALESCE(AVG(CASE WHEN p.is_valid_payment = true THEN f.total_amount ELSE NULL END), 0.0) as avg_ticket_amount,
    COALESCE(AVG(f.trip_distance), 0.0) as avg_distance
FROM {{ ref('fct_trips') }} f
LEFT JOIN {{ ref('dim_payment_types') }} p
    ON f.payment_type = p.payment_type
GROUP BY f.pickup_year_month, f.vendor_id
