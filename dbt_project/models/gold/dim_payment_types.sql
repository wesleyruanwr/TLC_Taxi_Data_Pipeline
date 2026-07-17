{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    payment_type,
    payment_description,
    is_valid_payment
FROM {{ source('silver', 'payment_types') }}
