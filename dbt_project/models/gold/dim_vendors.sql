{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 1 AS vendor_id, 'Creative Mobile Technologies (CMT)' AS vendor_description
UNION ALL
SELECT 2 AS vendor_id, 'VeriFone Inc. (VTS)' AS vendor_description
