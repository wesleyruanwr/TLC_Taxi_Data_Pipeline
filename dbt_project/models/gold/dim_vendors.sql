{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 1 AS vendor_id, 'Creative Mobile Technologies, LLC (CMT)' AS vendor_description
UNION ALL
SELECT 2 AS vendor_id, 'Curb Mobility, LLC (ex-VeriFone/VTS)' AS vendor_description
UNION ALL
SELECT 6 AS vendor_id, 'Myle Technologies Inc' AS vendor_description
UNION ALL
SELECT 7 AS vendor_id, 'Helix' AS vendor_description
