{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT 1 AS rate_code_id, 'Standard rate' AS rate_code_description
UNION ALL
SELECT 2 AS rate_code_id, 'JFK' AS rate_code_description
UNION ALL
SELECT 3 AS rate_code_id, 'Newark' AS rate_code_description
UNION ALL
SELECT 4 AS rate_code_id, 'Nassau/Westchester' AS rate_code_description
UNION ALL
SELECT 5 AS rate_code_id, 'Negotiated fare' AS rate_code_description
UNION ALL
SELECT 6 AS rate_code_id, 'Group ride' AS rate_code_description
