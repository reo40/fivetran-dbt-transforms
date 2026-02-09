{{ config(materialized='view') }}

SELECT
  CLASS,
  NAME
FROM {{ source('raw', 'TEST') }}
