{{ config(materialized='view') }}

SELECT
  p.PID,
  p.CLASS,
  p.NAME,
  cb.BUSHO
FROM {{ source('raw', 'TEST') }} p
LEFT JOIN {{ source('manual', 'CLASS_BUSHO') }} cb
  ON p.CLASS = cb.CLASS
