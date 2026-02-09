{{ config(materialized='view') }}

SELECT
  p.PID,
  p.CLASS,
  p.NAME,
  cb.BUSHO
FROM {{ ref('TEST') }} p
LEFT JOIN {{ ref('CLASS_BUSHO') }} cb
  ON p.CLASS = cb.CLASS
