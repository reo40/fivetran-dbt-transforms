{{ config(materialized='view') }}

SELECT
  s.CLASS,
  s.NAME,
  cb.BUSHO
FROM {{ ref('step1_test') }} s
LEFT JOIN {{ source('manual', 'CLASS_BUSHO') }} cb
  ON s.CLASS = cb.CLASS
