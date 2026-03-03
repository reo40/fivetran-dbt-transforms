{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['gauge_id','measurement_id'],
    alias='T_アナログメータ'
) }}

with src as (
  select
    {{ optional_col(source('raw','TMP_ANALOG'), 'GAUGE_ID',        'CAST(NULL AS VARCHAR)') }} as GAUGE_ID,
    {{ optional_col(source('raw','TMP_ANALOG'), 'GAUGE_NAME',      'CAST(NULL AS VARCHAR)') }} as GAUGE_NAME,
    {{ optional_col(source('raw','TMP_ANALOG'), 'MEASUREMENT_ID',  'CAST(NULL AS VARCHAR)') }} as MEASUREMENT_ID,
    {{ optional_col(source('raw','TMP_ANALOG'), 'VALUE',           'CAST(NULL AS NUMBER(38,3))') }} as VALUE,
    {{ optional_col(source('raw','TMP_ANALOG'), 'PREDICTED_VALUE', 'CAST(NULL AS NUMBER(38,3))') }} as PREDICTED_VALUE,
    {{ optional_col(source('raw','TMP_ANALOG'), 'CORRECTED_VALUE', 'CAST(NULL AS VARCHAR)') }} as CORRECTED_VALUE,
    {{ optional_col(source('raw','TMP_ANALOG'), 'IS_CONFIRMED',    'CAST(NULL AS VARCHAR)') }} as IS_CONFIRMED,
    {{ optional_col(source('raw','TMP_ANALOG'), 'DATETIME',        'CAST(NULL AS VARCHAR)') }} as DATETIME,
    {{ optional_col(source('raw','TMP_ANALOG'), 'UNIT',            'CAST(NULL AS VARCHAR)') }} as UNIT
  from {{ source('raw', 'TMP_ANALOG') }}
),

mapped as (
  select
    GAUGE_ID::varchar                                  as gauge_id,
    GAUGE_NAME::varchar                                as gauge_name,
    MEASUREMENT_ID::varchar                            as measurement_id,
    TRY_TO_NUMERIC(TO_VARCHAR(VALUE), 38, 3)           as value,
    TRY_TO_NUMERIC(TO_VARCHAR(PREDICTED_VALUE), 38, 3) as predicted_value,
    CORRECTED_VALUE::varchar                           as corrected_value,
    TO_VARCHAR(IS_CONFIRMED)                           as is_confirmed,
    TO_VARCHAR(
      TRY_TO_TIMESTAMP_NTZ(DATETIME),
      'YYYY-MM-DD"T"HH24:MI:SS'
    )                                                  as "取得時刻",
    UNIT::varchar                                      as unit
  from src
)

select *
from mapped
where DATETIME is not null
