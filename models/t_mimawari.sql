{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='DATE_',
    alias='T_みまわり伝書鳩'
) }}

with src as (
  select
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'FORMAT_VERSION',      'CAST(NULL AS NUMBER(38,3))') }} as FORMAT_VERSION,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'ID',                  'CAST(NULL AS NUMBER(18,0))') }} as ID,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'NAME',                'CAST(NULL AS VARCHAR)') }}      as NAME,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'DATA_INTR_TIME',       'CAST(NULL AS NUMBER(18,0))') }} as DATA_INTR_TIME,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'DATE',                'CAST(NULL AS VARCHAR)') }}      as DATE,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'SIGNAL_STRENGTH',      'CAST(NULL AS VARCHAR)') }}      as SIGNAL_STRENGTH,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'POWER_VOLTAGE',        'CAST(NULL AS NUMBER(38,3))') }} as POWER_VOLTAGE,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'TEMP',                 'CAST(NULL AS NUMBER(38,3))') }} as TEMP,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'CUMTEMP',              'CAST(NULL AS NUMBER(38,3))') }} as CUMTEMP,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'HUMI',                 'CAST(NULL AS NUMBER(38,3))') }} as HUMI,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'UV',                   'CAST(NULL AS NUMBER(18,0))') }} as UV,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'UVI',                  'CAST(NULL AS NUMBER(18,0))') }} as UVI,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'ILLUMI',               'CAST(NULL AS NUMBER(38,3))') }} as ILLUMI,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'RAIN',                 'CAST(NULL AS NUMBER(38,3))') }} as RAIN,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'RAIN_1_H',             'CAST(NULL AS NUMBER(38,3))') }} as RAIN_1_H,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'RAIN_24_H',            'CAST(NULL AS NUMBER(38,3))') }} as RAIN_24_H,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'RAIN_BGN',             'CAST(NULL AS NUMBER(38,3))') }} as RAIN_BGN,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'WIND_DIR',             'CAST(NULL AS NUMBER(18,0))') }} as WIND_DIR,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'WIND_DIR_STR',         'CAST(NULL AS VARCHAR)') }}      as WIND_DIR_STR,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'WIND_SPEED',           'CAST(NULL AS NUMBER(38,3))') }} as WIND_SPEED,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'MAX_WIND_SPEED',       'CAST(NULL AS NUMBER(38,3))') }} as MAX_WIND_SPEED,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'WBGT',                 'CAST(NULL AS NUMBER(18,0))') }} as WBGT,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'WBGT_STR',             'CAST(NULL AS VARCHAR)') }}      as WBGT_STR,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'ATMOS',                'CAST(NULL AS NUMBER(18,0))') }} as ATMOS,
    {{ optional_col(source('raw','TMP_MIMAWARI'), 'SEA_LEVEL_PRESSURE',   'CAST(NULL AS VARCHAR)') }}      as SEA_LEVEL_PRESSURE

  from {{ source('raw', 'TMP_MIMAWARI') }}
),

mapped as (
  select
    TRY_TO_NUMERIC(TO_VARCHAR(FORMAT_VERSION), 38, 3)  as FORMAT_VERSION,
    TRY_TO_NUMERIC(TO_VARCHAR(ID), 18, 0)              as ID,
    NAME::varchar                                      as NAME,
    TRY_TO_NUMERIC(TO_VARCHAR(DATA_INTR_TIME), 18, 0)  as DATA_INTR_TIME,
    TRY_TO_TIMESTAMP_TZ(
      regexp_replace(TO_VARCHAR(DATE), '\\s+', ' '),
      'YYYY/MM/DD HH24:MI:SS'
    )                                                  as DATE_,
    SIGNAL_STRENGTH::varchar                           as SIGNAL_STRENGTH,
    TRY_TO_NUMERIC(TO_VARCHAR(POWER_VOLTAGE), 38, 3)   as POWER_VOLTAGE,
    TRY_TO_NUMERIC(TO_VARCHAR(TEMP), 38, 3)            as TEMP,
    TRY_TO_NUMERIC(TO_VARCHAR(CUMTEMP), 38, 3)         as CUMTEMP,
    TRY_TO_NUMERIC(TO_VARCHAR(HUMI), 38, 3)            as HUMI,
    TRY_TO_NUMERIC(TO_VARCHAR(UV), 18, 0)              as UV,
    TRY_TO_NUMERIC(TO_VARCHAR(UVI), 18, 0)             as UVI,
    TRY_TO_NUMERIC(TO_VARCHAR(ILLUMI), 38, 3)          as ILLUMI,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN), 38, 3)            as RAIN,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_1_H), 38, 3)        as RAIN_1H,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_24_H), 38, 3)       as RAIN_24H,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_BGN), 38, 3)        as RAIN_BGN,
    TRY_TO_NUMERIC(TO_VARCHAR(WIND_DIR), 18, 0)        as WIND_DIR,
    WIND_DIR_STR::varchar                              as WIND_DIR_STR,
    TRY_TO_NUMERIC(TO_VARCHAR(WIND_SPEED), 38, 3)      as WIND_SPEED,
    TRY_TO_NUMERIC(TO_VARCHAR(MAX_WIND_SPEED), 38, 3)  as MAX_WIND_SPEED,
    TRY_TO_NUMERIC(TO_VARCHAR(WBGT), 18, 0)            as WBGT,
    WBGT_STR::varchar                                  as WBGT_STR,
    TRY_TO_NUMERIC(TO_VARCHAR(ATMOS), 18, 0)           as ATMOS,
    SEA_LEVEL_PRESSURE::varchar                        as SEA_LEVEL_PRESSURE

  from src
)

select *
from mapped
