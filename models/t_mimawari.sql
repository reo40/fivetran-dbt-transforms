{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='DATE_',
    alias='T_みまわり伝書鳩'
) }}

with src as (
  select
    FORMAT_VERSION,
    ID,
    NAME,
    DATA_INTR_TIME,
    DATE,
    SIGNAL_STRENGTH,
    POWER_VOLTAGE,
    TEMP,
    CUMTEMP,
    HUMI,
    UV,
    UVI,
    ILLUMI,
    RAIN,
    RAIN_1_H,
    RAIN_24_H,
    RAIN_BGN,
    WIND_DIR,
    WIND_DIR_STR,
    WIND_SPEED,
    MAX_WIND_SPEED,
    WBGT,
    WBGT_STR,
    ATMOS,
    SEA_LEVEL_PRESSURE
  from {{ source('raw', 'TMP_MIMAWARI') }}
),

mapped as (
  select
    TRY_TO_NUMERIC(TO_VARCHAR(FORMAT_VERSION), 38, 3) as FORMAT_VERSION,
    TRY_TO_NUMERIC(TO_VARCHAR(ID), 18, 0)             as ID,
    NAME::varchar                                      as NAME,
    TRY_TO_NUMERIC(TO_VARCHAR(DATA_INTR_TIME), 18, 0)  as DATA_INTR_TIME,

    /* 日付の型変更 */
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

    /* 列名変更：RAIN_1_H -> RAIN_1H / RAIN_24_H -> RAIN_24H */
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
