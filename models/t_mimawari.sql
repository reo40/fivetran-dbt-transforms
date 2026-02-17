{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ID'
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
    /* 数値：混在に強い（必ず VARCHAR 経由） */
    TRY_TO_NUMERIC(TO_VARCHAR(FORMAT_VERSION), 38, 1) as FORMAT_VERSION,

    /* merge のキーなので型を固定（必要に応じて 18,0 から変更してOK） */
    TRY_TO_NUMERIC(TO_VARCHAR(ID), 18, 0)             as ID,

    NAME::varchar                                      as NAME,

    TRY_TO_NUMERIC(TO_VARCHAR(DATA_INTR_TIME), 18, 0)  as DATA_INTR_TIME,

    /* raw: '2026/1/28  23:57:00' (spaces may vary) */
    to_timestamp_ntz(
      regexp_replace(TO_VARCHAR(DATE), '\\s+', ' '),
      'YYYY/MM/DD HH24:MI'
    )                                                  as DATE_,

    SIGNAL_STRENGTH::varchar                           as SIGNAL_STRENGTH,

    /* 小数1桁で固定（NUMBER(38,1) 相当） */
    TRY_TO_NUMERIC(TO_VARCHAR(POWER_VOLTAGE), 38, 1)   as POWER_VOLTAGE,
    TRY_TO_NUMERIC(TO_VARCHAR(TEMP), 38, 1)            as TEMP,
    TRY_TO_NUMERIC(TO_VARCHAR(CUMTEMP), 38, 1)         as CUMTEMP,
    TRY_TO_NUMERIC(TO_VARCHAR(HUMI), 38, 1)            as HUMI,

    /* uv/uvi は整数想定 */
    TRY_TO_NUMERIC(TO_VARCHAR(UV), 18, 0)              as UV,
    TRY_TO_NUMERIC(TO_VARCHAR(UVI), 18, 0)             as UVI,

    TRY_TO_NUMERIC(TO_VARCHAR(ILLUMI), 38, 1)          as ILLUMI,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN), 38, 1)            as RAIN,

    /* 列名差分：RAIN_1_H -> RAIN_1H / RAIN_24_H -> RAIN_24H */
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_1_H), 38, 1)        as RAIN_1H,
    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_24_H), 38, 1)       as RAIN_24H,

    TRY_TO_NUMERIC(TO_VARCHAR(RAIN_BGN), 38, 1)        as RAIN_BGN,

    TRY_TO_NUMERIC(TO_VARCHAR(WIND_DIR), 18, 0)        as WIND_DIR,
    WIND_DIR_STR::varchar                              as WIND_DIR_STR,

    TRY_TO_NUMERIC(TO_VARCHAR(WIND_SPEED), 38, 1)      as WIND_SPEED,
    TRY_TO_NUMERIC(TO_VARCHAR(MAX_WIND_SPEED), 38, 1)  as MAX_WIND_SPEED,

    /* wbgt/atmos は整数想定（もし小数が来るなら 38,1 に変更） */
    TRY_TO_NUMERIC(TO_VARCHAR(WBGT), 18, 0)            as WBGT,
    WBGT_STR::varchar                                  as WBGT_STR,
    TRY_TO_NUMERIC(TO_VARCHAR(ATMOS), 18, 0)           as ATMOS,

    SEA_LEVEL_PRESSURE::varchar                        as SEA_LEVEL_PRESSURE

  from src
)

select *
from mapped
where DATE_ is not null
  and ID is not null
