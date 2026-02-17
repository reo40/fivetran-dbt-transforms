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
  from {{ source('share_point_raw', 'TMP_MIMAWARI') }}
),

mapped as (
  select
    try_to_number(FORMAT_VERSION, 38, 1)                    as FORMAT_VERSION,
    ID                                                      as ID,
    NAME                                                    as NAME,
    DATA_INTR_TIME                                          as DATA_INTR_TIME,

    /* raw: '2026/1/28  23:57:00' (spaces may vary) */
    to_timestamp_ntz(
      regexp_replace(DATE, '\\s+', ' '),
      'YYYY/M/D HH24:MI:SS'
    )                                                       as DATE_,

    SIGNAL_STRENGTH                                         as SIGNAL_STRENGTH,

    /* raw FLOAT -> target NUMBER(38,1) */
    try_to_number(POWER_VOLTAGE, 38, 1)                     as POWER_VOLTAGE,
    try_to_number(TEMP, 38, 1)                              as TEMP,
    try_to_number(CUMTEMP, 38, 1)                           as CUMTEMP,

    try_to_number(HUMI, 38, 1)                              as HUMI,
    UV                                                      as UV,
    UVI                                                     as UVI,
    try_to_number(ILLUMI, 38, 1)                            as ILLUMI,

    try_to_number(RAIN, 38, 1)                              as RAIN,
    try_to_number(RAIN_1_H, 38, 1)                          as RAIN_1H,
    try_to_number(RAIN_24_H, 38, 1)                         as RAIN_24H,
    try_to_number(RAIN_BGN, 38, 1)                          as RAIN_BGN,

    WIND_DIR                                                as WIND_DIR,
    WIND_DIR_STR                                            as WIND_DIR_STR,
    try_to_number(WIND_SPEED, 38, 1)                        as WIND_SPEED,
    try_to_number(MAX_WIND_SPEED, 38, 1)                    as MAX_WIND_SPEED,

    WBGT                                                    as WBGT,
    WBGT_STR                                                as WBGT_STR,
    ATMOS                                                   as ATMOS,
    SEA_LEVEL_PRESSURE                                      as SEA_LEVEL_PRESSURE

  from src
)

select *
from mapped
where DATE_ is not null   -- targetが NOT NULL のため
;
