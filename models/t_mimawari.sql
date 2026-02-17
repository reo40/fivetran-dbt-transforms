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
    /* number系（混在にも強い形に） */
    cast(round(try_to_double(FORMAT_VERSION), 1) as number(38,1)) as FORMAT_VERSION,

    ID                                                     as ID,
    NAME                                                   as NAME,
    DATA_INTR_TIME                                         as DATA_INTR_TIME,

    /* raw: '2026/1/28  23:57:00' (spaces may vary) */
    to_timestamp_ntz(
      regexp_replace(DATE, '\\s+', ' '),
      'YYYY/M/D HH24:MI:SS'
    )                                                      as DATE_,

    SIGNAL_STRENGTH                                        as SIGNAL_STRENGTH,

    /* raw FLOAT -> target NUMBER(38,1)
       TRY_CAST(FLOAT AS NUMBER(38,1)) がSnowflakeで落ちるケースがあるので
       round + cast に統一（NULLも自然に通る）
    */
    cast(round(try_to_double(POWER_VOLTAGE), 1) as number(38,1)) as POWER_VOLTAGE,
    cast(round(try_to_double(TEMP), 1)          as number(38,1)) as TEMP,
    cast(round(try_to_double(CUMTEMP), 1)       as number(38,1)) as CUMTEMP,

    /* rawはNUMBER(38,1)だが、混在しても落ちにくいよう統一 */
    cast(round(try_to_double(HUMI), 1) as number(38,1))         as HUMI,

    UV                                                     as UV,
    UVI                                                    as UVI,

    cast(round(try_to_double(ILLUMI), 1) as number(38,1))       as ILLUMI,
    cast(round(try_to_double(RAIN), 1) as number(38,1))         as RAIN,

    /* 列名差分：RAIN_1_H -> RAIN_1H / RAIN_24_H -> RAIN_24H */
    cast(round(try_to_double(RAIN_1_H), 1) as number(38,1))     as RAIN_1H,
    cast(round(try_to_double(RAIN_24_H), 1) as number(38,1))    as RAIN_24H,

    cast(round(try_to_double(RAIN_BGN), 1) as number(38,1))     as RAIN_BGN,

    WIND_DIR                                               as WIND_DIR,
    WIND_DIR_STR                                           as WIND_DIR_STR,

    cast(round(try_to_double(WIND_SPEED), 1) as number(38,1))   as WIND_SPEED,
    cast(round(try_to_double(MAX_WIND_SPEED), 1) as number(38,1)) as MAX_WIND_SPEED,

    WBGT                                                   as WBGT,
    WBGT_STR                                               as WBGT_STR,
    ATMOS                                                  as ATMOS,
    SEA_LEVEL_PRESSURE                                     as SEA_LEVEL_PRESSURE

  from src
)

select *
from mapped
where DATE_ is not null
