{{ config(materialized='view') }}

select
  /* format_version NUMERIC (Snowflakeだと実質 NUMBER(38,0) なので整数でOK想定) */
  TRY_TO_DECIMAL(TO_VARCHAR(format_version), 38, 0) as format_version,

  /* id NUMERIC(18,0) */
  TRY_TO_DECIMAL(TO_VARCHAR(id), 18, 0) as id,

  name::varchar as name,

  /* data_intr_time NUMERIC(18,0) */
  TRY_TO_DECIMAL(TO_VARCHAR(data_intr_time), 18, 0) as data_intr_time,

  /* date_ TIMESTAMP NOT NULL UNIQUE */
  TRY_TO_TIMESTAMP(TO_VARCHAR(date)) as date_,

  signal_strength::varchar as signal_strength,

  /* power_voltage NUMERIC（小数の可能性があるなら 2桁、なければ 0桁でもOK） */
  TRY_TO_DECIMAL(TO_VARCHAR(power_voltage), 18, 2) as power_voltage,

  /* 小数あり得る系：2桁保持（必要なら 3 に変更） */
  TRY_TO_DECIMAL(TO_VARCHAR(temp), 18, 2) as temp,
  TRY_TO_DECIMAL(TO_VARCHAR(cumtemp), 18, 2) as cumtemp,
  TRY_TO_DECIMAL(TO_VARCHAR(humi), 18, 2) as humi,

  /* uv/uvi NUMERIC(18,0) */
  TRY_TO_DECIMAL(TO_VARCHAR(uv), 18, 0) as uv,
  TRY_TO_DECIMAL(TO_VARCHAR(uvi), 18, 0) as uvi,

  TRY_TO_DECIMAL(TO_VARCHAR(illumi), 18, 2) as illumi,
  TRY_TO_DECIMAL(TO_VARCHAR(rain), 18, 2) as rain,
  TRY_TO_DECIMAL(TO_VARCHAR(rain_1_h), 18, 2) as rain_1h,
  TRY_TO_DECIMAL(TO_VARCHAR(rain_24_h), 18, 2) as rain_24h,
  TRY_TO_DECIMAL(TO_VARCHAR(rain_bgn), 18, 2) as rain_bgn,

  /* wind_dir NUMERIC(18,0) */
  TRY_TO_DECIMAL(TO_VARCHAR(wind_dir), 18, 0) as wind_dir,
  wind_dir_str::varchar as wind_dir_str,

  TRY_TO_DECIMAL(TO_VARCHAR(wind_speed), 18, 2) as wind_speed,
  TRY_TO_DECIMAL(TO_VARCHAR(max_wind_speed), 18, 2) as max_wind_speed,

  /* wbgt/atmos NUMERIC(18,0) */
  TRY_TO_DECIMAL(TO_VARCHAR(wbgt), 18, 0) as wbgt,
  wbgt_str::varchar as wbgt_str,
  TRY_TO_DECIMAL(TO_VARCHAR(atmos), 18, 0) as atmos,

  sea_level_pressure::varchar as sea_level_pressure

from {{ source('raw', 'MIMAWARI') }}
