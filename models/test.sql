{{ config(materialized='view') }}

select
  try_cast(format_version as number) as format_version,
  try_cast(id as number(18,0))       as id,
  try_cast(name as varchar)          as name,
  try_cast(data_intr_time as number(18,0)) as data_intr_time,
  try_cast(date_ as timestamp)       as date_,
  try_cast(signal_strength as varchar) as signal_strength,
  try_cast(power_voltage as number)  as power_voltage,
  try_cast(temp as number)           as temp,
  try_cast(cumtemp as number)        as cumtemp,
  try_cast(humi as number)           as humi,
  try_cast(uv as number(18,0))       as uv,
  try_cast(uvi as number(18,0))      as uvi,
  try_cast(illumi as number)         as illumi,
  try_cast(rain as number)           as rain,
  try_cast(rain_1h as number)        as rain_1h,
  try_cast(rain_24h as number)       as rain_24h,
  try_cast(rain_bgn as number)       as rain_bgn,
  try_cast(wind_dir as number(18,0)) as wind_dir,
  try_cast(wind_dir_str as varchar)  as wind_dir_str,
  try_cast(wind_speed as number)     as wind_speed,
  try_cast(max_wind_speed as number) as max_wind_speed,
  try_cast(wbgt as number(18,0))     as wbgt,
  try_cast(wbgt_str as varchar)      as wbgt_str,
  try_cast(atmos as number(18,0))    as atmos,
  try_cast(sea_level_pressure as varchar) as sea_level_pressure
from {{ source('raw', 'YOUR_RAW_TABLE_NAME') }}
