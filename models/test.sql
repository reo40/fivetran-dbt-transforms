{{ config(materialized='view') }}

select
  try_to_number(format_version) as format_version,
  try_to_number(id) as id,
  name::varchar as name,
  try_to_number(data_intr_time) as data_intr_time,
  try_to_timestamp(date) as date_,
  signal_strength::varchar as signal_strength,
  try_to_number(power_voltage) as power_voltage,
  try_to_number(temp) as temp,
  try_to_number(cumtemp) as cumtemp,
  try_to_number(humi) as humi,
  try_to_number(uv) as uv,
  try_to_number(uvi) as uvi,
  try_to_number(illumi) as illumi,
  try_to_number(rain) as rain,
  try_to_number(rain_1_h) as rain_1h,
  try_to_number(rain_24_h) as rain_24h,
  try_to_number(rain_bgn) as rain_bgn,
  try_to_number(wind_dir) as wind_dir,
  wind_dir_str::varchar as wind_dir_str,
  try_to_number(wind_speed) as wind_speed,
  try_to_number(max_wind_speed) as max_wind_speed,
  try_to_number(wbgt) as wbgt,
  wbgt_str::varchar as wbgt_str,
  try_to_number(atmos) as atmos,
  sea_level_pressure::varchar as sea_level_pressure
from {{ source('raw', 'MIMAWARI') }}
