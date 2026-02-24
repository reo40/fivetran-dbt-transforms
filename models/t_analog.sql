{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['gauge_id','measurement_id']
) }}

with src as (
  select
    GAUGE_ID,
    GAUGE_NAME,
    MEASUREMENT_ID,
    VALUE,
    PREDICTED_VALUE,
    CORRECTED_VALUE,
    IS_CONFIRMED,
    DATETIME,
    UNIT
  from {{ source('raw', 'TMP_ANALOG') }}
),

mapped as (
  select
    GAUGE_ID::varchar                                  as gauge_id,
    GAUGE_NAME::varchar                                as gauge_name,
    MEASUREMENT_ID::varchar                            as measurement_id,

    /* 小数1桁で固定（NUMBER(38,1) 相当） */
    TRY_TO_NUMERIC(TO_VARCHAR(VALUE), 38, 1)           as VALUE,
    TRY_TO_NUMERIC(TO_VARCHAR(PREDICTED_VALUE), 38, 1) as PREDICTED_VALUE,

    /* corrected_value は VARCHAR 指定 */
    CORRECTED_VALUE::varchar                           as corrected_value,

    /* boolean → 文字列 */
    TO_VARCHAR(IS_CONFIRMED)                           as is_confirmed,

    /* 元CSV形式に戻す */
    TO_VARCHAR(DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS')    as "取得時刻",

    UNIT::varchar                                      as unit

  from src
)

select *
from mapped
