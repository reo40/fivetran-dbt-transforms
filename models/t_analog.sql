{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['gauge_id','measurement_id']
) }}

with src as (
  select
    {{ optional_col(source('raw','TMP_ANALOG'), 'GAUGE_ID') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'GAUGE_NAME') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'MEASUREMENT_ID') }},

    -- 数値列は “型付きNULL” を default にするとブレが減ります
    {{ optional_col(source('raw','TMP_ANALOG'), 'VALUE', 'CAST(NULL AS NUMBER(38,1))') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'PREDICTED_VALUE', 'CAST(NULL AS NUMBER(38,1))') }},

    {{ optional_col(source('raw','TMP_ANALOG'), 'CORRECTED_VALUE') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'IS_CONFIRMED') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'DATETIME') }},
    {{ optional_col(source('raw','TMP_ANALOG'), 'UNIT') }}

  from {{ source('raw', 'TMP_ANALOG') }}
),

mapped as (
  select
    GAUGE_ID::varchar                                  as gauge_id,
    GAUGE_NAME::varchar                                as gauge_name,
    MEASUREMENT_ID::varchar                            as measurement_id,

    /* 小数1桁で固定（NUMBER(38,1) 相当） */
    TRY_TO_NUMERIC(TO_VARCHAR(VALUE), 38, 1)           as value,
    TRY_TO_NUMERIC(TO_VARCHAR(PREDICTED_VALUE), 38, 1) as predicted_value,

    CORRECTED_VALUE::varchar                           as corrected_value,

    TO_VARCHAR(IS_CONFIRMED)                           as is_confirmed,

    TO_VARCHAR(DATETIME, 'YYYY-MM-DD"T"HH24:MI:SS')    as "取得時刻",

    UNIT::varchar                                      as unit
  from src
)

select * from mapped
