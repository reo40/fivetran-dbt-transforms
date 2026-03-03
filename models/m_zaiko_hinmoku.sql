{{ config(
    materialized='incremental',
    incremental_strategy='append',
    alias='M_在庫_品目マスタ'
) }}

with src as (
  select
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HINMOKU', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'FC_HINBAN', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'KANJI_SHINAMEI', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HINMOKU_TEXT', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HINMOKU_TYPE', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'UNITE_KUBUN', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'FF_KUBUN', 'CAST(NULL AS VARCHAR)') }},

    {{ optional_col(source('raw','TMP_HINMOKU'), 'SHISAKU_KUBUN', 'CAST(NULL AS NUMBER(18,0))') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'YOURYOU', 'CAST(NULL AS NUMBER(18,2))') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'YOURYOU_TANI', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'PLANT', 'CAST(NULL AS NUMBER(18,0))') }},

    {{ optional_col(source('raw','TMP_HINMOKU'), 'DAIHYO_SETSUBI', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'DAIHYO_KOUJO', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'WAKO_CODE', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'TSUKA', 'CAST(NULL AS NUMBER(18,5))') }},

    {{ optional_col(source('raw','TMP_HINMOKU'), 'SAP_HINBAN', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HINMOKU_PLANT', 'CAST(NULL AS VARCHAR)') }},

    {{ optional_col(source('raw','TMP_HINMOKU'), 'HYOUJUN_GENKA', 'CAST(NULL AS NUMBER(18,2))') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HYOUJUN_GENKA_PER_KG', 'CAST(NULL AS NUMBER(18,5))') }},

    {{ optional_col(source('raw','TMP_HINMOKU'), 'SOUGENKA', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'HENDOUHI', 'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), 'KOTEIHI', 'CAST(NULL AS NUMBER(18,0))') }},
    {{ optional_col(source('raw','TMP_HINMOKU'), '_FILE', 'CAST(NULL AS VARCHAR)') }},

    -- ★ 追加：同期時刻
    {{ optional_col(source('raw','TMP_HINMOKU'), '_FIVETRAN_SYNCED', 'CAST(NULL AS TIMESTAMP_TZ)') }} as _FIVETRAN_SYNCED

  from {{ source('raw','TMP_HINMOKU') }}

  {% if is_incremental() %}
    where _FIVETRAN_SYNCED >
      (select coalesce(max(_FIVETRAN_SYNCED), '1900-01-01'::timestamp_tz) from {{ this }})
  {% endif %}
),

mapped as (
  select
    HINMOKU::varchar                                          as "品目",
    FC_HINBAN::varchar                                        as "FC品番",
    KANJI_SHINAMEI::varchar                                   as "漢字　品名　表示用1",
    HINMOKU_TEXT::varchar                                     as "品目テキスト",
    HINMOKU_TYPE::varchar                                     as "品目タイプ",
    UNITE_KUBUN::varchar                                      as "UNITE用事業区分",
    FF_KUBUN::varchar                                         as "FF詳細事業区分",

    TRY_TO_DECIMAL(TO_VARCHAR(SHISAKU_KUBUN), 18, 0)           as "施策区分",
    TRY_TO_DECIMAL(TO_VARCHAR(PLANT), 18, 0)                   as "プラント",
    TRY_TO_DECIMAL(TO_VARCHAR(KOTEIHI), 18, 0)                 as "固定費",

    TRY_TO_DECIMAL(TO_VARCHAR(YOURYOU), 18, 2)                 as "容量",
    TRY_TO_DECIMAL(TO_VARCHAR(HYOUJUN_GENKA), 18, 2)           as "標準原価",

    YOURYOU_TANI::varchar                                     as "容量単位",
    DAIHYO_SETSUBI::varchar                                    as "代表製造設備",
    DAIHYO_KOUJO::varchar                                      as "代表製造工場",
    WAKO_CODE::varchar                                         as "和光コード",

    TRY_TO_DECIMAL(TO_VARCHAR(TSUKA), 18, 5)                   as "通貨",
    TRY_TO_DECIMAL(TO_VARCHAR(HYOUJUN_GENKA_PER_KG), 18, 5)    as "標準原価kgあたり",

    SAP_HINBAN::varchar                                       as "SAP品番",
    HINMOKU_PLANT::varchar                                     as "品目+プラント",

    SOUGENKA::varchar                                         as "総原価",
    HENDOUHI::varchar                                         as "変動費",

    TO_DATE(REGEXP_SUBSTR(_FILE, '[0-9]{8}'), 'YYYYMMDD')      as "日付",

    -- ★ 追加：差分用ウォーターマーク（列として保持しておくのがおすすめ）
    _FIVETRAN_SYNCED                                          as "_FIVETRAN_SYNCED"

  from src
)

select * from mapped
