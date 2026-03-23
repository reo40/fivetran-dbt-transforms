{{ config(
    materialized='table',
    alias='T_在庫_MES'
) }}

with src as (
  select
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HINMEI_CODE',        'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HINMEI',             'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'LOT_NUM',            'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'PALLETE_NUM',        'CAST(NULL AS NUMBER(18,0))') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'YOUKI_SHUBETSU',     'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'MAKER_LOT',          'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HOKAN_PLACE',        'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'IREME',              'CAST(NULL AS FLOAT)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'KOSU',               'CAST(NULL AS NUMBER(18,0))') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'ZAIKORYOU',          'CAST(NULL AS FLOAT)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HIKIATEKANOURYOU',   'CAST(NULL AS FLOAT)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'SHUKKO_YOUKYU',      'CAST(NULL AS NUMBER(18,0))') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'TANI',               'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HAN_NUM',            'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'HINSHITSU_STATE',    'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'KAIFU_STATE',        'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'YUKOUKIGEN',         'CAST(NULL AS TIMESTAMP_NTZ)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'LOCK_STATE',         'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'LOCK_JISSHISHA',     'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'LOCK_DATE',          'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'GMP',                'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'TORIATSUKAI',        'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'KIKENBUTSU',         'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'KENKYUSHISAKU',      'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'DOKUBUTSU',          'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), 'GEKIBUTSU',          'CAST(NULL AS VARCHAR)') }},
    {{ optional_col(source('raw','TMP_ZAIKO_MES'), '_FILE',              'CAST(NULL AS VARCHAR)') }}
  from {{ source('raw','TMP_ZAIKO_MES') }}
),

mapped as (
  select
    HINMEI_CODE::varchar                                    as "品名コード",
    HINMEI::varchar                                         as "品名",
    LOT_NUM::varchar                                        as "ロット番号",
    TRY_TO_DECIMAL(TO_VARCHAR(PALLETE_NUM), 18, 0)          as "パレット番号",
    YOUKI_SHUBETSU::varchar                                 as "容器種別",
    MAKER_LOT::varchar                                      as "メーカロット",
    HOKAN_PLACE::varchar                                    as "保管場所",
    TRY_TO_DECIMAL(TO_VARCHAR(IREME), 38, 3)                as "入れ目",
    TRY_TO_DECIMAL(TO_VARCHAR(KOSU), 18, 0)                 as "個数",
    TRY_TO_DECIMAL(TO_VARCHAR(ZAIKORYOU), 38, 3)            as "在庫量",
    TRY_TO_DECIMAL(TO_VARCHAR(HIKIATEKANOURYOU), 38, 3)     as "引当可能量",
    TRY_TO_DECIMAL(TO_VARCHAR(SHUKKO_YOUKYU), 18, 0)        as "出庫要求量",
    TANI::varchar                                           as "単位",
    HAN_NUM::varchar                                        as "版番号",
    HINSHITSU_STATE::varchar                                as "品質状態",
    KAIFU_STATE::varchar                                    as "開封状態",
    TRY_TO_DATE(YUKOUKIGEN)                                 as "有効期限",
    LOCK_STATE::varchar                                     as "ロック状態",
    LOCK_JISSHISHA::varchar                                 as "ロック実施者",
    LOCK_DATE::varchar                                      as "ロック実施日時",
    GMP::varchar                                            as "GMP",
    TORIATSUKAI::varchar                                    as "取扱",
    KIKENBUTSU::varchar                                     as "危険物",
    KENKYUSHISAKU::varchar                                  as "研究試作",
    DOKUBUTSU::varchar                                      as "毒物",
    GEKIBUTSU::varchar                                      as "劇物",
    TO_DATE(REGEXP_SUBSTR(_FILE, '[0-9]{8}'), 'YYYYMMDD')   as "日付"
  from src
)

select *
from mapped
