{% macro optional_col(relation, colname, default_sql='NULL') %}
  {%- set cols = adapter.get_columns_in_relation(relation) -%}
  {%- set colnames = cols | map(attribute='name') | list -%}

  {# 大文字小文字の差を吸収して存在チェック #}
  {%- set colnames_upper = colnames | map('upper') | list -%}

  {%- if colname.upper() in colnames_upper -%}
    {{ adapter.quote(colname) }}
  {%- else -%}
    {{ default_sql }} as {{ adapter.quote(colname) }}
  {%- endif -%}
{% endmacro %}
