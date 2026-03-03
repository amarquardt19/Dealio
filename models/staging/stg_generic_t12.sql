-- Generic T12 staging: auto-discovers and standardizes ALL t12 tables
-- in rawdatalabeling. No manual source list needed — new tables are
-- picked up automatically at build time via get_t12_tables() macro.
-- Dynamically handles ANY number of columns per table:
--   STRUCTURED  → from ingest_t12_docx notebook (has row_type column)
--   RAW         → auto-detects account_code, account_name, months, total

{# Auto-discover t12 tables. Falls back to known list during parse (IDE). #}
{% set discovered = get_t12_tables() %}
{% set generic_sources = discovered if discovered | length > 0 else var('generic_t12_sources', [
    't12|1223|junctionatvinings',
    't12|0123|eastponcevillage'
]) %}

with

{% for table_name in generic_sources %}
    {% set property_slug = table_name.split('|')[2] if table_name.split('|') | length >= 3 else table_name %}

    {# Detect table schema #}
    {% set tbl = adapter.get_relation(database='workspace', schema='rawdatalabeling', identifier=table_name) %}
    {% if tbl is not none %}
        {% set cols = adapter.get_columns_in_relation(tbl) %}
        {% set num_cols = cols | length %}
        {% set col_names = cols | map(attribute='name') | list %}
        {% set first_col = col_names[0] %}
        {% set is_structured = (first_col == 'row_type') %}
    {% else %}
        {% set cols = [] %}
        {% set num_cols = 0 %}
        {% set col_names = [] %}
        {% set first_col = '_c0' %}
        {% set is_structured = false %}
    {% endif %}

{% if is_structured %}
-- Structured source (from ingest_t12_docx notebook): {{ table_name }}
_staged_{{ loop.index }} as (
    select
        '{{ property_slug }}' as property_name,
        account_code,
        trim(account_name) as account_name,
        cast(month_01 as string) as month_01,
        cast(month_02 as string) as month_02,
        cast(month_03 as string) as month_03,
        cast(month_04 as string) as month_04,
        cast(month_05 as string) as month_05,
        cast(month_06 as string) as month_06,
        cast(month_07 as string) as month_07,
        cast(month_08 as string) as month_08,
        cast(month_09 as string) as month_09,
        cast(month_10 as string) as month_10,
        cast(month_11 as string) as month_11,
        cast(month_12 as string) as month_12,
        cast(total as string) as total,
        row_idx as row_num
    from workspace.rawdatalabeling.`{{ table_name }}`
),

{% else %}
-- Raw source ({{ num_cols }} cols): {{ table_name }}
{#
    Dynamic column mapping:
    - 15+ cols: col[0]=account_code, col[1]=account_name, col[2:-1]=months, col[-1]=total
    - <15 cols: col[0]=account_name, col[1:-1]=months, col[-1]=total
    Missing months (if fewer than 12 data cols) are null-filled.
#}
    {% set has_account_code = (num_cols >= 15) %}
    {% if has_account_code %}
        {% set data_cols = col_names[2:] %}
    {% else %}
        {% set data_cols = col_names[1:] %}
    {% endif %}
    {% set total_col = data_cols[-1] if data_cols | length > 0 else none %}
    {% set month_data_cols = data_cols[:-1] if data_cols | length > 1 else [] %}

_raw_{{ loop.index }} as (
    select *, monotonically_increasing_id() as _row_id
    from workspace.rawdatalabeling.`{{ table_name }}`
),

_staged_{{ loop.index }} as (
    select
        '{{ property_slug }}' as property_name,
        {% if has_account_code %}
        `{{ first_col }}` as account_code,
        trim(`{{ col_names[1] }}`) as account_name,
        {% else %}
        null as account_code,
        trim(`{{ first_col }}`) as account_name,
        {% endif %}
        {% for m in range(1, 13) %}
        {% if m - 1 < month_data_cols | length %}
        `{{ month_data_cols[m - 1] }}` as month_{{ '%02d' % m }},
        {% else %}
        null as month_{{ '%02d' % m }},
        {% endif %}
        {% endfor %}
        {% if total_col %}
        `{{ total_col }}` as total,
        {% else %}
        null as total,
        {% endif %}
        row_number() over (order by _row_id) as row_num
    from _raw_{{ loop.index }}
),

{% endif %}
{% endfor %}

final as (
    {% for table_name in generic_sources %}
    select * from _staged_{{ loop.index }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select * from final
