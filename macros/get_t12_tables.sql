{% macro get_t12_tables() %}
{#
    Auto-discover all t12 tables in rawdatalabeling at build time.
    Excludes tables that have their own dedicated staging models.
    Returns a list of table names like ['t12|1223|junctionatvinings', 't12|0123|eastponcevillage', ...].

    During `dbt parse` (no DB connection), returns an empty list safely.
    During `dbt build/run/compile`, queries the catalog and returns all matching tables.
#}

{% set exclude_tables = [
    't12|022023|preserveatgreison',
    't12|0925|skytop'
] %}

{% if execute %}
    {% set query %}
        SELECT table_name
        FROM workspace.information_schema.tables
        WHERE table_schema = 'rawdatalabeling'
          AND lower(table_name) LIKE 't12|%'
        ORDER BY table_name
    {% endset %}

    {% set results = run_query(query) %}
    {% set t12_tables = [] %}
    {% for row in results %}
        {% set tname = row['table_name'] %}
        {% if tname not in exclude_tables %}
            {% do t12_tables.append(tname) %}
        {% endif %}
    {% endfor %}

    {{ return(t12_tables) }}
{% else %}
    {{ return([]) }}
{% endif %}

{% endmacro %}
