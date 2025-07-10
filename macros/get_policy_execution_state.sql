{% macro get_policy_execution_state() %}
    {% set query %}
        SELECT *
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state;
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if execute %}
        {% for row in results %}
            {{ log(row, info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 