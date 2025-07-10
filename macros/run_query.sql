{% macro execute_query(query) %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% for row in results %}
            {{ log(row, info=True) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 