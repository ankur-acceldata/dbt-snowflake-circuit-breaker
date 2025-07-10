{% macro drop_policy_execution_objects() %}
    {% set drop_objects_sql %}
        DROP FUNCTION IF EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_step();
        DROP PROCEDURE IF EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_step();
        DROP TABLE IF EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_state;
    {% endset %}
    
    {% do run_query(drop_objects_sql) %}
{% endmacro %} 