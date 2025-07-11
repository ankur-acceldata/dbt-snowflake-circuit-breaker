{% macro run_policy_workflow() %}
    {{ log("Starting Policy Execution Workflow...", info=True) }}
    
    {# Sample policy data for testing #}
    {% set policy_data = {
        "policy_name": "sample_policy",
        "policy_type": "data_governance", 
        "description": "Sample policy for data governance with circuit breaker",
        "priority": "high",
        "created_by": "dbt_user",
        "expected_policy_id": "744df8ed-a494-4b2e-ae19-f96ae7bfff45",
        "parameters": {
            "retention_days": 90,
            "classification": "sensitive", 
            "compliance_level": "strict",
            "circuit_breaker_enabled": true
        }
    } %}
    
    {{ log("Policy data: " ~ tojson(policy_data), info=True) }}
    
    {# Execute the policy workflow directly #}
    {{ log("Executing the policy workflow...", info=True) }}
    {% set workflow_result = policy_execution(policy_data) %}
    
    {{ log("Workflow result: " ~ workflow_result, info=True) }}
    
    {# Show recent executions count #}
    {% set count_sql %}
        SELECT COUNT(*) as total_executions,
               SUM(CASE WHEN "status" = 'completed' THEN 1 ELSE 0 END) as completed_executions,
               SUM(CASE WHEN "status" = 'error' THEN 1 ELSE 0 END) as failed_executions
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
        WHERE "start_time" >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    {% endset %}
    
    {% if count_sql %}
        {% set count_results = run_query(count_sql) %}
        {% if count_results and count_results.rows and count_results.rows|length > 0 %}
            {% set row = count_results.rows[0] %}
            {{ log("Executions in last hour - Total: " ~ row[0] ~ ", Completed: " ~ row[1] ~ ", Failed: " ~ row[2], info=True) }}
        {% endif %}
    {% endif %}
    
    {{ log("Check the policy_execution_state table for detailed results", info=True) }}
    
{% endmacro %} 