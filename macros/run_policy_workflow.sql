{% macro run_policy_workflow() %}
    {{ log("Starting Policy Execution Workflow...", info=True) }}
    
    {# Execute the policy workflow with sample data #}
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
    
    {# Execute the policy workflow #}
    {% set workflow_sql = policy_execution(policy_data) %}
    {% do run_query(workflow_sql) %}
    
    {{ log("Policy workflow execution completed!", info=True) }}
    {{ log("Check the policy_execution_state table for results", info=True) }}
    
    {# Show simple results count #}
    {% set count_sql %}
        SELECT COUNT(*) as total_executions
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
    {% endset %}
    
    {% set count_results = run_query(count_sql) %}
    
    {% if count_results %}
        {% for row in count_results %}
            {{ log("Total executions in state table: " ~ row[0], info=True) }}
        {% endfor %}
    {% endif %}
    
    {{ log("To see detailed results, query the policy_execution_state table directly", info=True) }}
    
{% endmacro %} 