{{ config(materialized='view') }}

{#
  Stateless Circuit Breaker Flow
  - Executes all 3 steps without storing state in policy_execution_state table
  - Returns results directly
  - Steps: create policy -> poll status -> trigger circuit breaker
#}

{{ log("=== Starting Stateless Circuit Breaker Flow ===", info=True) }}

{# Sample policy data for testing #}
{% set policy_data = {
    "policy_name": "stateless_policy",
    "policy_type": "data_governance", 
    "description": "Stateless policy execution without state persistence",
    "priority": "high",
    "created_by": "dbt_stateless_user",
    "expected_policy_id": "stateless-744df8ed-a494-4b2e-ae19-f96ae7bfff45",
    "parameters": {
        "retention_days": 90,
        "classification": "sensitive", 
        "compliance_level": "strict",
        "circuit_breaker_enabled": true,
        "stateless_mode": true
    }
} %}

{{ log("Policy data for stateless flow: " ~ tojson(policy_data), info=True) }}

{# Execute the stateless circuit breaker workflow #}
{% set workflow_result = stateless_circuit_breaker_execution(policy_data) %}

{{ log("Stateless workflow result: " ~ workflow_result, info=True) }}

SELECT 
    '{{ workflow_result }}' as workflow_status,
    'Stateless circuit breaker execution completed - no state persisted' as message,
    CURRENT_TIMESTAMP() as execution_timestamp 