{{
    config(
        materialized='table',
        tags=['api_test']
    )
}}

-- Test the createpolicy API call with sample data
{{ create_policy_api({
    "policy_name": "test_policy",
    "policy_type": "access_control",
    "description": "Test policy for API integration",
    "priority": "medium",
    "created_by": "dbt_test",
    "test_mode": true,
    "parameters": {
        "max_users": 100,
        "access_level": "read_only",
        "expiry_days": 30
    }
}) }} 