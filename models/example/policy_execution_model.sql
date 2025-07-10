{{
    config(
        materialized='table',
        tags=['policy_execution']
    )
}}

-- Execute policy execution and show results
{{ policy_execution() }} 