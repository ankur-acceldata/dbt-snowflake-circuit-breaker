-- Policy Execution Model - Shows the results of policy execution workflow
-- 
-- To execute the workflow, run:
-- dbt run-operation run_policy_workflow
--
-- This model creates the state table structure and shows dummy data until workflow is executed

{{ config(
    materialized='table',
    pre_hook="CREATE TABLE IF NOT EXISTS " ~ target.database ~ "." ~ target.schema ~ ".policy_execution_state (
        execution_id VARCHAR,
        policy_id VARCHAR,
        poll_count NUMBER DEFAULT 0,
        status VARCHAR DEFAULT 'pending',
        start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        last_poll_time TIMESTAMP_NTZ,
        completed BOOLEAN DEFAULT FALSE,
        requires_circuit_breaker BOOLEAN DEFAULT FALSE,
        circuit_breaker_initiated BOOLEAN DEFAULT FALSE,
        circuit_breaker_completed BOOLEAN DEFAULT FALSE,
        error_message VARCHAR,
        message VARCHAR,
        api_response VARIANT,
        policy_data VARIANT,
        policy_status_response VARIANT,
        circuit_breaker_response VARIANT
    )"
) }}

-- Show dummy data until real execution is run
SELECT 
    'no_execution' as execution_id,
    'no_policy' as policy_id,
    'not_started' as status,
    0 as poll_count,
    FALSE as circuit_breaker_initiated,
    FALSE as circuit_breaker_completed,
    CURRENT_TIMESTAMP() as start_time,
    CURRENT_TIMESTAMP() as last_poll_time,
    FALSE as completed,
    'No execution has been run yet. Use: dbt run-operation run_policy_workflow' as message,
    null as error_message,
    null as api_response,
    null as policy_status_response,
    null as circuit_breaker_response 