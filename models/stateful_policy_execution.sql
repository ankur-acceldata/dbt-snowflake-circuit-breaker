{{
    config(
        materialized='view',
        tags=['production', 'stateful'],
        pre_hook="CREATE TABLE IF NOT EXISTS " ~ target.database ~ "." ~ target.schema ~ ".policy_execution_state (
            \"execution_id\" VARCHAR,
            \"policy_id\" VARCHAR,
            \"poll_count\" NUMBER DEFAULT 0,
            \"status\" VARCHAR DEFAULT 'pending',
            \"start_time\" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            \"last_poll_time\" TIMESTAMP_NTZ,
            \"completed\" BOOLEAN DEFAULT FALSE,
            \"requires_circuit_breaker\" BOOLEAN DEFAULT FALSE,
            \"circuit_breaker_initiated\" BOOLEAN DEFAULT FALSE,
            \"circuit_breaker_completed\" BOOLEAN DEFAULT FALSE,
            \"error_message\" VARCHAR,
            \"message\" VARCHAR,
            \"api_response\" VARIANT,
            \"policy_data\" VARIANT,
            \"policy_status_response\" VARIANT,
            \"circuit_breaker_response\" VARIANT
        )",
        post_hook="{{ run_policy_workflow() }}"
    )
}}

-- Production Stateful Policy Execution Model
-- This model creates the state table and executes the complete policy workflow
-- 
-- To execute everything in one command:
-- dbt run --models stateful_policy_execution
--
-- This will:
-- 1. Create the state table (pre-hook)
-- 2. Execute the complete policy workflow (post-hook)

-- Minimal select to make the model valid - the real work happens in the post-hook
SELECT 'Policy workflow executed - check policy_execution_state table for results' as status
