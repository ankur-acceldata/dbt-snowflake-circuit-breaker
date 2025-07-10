{% macro policy_execution() %}
    {# Create state table for tracking policy execution #}
    {% set create_state_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_state (
            execution_id VARCHAR,
            poll_count NUMBER DEFAULT 0,
            status VARCHAR DEFAULT 'pending',
            start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            last_poll_time TIMESTAMP_NTZ,
            completed BOOLEAN DEFAULT FALSE,
            requires_circuit_breaker BOOLEAN DEFAULT FALSE,
            circuit_breaker_initiated BOOLEAN DEFAULT FALSE,
            error_message VARCHAR,
            message VARCHAR
        );
    {% endset %}
    {% do run_query(create_state_table_sql) %}

    {# Initialize state if empty #}
    {% set init_state_sql %}
        INSERT INTO {{ target.database }}.{{ target.schema }}.policy_execution_state (
            execution_id, status, last_poll_time, message
        )
        SELECT 
            'exec_123', 'initiated', CURRENT_TIMESTAMP(), 'Policy execution initiated'
        WHERE NOT EXISTS (
            SELECT 1 FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
        );
    {% endset %}
    {% do run_query(init_state_sql) %}

    {# Execute policy steps #}
    {% set execute_sql %}
        WITH RECURSIVE policy_steps AS (
            -- Base case: Get current state
            SELECT 
                execution_id,
                poll_count,
                status,
                completed,
                requires_circuit_breaker,
                circuit_breaker_initiated,
                message,
                error_message,
                1 as step
            FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
            
            UNION ALL
            
            -- Recursive case: Update state based on previous state
            SELECT
                execution_id,
                CASE 
                    WHEN NOT completed AND poll_count < 3 THEN poll_count + 1
                    ELSE poll_count
                END as poll_count,
                CASE 
                    WHEN NOT completed AND poll_count >= 2 THEN 'completed'
                    WHEN NOT completed THEN 'in_progress'
                    ELSE status
                END as status,
                CASE 
                    WHEN NOT completed AND poll_count >= 2 THEN TRUE
                    ELSE completed
                END as completed,
                CASE 
                    WHEN NOT completed AND poll_count >= 2 THEN TRUE
                    ELSE requires_circuit_breaker
                END as requires_circuit_breaker,
                CASE 
                    WHEN NOT completed AND poll_count >= 2 THEN TRUE
                    ELSE circuit_breaker_initiated
                END as circuit_breaker_initiated,
                CASE 
                    WHEN NOT completed AND poll_count >= 2 THEN 'Circuit breaker initiated successfully'
                    WHEN NOT completed THEN 'Execution in progress'
                    ELSE message
                END as message,
                error_message,
                step + 1
            FROM policy_steps
            WHERE NOT completed
            AND step < 10  -- Safety limit
        )
        -- Get final state
        SELECT 
            execution_id,
            status,
            poll_count as polls_completed,
            requires_circuit_breaker,
            circuit_breaker_initiated,
            message,
            error_message,
            step as total_steps
        FROM policy_steps
        WHERE completed = TRUE
        OR step = 10
        ORDER BY step DESC
        LIMIT 1
    {% endset %}
    
    {% do run_query("TRUNCATE TABLE " ~ target.database ~ "." ~ target.schema ~ ".policy_execution_state") %}
    {% do return(execute_sql) %}
{% endmacro %} 