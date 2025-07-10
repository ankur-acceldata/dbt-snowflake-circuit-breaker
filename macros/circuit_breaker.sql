{% macro circuit_breaker_python() %}
    {# First create the state table to track our polling #}
    {% set create_state_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.circuit_breaker_state (
            todo_id NUMBER,
            poll_count NUMBER DEFAULT 0,
            status VARCHAR DEFAULT 'pending',
            start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            last_poll_time TIMESTAMP_NTZ,
            completed BOOLEAN DEFAULT FALSE
        );
    {% endset %}
    {% do run_query(create_state_table_sql) %}

    {# Create the UDF for a single operation step #}
    {% set create_udf_sql %}
        CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.circuit_breaker_step()
        RETURNS OBJECT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'circuit_breaker_step'
        AS
        $$
import json
from datetime import datetime
from typing import Dict, Any

def log_info(message: str) -> None:
    """Log info message to Snowflake."""
    print(f"INFO [{datetime.now().isoformat()}]: {message}")

def log_error(message: str) -> None:
    """Log error message to Snowflake."""
    print(f"ERROR [{datetime.now().isoformat()}]: {message}")

def simulate_api_call(operation: str, todo_id: int = None) -> Dict[str, Any]:
    """Simulate API calls with fixed responses."""
    log_info(f"Making API call: {operation} " + (f"for todo_id: {todo_id}" if todo_id else ""))
    
    if operation == 'create':
        result = {
            'id': 1,
            'title': 'foo',
            'body': 'bar',
            'userId': 1
        }
        log_info(f"Created todo with response: {result}")
        return result
    elif operation == 'get':
        result = {
            'id': todo_id,
            'title': 'foo',
            'body': 'bar',
            'userId': 1,
            'status': 'pending'
        }
        log_info(f"Got todo details: {result}")
        return result
    elif operation == 'delete':
        log_info(f"Deleted todo {todo_id}")
        return True
    return None

def circuit_breaker_step():
    """Execute a single step of the circuit breaker operation."""
    try:
        # Get current state
        state_result = _session.sql("""
            SELECT todo_id, poll_count, status, start_time, last_poll_time, completed
            FROM circuit_breaker_state 
            LIMIT 1
        """).collect()
        
        if not state_result:  # No state yet, this is our first run
            log_info("Starting new circuit breaker operation")
            todo = simulate_api_call('create')
            todo_id = todo['id']
            
            _session.sql(f"""
                INSERT INTO circuit_breaker_state (todo_id, poll_count, last_poll_time)
                VALUES ({todo_id}, 0, CURRENT_TIMESTAMP())
            """).collect()
            
            return {
                "TODO_ID": todo_id,
                "STATUS": "created",
                "POLL_COUNT": 0,
                "COMPLETED": False
            }
        
        # Get existing state
        state = state_result[0]
        todo_id = state['TODO_ID']
        poll_count = state['POLL_COUNT']
        completed = state['COMPLETED']
        
        if completed:
            return {
                "TODO_ID": todo_id,
                "STATUS": "completed",
                "POLL_COUNT": poll_count,
                "COMPLETED": True
            }
        
        if poll_count < 5:  # Still need to poll
            log_info(f"Executing poll {poll_count + 1} of 5")
            todo_details = simulate_api_call('get', todo_id)
            
            _session.sql(f"""
                UPDATE circuit_breaker_state 
                SET poll_count = poll_count + 1,
                    last_poll_time = CURRENT_TIMESTAMP()
                WHERE todo_id = {todo_id}
            """).collect()
            
            return {
                "TODO_ID": todo_id,
                "STATUS": "polling",
                "POLL_COUNT": poll_count + 1,
                "COMPLETED": False
            }
        else:  # All polls complete, time to delete
            log_info("All polls complete, deleting todo")
            success = simulate_api_call('delete', todo_id)
            
            if success:
                _session.sql(f"""
                    UPDATE circuit_breaker_state 
                    SET status = 'completed',
                        completed = TRUE
                    WHERE todo_id = {todo_id}
                """).collect()
            
            return {
                "TODO_ID": todo_id,
                "STATUS": "completed" if success else "error",
                "POLL_COUNT": poll_count,
                "COMPLETED": True,
                "DELETION_SUCCESS": success
            }
            
    except Exception as e:
        error_msg = str(e)
        log_error(f"Circuit breaker step failed: {error_msg}")
        return {
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "COMPLETED": True
        }
$$
    {% endset %}
    
    {% do run_query(create_udf_sql) %}
    
    {# Execute the circuit breaker steps until completion #}
    {% set execute_sql %}
        WITH RECURSIVE circuit_breaker AS (
            SELECT 
                circuit_breaker_step() as result,
                1 as step
            
            UNION ALL
            
            SELECT 
                circuit_breaker_step() as result,
                step + 1
            FROM circuit_breaker
            WHERE NOT result:COMPLETED
            AND step < 10  -- Safety limit
        )
        SELECT 
            result:TODO_ID::NUMBER as todo_id,
            result:STATUS::VARCHAR as status,
            result:POLL_COUNT::NUMBER as polls_completed,
            result:DELETION_SUCCESS::BOOLEAN as deletion_success,
            result:ERROR_MESSAGE::VARCHAR as error_message,
            step as total_steps
        FROM circuit_breaker
        WHERE result:COMPLETED
        ORDER BY step DESC
        LIMIT 1
    {% endset %}
    
    {% do run_query("TRUNCATE TABLE " ~ target.database ~ "." ~ target.schema ~ ".circuit_breaker_state") %}
    {% do return(execute_sql) %}
{% endmacro %} 