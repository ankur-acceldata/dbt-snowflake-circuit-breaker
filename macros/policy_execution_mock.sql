{% macro policy_execution_mock(policy_data={}) %}
    {# Create state table for tracking policy execution #}
    {% set create_state_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_state (
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
        );
    {% endset %}
    {% do run_query(create_state_table_sql) %}

    {# Create the stored procedure for policy execution with MOCK API responses #}
    {% set create_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.execute_policy_workflow_mock(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'execute_policy_workflow_mock'
        PACKAGES = ('snowflake-snowpark-python')
        AS
        $$
import json
from datetime import datetime
import time
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def execute_policy_workflow_mock(session: Session, policy_data):
    """Execute a MOCK policy workflow that simulates API responses."""
    try:
        # Clear any existing state
        try:
            session.sql("DELETE FROM policy_execution_state").collect()
        except:
            pass
        
        # Prepare payload
        if isinstance(policy_data, str):
            payload = json.loads(policy_data)
        else:
            payload = policy_data or {}
            
        # Add timestamp if not present
        if 'timestamp' not in payload:
            payload['timestamp'] = datetime.now().isoformat()
        
        # Step 1: MOCK Create Policy Response
        policy_id = payload.get('expected_policy_id', f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Mock API response
        mock_api_response = {
            "policy_id": policy_id,
            "execution_id": execution_id,
            "status": "created",
            "message": "Policy created successfully (MOCK)",
            "mock": True
        }
        
        # Insert initial state
        api_response_json = json.dumps(mock_api_response).replace("'", "''")
        payload_json = json.dumps(payload).replace("'", "''")
        
        session.sql(f"""
            INSERT INTO policy_execution_state (
                execution_id, policy_id, poll_count, status, last_poll_time, 
                api_response, policy_data, message
            )
            VALUES (
                '{execution_id}', '{policy_id}', 0, 'policy_created', CURRENT_TIMESTAMP(),
                PARSE_JSON('{api_response_json}'),
                PARSE_JSON('{payload_json}'),
                'Policy created successfully (MOCK)'
            )
        """).collect()
        
        # Step 2: MOCK Policy Status Polling (simulate 3 attempts)
        poll_count = 0
        max_polls = 3
        
        for attempt in range(1, max_polls + 1):
            poll_count = attempt
            
            # Mock different responses based on attempt
            if attempt == 1:
                mock_status = "processing"
                mock_message = "Policy is being processed (MOCK)"
            elif attempt == 2:
                mock_status = "processing"
                mock_message = "Policy still processing (MOCK)"
            else:  # attempt == 3
                mock_status = "success"
                mock_message = "Policy is ready for circuit breaker (MOCK)"
            
            mock_policy_response = {
                "policy_id": policy_id,
                "status": mock_status,
                "message": mock_message,
                "attempt": attempt,
                "mock": True
            }
            
            # Update state with poll result
            policy_response_json = json.dumps(mock_policy_response).replace("'", "''")
            message = f'Policy status check attempt {attempt}/3. Status: {mock_status} (MOCK)'.replace("'", "''")
            
            session.sql(f"""
                UPDATE policy_execution_state 
                SET poll_count = {poll_count},
                    last_poll_time = CURRENT_TIMESTAMP(),
                    policy_status_response = PARSE_JSON('{policy_response_json}'),
                    message = '{message}'
                WHERE execution_id = '{execution_id}'
            """).collect()
            
            # Check if policy is ready (on the 3rd attempt)
            if mock_status == "success":
                success_message = f'Policy status check successful after {poll_count} attempts (MOCK)'
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET status = 'policy_ready',
                        requires_circuit_breaker = TRUE,
                        message = '{success_message}'
                    WHERE execution_id = '{execution_id}'
                """).collect()
                break
            
            # Simulate delay between polls (2 seconds)
            time.sleep(2)
        
        # Step 3: MOCK Circuit Breaker Response
        mock_circuit_response = {
            "circuit_breaker_id": f"cb_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "initiated",
            "message": "Circuit breaker initiated successfully (MOCK)",
            "policy_id": policy_id,
            "execution_id": execution_id,
            "mock": True
        }
        
        # Update state as completed
        circuit_response_json = json.dumps(mock_circuit_response).replace("'", "''")
        session.sql(f"""
            UPDATE policy_execution_state 
            SET status = 'completed',
                circuit_breaker_initiated = TRUE,
                circuit_breaker_completed = TRUE,
                circuit_breaker_response = PARSE_JSON('{circuit_response_json}'),
                message = 'Circuit breaker initiated successfully (MOCK)',
                completed = TRUE
            WHERE execution_id = '{execution_id}'
        """).collect()
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "completed",
            "POLL_COUNT": poll_count,
            "COMPLETED": True,
            "CIRCUIT_BREAKER_INITIATED": True,
            "CIRCUIT_BREAKER_COMPLETED": True,
            "MESSAGE": "Circuit breaker initiated successfully (MOCK)",
            "SUCCESS": True,
            "MOCK": True
        }
            
    except Exception as e:
        error_msg = f"Mock policy execution workflow failed: {str(e)}"
        return {
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "COMPLETED": True,
            "SUCCESS": False,
            "MOCK": True
        }
$$
    {% endset %}
    
    {% do run_query(create_policy_procedure_sql) %}
    
    {# Execute the mock stored procedure and return results #}
    {% set execute_sql %}
        CALL {{ target.database }}.{{ target.schema }}.execute_policy_workflow_mock(
            PARSE_JSON('{{ tojson(policy_data) }}')
        )
    {% endset %}
    
    {% do run_query("DELETE FROM " ~ target.database ~ "." ~ target.schema ~ ".policy_execution_state WHERE completed = TRUE AND start_time < DATEADD(hour, -1, CURRENT_TIMESTAMP())") %}
    {% do return(execute_sql) %}
{% endmacro %} 