{% macro policy_execution(policy_data={}) %}
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

    {# Create the stored procedure for policy execution with real API calls #}
    {% set create_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.execute_policy_workflow(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'execute_policy_workflow'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        AS
        $$
import requests
import json
from datetime import datetime
import time
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def execute_policy_workflow(session: Session, policy_data):
    """Execute the complete policy workflow: create -> poll -> circuit breaker."""
    try:
        # Clear any existing state
        try:
            session.sql("DELETE FROM policy_execution_state").collect()
        except:
            # Table might not exist, continue
            pass
        
        # Prepare payload
        if isinstance(policy_data, str):
            payload = json.loads(policy_data)
        else:
            payload = policy_data or {}
            
        # Add timestamp if not present
        if 'timestamp' not in payload:
            payload['timestamp'] = datetime.now().isoformat()
        
        # Step 1: Create Policy
        try:
            create_url = "https://dbttest.free.beeceptor.com/createpolicy"
            
            response = requests.post(
                create_url,
                json=payload,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                timeout=30
            )
            
            response.raise_for_status()
            api_response = response.json() if response.content else {}
            
            # Extract policy ID and execution ID from response
            policy_id = api_response.get('policy_id', api_response.get('id', f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
            execution_id = api_response.get('execution_id', f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            
            # Insert initial state
            api_response_json = json.dumps(api_response).replace("'", "''")
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
                    'Policy created successfully'
                )
            """).collect()
            
        except requests.exceptions.RequestException as e:
            error_msg = str(e).replace("'", "''")
            execution_id = f"exec_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            session.sql(f"""
                INSERT INTO policy_execution_state (
                    execution_id, status, error_message, last_poll_time, completed
                )
                VALUES (
                    '{execution_id}', 'error', '{error_msg}', CURRENT_TIMESTAMP(), TRUE
                )
            """).collect()
            
            return {
                "EXECUTION_ID": execution_id,
                "STATUS": "error",
                "ERROR_MESSAGE": error_msg,
                "COMPLETED": True,
                "SUCCESS": False
            }
        except Exception as e:
            error_msg = f"Policy creation setup failed: {str(e)}".replace("'", "''")
            execution_id = f"exec_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            return {
                "EXECUTION_ID": execution_id,
                "STATUS": "error",
                "ERROR_MESSAGE": error_msg,
                "COMPLETED": True,
                "SUCCESS": False
            }
        
        # Step 2: Poll Policy Status (up to 3 attempts)
        policy_ready = False
        poll_count = 0
        max_polls = 3
        
        while poll_count < max_polls and not policy_ready:
            poll_count += 1
            
            try:
                policy_status_url = f"https://dbttest.free.beeceptor.com/policy/{policy_id}"
                
                response = requests.get(
                    policy_status_url,
                    headers={'Accept': 'application/json'},
                    timeout=30
                )
                
                response.raise_for_status()
                policy_status_response = response.json() if response.content else {}
                
                policy_status = policy_status_response.get('status', 'unknown')
                
                # Update state with poll result
                policy_response_json = json.dumps(policy_status_response).replace("'", "''")
                message = f'Policy status check attempt {poll_count}/3. Status: {policy_status}'.replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET poll_count = {poll_count},
                        last_poll_time = CURRENT_TIMESTAMP(),
                        policy_status_response = PARSE_JSON('{policy_response_json}'),
                        message = '{message}'
                    WHERE execution_id = '{execution_id}'
                """).collect()
                
                # Check if policy is ready
                if policy_status in ['success', 'completed', 'active', 'ready']:
                    policy_ready = True
                    
                    # Update state to policy_ready
                    success_message = f'Policy status check successful after {poll_count} attempts'
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET status = 'policy_ready',
                            requires_circuit_breaker = TRUE,
                            message = '{success_message}'
                        WHERE execution_id = '{execution_id}'
                    """).collect()
                    
                elif poll_count >= max_polls:
                    # Max retries reached without success
                    error_message = f'Policy status check failed after {poll_count} attempts. Last status: {policy_status}'.replace("'", "''")
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET status = 'error',
                            completed = TRUE,
                            message = '{error_message}'
                        WHERE execution_id = '{execution_id}'
                    """).collect()
                    
                    return {
                        "EXECUTION_ID": execution_id,
                        "POLICY_ID": policy_id,
                        "STATUS": "error",
                        "POLL_COUNT": poll_count,
                        "ERROR_MESSAGE": f"Policy status check failed after {poll_count} attempts",
                        "COMPLETED": True,
                        "SUCCESS": False
                    }
                    
                # Small delay between polls (if not the last attempt)
                if poll_count < max_polls and not policy_ready:
                    time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                error_msg = f"Policy status check failed (attempt {poll_count}): {str(e)}".replace("'", "''")
                
                if poll_count >= max_polls:
                    # Max retries reached
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET status = 'error',
                            completed = TRUE,
                            error_message = '{error_msg}',
                            message = 'Policy status check failed after {poll_count} attempts'
                        WHERE execution_id = '{execution_id}'
                    """).collect()
                    
                    return {
                        "EXECUTION_ID": execution_id,
                        "POLICY_ID": policy_id,
                        "STATUS": "error",
                        "POLL_COUNT": poll_count,
                        "ERROR_MESSAGE": error_msg,
                        "COMPLETED": True,
                        "SUCCESS": False
                    }
                else:
                    # Continue with next attempt
                    retry_message = f'Policy status check attempt {poll_count}/3 failed, retrying'
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET error_message = '{error_msg}',
                            message = '{retry_message}'
                        WHERE execution_id = '{execution_id}'
                    """).collect()
        
        # Step 3: Call Circuit Breaker API
        if policy_ready:
            try:
                circuit_url = "https://dbttest.free.beeceptor.com/circuit"
                
                circuit_payload = {
                    "policy_id": policy_id,
                    "execution_id": execution_id,
                    "trigger_reason": "policy_execution_completed",
                    "timestamp": datetime.now().isoformat()
                }
                
                response = requests.post(
                    circuit_url,
                    json=circuit_payload,
                    headers={
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    },
                    timeout=30
                )
                
                response.raise_for_status()
                circuit_response = response.json() if response.content else {}
                
                # Update state as completed
                circuit_response_json = json.dumps(circuit_response).replace("'", "''")
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET status = 'completed',
                        circuit_breaker_initiated = TRUE,
                        circuit_breaker_completed = TRUE,
                        circuit_breaker_response = PARSE_JSON('{circuit_response_json}'),
                        message = 'Circuit breaker initiated successfully',
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
                    "MESSAGE": "Circuit breaker initiated successfully",
                    "SUCCESS": True
                }
                
            except requests.exceptions.RequestException as e:
                error_msg = f"Circuit breaker API call failed: {str(e)}".replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET status = 'error',
                        error_message = '{error_msg}',
                        message = 'Circuit breaker initiation failed',
                        completed = TRUE
                    WHERE execution_id = '{execution_id}'
                """).collect()
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "error",
                    "ERROR_MESSAGE": error_msg,
                    "COMPLETED": True,
                    "SUCCESS": False
                }
        
        # Should not reach here
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": "Unexpected end of workflow",
            "COMPLETED": True,
            "SUCCESS": False
        }
            
    except Exception as e:
        error_msg = f"Policy execution workflow failed: {str(e)}"
        return {
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "COMPLETED": True,
            "SUCCESS": False
        }
$$
    {% endset %}
    
    {% do run_query(create_policy_procedure_sql) %}
    
    {# Execute the stored procedure and return results #}
    {% set execute_sql %}
        CALL {{ target.database }}.{{ target.schema }}.execute_policy_workflow(
            PARSE_JSON('{{ tojson(policy_data) }}')
        )
    {% endset %}
    
    {% do run_query("DELETE FROM " ~ target.database ~ "." ~ target.schema ~ ".policy_execution_state WHERE completed = TRUE AND start_time < DATEADD(hour, -1, CURRENT_TIMESTAMP())") %}
    {% do return(execute_sql) %}
{% endmacro %} 