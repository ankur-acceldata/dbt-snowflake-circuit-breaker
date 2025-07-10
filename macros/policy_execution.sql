{% macro policy_execution(policy_data={}) %}
    {# Create state table for tracking policy execution #}
    {% set create_state_table_sql %}
        CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.policy_execution_state (
            "execution_id" VARCHAR,
            "policy_id" VARCHAR,
            "poll_count" NUMBER DEFAULT 0,
            "status" VARCHAR DEFAULT 'pending',
            "start_time" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            "last_poll_time" TIMESTAMP_NTZ,
            "completed" BOOLEAN DEFAULT FALSE,
            "requires_circuit_breaker" BOOLEAN DEFAULT FALSE,
            "circuit_breaker_initiated" BOOLEAN DEFAULT FALSE,
            "circuit_breaker_completed" BOOLEAN DEFAULT FALSE,
            "error_message" VARCHAR,
            "message" VARCHAR,
            "api_response" VARIANT,
            "policy_data" VARIANT,
            "policy_status_response" VARIANT,
            "circuit_breaker_response" VARIANT
        );
    {% endset %}
    {% do run_query(create_state_table_sql) %}

    {# Create the stored procedure for policy execution with proper 3-step workflow #}
    {% set create_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.execute_policy_workflow(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'execute_policy_workflow'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
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
        print(f"DEBUG: Starting 3-step policy workflow execution")
        
        # Clear only completed records older than 1 hour
        try:
            session.sql('DELETE FROM policy_execution_state WHERE "completed" = TRUE AND "start_time" < DATEADD(hour, -1, CURRENT_TIMESTAMP())').collect()
            print("DEBUG: Cleared old completed records")
        except Exception as e:
            print(f"DEBUG: Could not clear old records: {e}")
            pass
        
        # Prepare payload
        if isinstance(policy_data, str):
            payload = json.loads(policy_data)
        else:
            payload = policy_data or {}
            
        # Add timestamp if not present
        if 'timestamp' not in payload:
            payload['timestamp'] = datetime.now().isoformat()
        
        # STEP 1: Create Policy and Store Policy ID in State Table
        print(f"DEBUG: STEP 1 - Creating policy and storing in state table")
        try:
            create_url = "https://dbttest.free.beeceptor.com/createpolicy"
            print(f"DEBUG: STEP 1 - Creating policy at {create_url}")
            
            response = requests.post(
                create_url,
                json=payload,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                timeout=30
            )
            
            print(f"DEBUG: STEP 1 - API response status: {response.status_code}")
            response.raise_for_status()
            
            # Handle JSON parsing more gracefully
            try:
                if response.content:
                    api_response = response.json()
                else:
                    api_response = {"message": "Empty response", "policy_id": f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
            except json.JSONDecodeError as e:
                # If response is not JSON, create a fallback response
                api_response = {
                    "message": "Non-JSON response received", 
                    "policy_id": f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "execution_id": f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "raw_response": response.text[:200] if response.text else "No content"
                }
                print(f"DEBUG: STEP 1 - JSON parse error: {e}, using fallback response")
            
            print(f"DEBUG: STEP 1 - API response: {api_response}")
            
            # Extract policy ID and execution ID from response
            policy_id = api_response.get('policy_id', api_response.get('id', f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
            execution_id = api_response.get('execution_id', f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            print(f"DEBUG: STEP 1 - Extracted policy_id: {policy_id}, execution_id: {execution_id}")
            
            # Store policy ID in state table - using separate INSERT for JSON data
            session.sql(f"""
                INSERT INTO policy_execution_state (
                    "execution_id", "policy_id", "poll_count", "status", "last_poll_time", "message"
                )
                VALUES (
                    '{execution_id}', '{policy_id}', 0, 'policy_created', CURRENT_TIMESTAMP(),
                    'Policy created successfully and stored in state table'
                )
            """).collect()
            
            # Update with JSON data separately to avoid VALUES clause issues
            api_response_json = json.dumps(api_response).replace("'", "''")
            payload_json = json.dumps(payload).replace("'", "''")
            
            session.sql(f"""
                UPDATE policy_execution_state 
                SET "api_response" = PARSE_JSON('{api_response_json}'),
                    "policy_data" = PARSE_JSON('{payload_json}')
                WHERE "execution_id" = '{execution_id}'
            """).collect()
            
            # Verify policy ID was stored in database
            verify_result = session.sql(f"""
                SELECT "policy_id", "status" FROM policy_execution_state 
                WHERE "execution_id" = '{execution_id}' AND "policy_id" = '{policy_id}'
            """).collect()
            
            if not verify_result:
                raise Exception("Failed to verify policy ID storage in database")
            
            print(f"DEBUG: STEP 1 - Policy ID successfully stored in state table: {policy_id}")
            
        except requests.exceptions.RequestException as e:
            error_msg = f"STEP 1 FAILED - API call error: {str(e)}".replace("'", "''")
            execution_id = f"exec_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            session.sql(f"""
                INSERT INTO policy_execution_state (
                    "execution_id", "status", "error_message", "last_poll_time", "completed"
                )
                VALUES (
                    '{execution_id}', 'error', '{error_msg}', CURRENT_TIMESTAMP(), TRUE
                )
            """).collect()
            
            return {
                "EXECUTION_ID": execution_id,
                "STATUS": "error",
                "ERROR_MESSAGE": error_msg,
                "STEP_FAILED": 1,
                "COMPLETED": True,
                "SUCCESS": False
            }
        except Exception as e:
            error_msg = f"STEP 1 FAILED - {str(e)}".replace("'", "''")
            execution_id = f"exec_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            session.sql(f"""
                INSERT INTO policy_execution_state (
                    "execution_id", "status", "error_message", "last_poll_time", "completed"
                )
                VALUES (
                    '{execution_id}', 'error', '{error_msg}', CURRENT_TIMESTAMP(), TRUE
                )
            """).collect()
            
            return {
                "EXECUTION_ID": execution_id,
                "STATUS": "error",
                "ERROR_MESSAGE": error_msg,
                "STEP_FAILED": 1,
                "COMPLETED": True,
                "SUCCESS": False
            }
        
        # STEP 2: Poll Policy Status API with Max 3 Retries and Validate Database State
        print(f"DEBUG: STEP 2 - Starting policy status polling (max 3 retries)")
        policy_ready = False
        poll_count = 0
        max_polls = 3
        
        while poll_count < max_polls and not policy_ready:
            poll_count += 1
            print(f"DEBUG: STEP 2 - Poll attempt {poll_count} of {max_polls}")
            
            try:
                # Check database state first
                db_state_result = session.sql(f"""
                    SELECT "policy_id", "status" FROM policy_execution_state 
                    WHERE "execution_id" = '{execution_id}' AND "policy_id" = '{policy_id}'
                """).collect()
                
                if not db_state_result:
                    raise Exception("Policy not found in database state table")
                
                db_status = db_state_result[0][1]
                print(f"DEBUG: STEP 2 - Database state: {db_status}")
                
                # Call policy status API
                policy_status_url = f"https://dbttest.free.beeceptor.com/policy/{policy_id}"
                print(f"DEBUG: STEP 2 - Polling URL: {policy_status_url}")
                
                response = requests.get(
                    policy_status_url,
                    headers={'Accept': 'application/json'},
                    timeout=30
                )
                
                response.raise_for_status()
                
                # Handle JSON parsing more gracefully
                try:
                    if response.content:
                        policy_status_response = response.json()
                    else:
                        policy_status_response = {"status": "unknown", "message": "Empty response"}
                except json.JSONDecodeError as e:
                    # If response is not JSON, create a fallback response
                    policy_status_response = {
                        "status": "unknown", 
                        "message": "Non-JSON response received",
                        "raw_response": response.text[:200] if response.text else "No content"
                    }
                    print(f"DEBUG: STEP 2 - JSON parse error: {e}, using fallback response")
                
                api_policy_status = policy_status_response.get('status', 'unknown')
                print(f"DEBUG: STEP 2 - API policy status: {api_policy_status}")
                
                # Update state table with poll result
                message = f'Poll {poll_count}/3: API status={api_policy_status}, DB status={db_status}'.replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "poll_count" = {poll_count},
                        "last_poll_time" = CURRENT_TIMESTAMP(),
                        "message" = '{message}'
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                # Update with JSON data separately to avoid parsing issues
                policy_response_json = json.dumps(policy_status_response).replace("'", "''")
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "policy_status_response" = PARSE_JSON('{policy_response_json}')
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                # Check if policy is ready (both API and database validation)
                success_statuses = ['success', 'completed', 'active', 'ready']
                api_ready = api_policy_status in success_statuses
                db_ready = db_status in ['policy_created', 'policy_ready']
                
                print(f"DEBUG: STEP 2 - API ready: {api_ready}, DB ready: {db_ready}")
                
                if api_ready and db_ready:
                    policy_ready = True
                    print(f"DEBUG: STEP 2 - Policy is ready! (API: {api_policy_status}, DB: {db_status})")
                    
                    # Update state to policy_ready with success status
                    success_message = f'Policy ready after {poll_count} attempts (API: {api_policy_status})'
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET "status" = 'policy_ready',
                            "requires_circuit_breaker" = TRUE,
                            "message" = '{success_message}'
                        WHERE "execution_id" = '{execution_id}'
                    """).collect()
                    
                    # Verify success status is stored in table
                    verify_result = session.sql(f"""
                        SELECT "status" FROM policy_execution_state 
                        WHERE "execution_id" = '{execution_id}' AND "status" = 'policy_ready'
                    """).collect()
                    
                    if not verify_result:
                        raise Exception("Failed to verify success status in database")
                    
                    print(f"DEBUG: STEP 2 - Success status confirmed in database table")
                    
                elif poll_count >= max_polls:
                    # Max retries reached without success
                    error_message = f'STEP 2 FAILED - Max retries reached. API: {api_policy_status}, DB: {db_status}'.replace("'", "''")
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET "status" = 'error',
                            "completed" = TRUE,
                            "error_message" = '{error_message}'
                        WHERE "execution_id" = '{execution_id}'
                    """).collect()
                    
                    return {
                        "EXECUTION_ID": execution_id,
                        "POLICY_ID": policy_id,
                        "STATUS": "error",
                        "POLL_COUNT": poll_count,
                        "ERROR_MESSAGE": f"Policy status polling failed after {poll_count} attempts",
                        "STEP_FAILED": 2,
                        "COMPLETED": True,
                        "SUCCESS": False
                    }
                
                # Small delay between polls (if not the last attempt)
                if poll_count < max_polls and not policy_ready:
                    print(f"DEBUG: STEP 2 - Waiting 2 seconds before next poll...")
                    time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                error_msg = f"STEP 2 - API call failed (attempt {poll_count}): {str(e)}".replace("'", "''")
                
                if poll_count >= max_polls:
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET "status" = 'error',
                            "completed" = TRUE,
                            "error_message" = '{error_msg}'
                        WHERE "execution_id" = '{execution_id}'
                    """).collect()
                    
                    return {
                        "EXECUTION_ID": execution_id,
                        "POLICY_ID": policy_id,
                        "STATUS": "error",
                        "POLL_COUNT": poll_count,
                        "ERROR_MESSAGE": error_msg,
                        "STEP_FAILED": 2,
                        "COMPLETED": True,
                        "SUCCESS": False
                    }
                else:
                    # Continue with next attempt
                    retry_message = f'Poll attempt {poll_count}/3 failed, retrying'
                    session.sql(f"""
                        UPDATE policy_execution_state 
                        SET "error_message" = '{error_msg}',
                            "message" = '{retry_message}'
                        WHERE "execution_id" = '{execution_id}'
                    """).collect()
                    
            except Exception as e:
                error_msg = f"STEP 2 - Database validation failed: {str(e)}".replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "status" = 'error',
                        "completed" = TRUE,
                        "error_message" = '{error_msg}'
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "error",
                    "ERROR_MESSAGE": error_msg,
                    "STEP_FAILED": 2,
                    "COMPLETED": True,
                    "SUCCESS": False
                }
        
        # STEP 3: Call Circuit Breaker API (Only if Step 2 Success)
        print(f"DEBUG: STEP 3 - Policy ready status: {policy_ready}")
        if policy_ready:
            print(f"DEBUG: STEP 3 - Calling circuit breaker API")
            try:
                # Final verification that policy is ready in database
                final_verify = session.sql(f"""
                    SELECT "status" FROM policy_execution_state 
                    WHERE "execution_id" = '{execution_id}' AND "status" = 'policy_ready'
                """).collect()
                
                if not final_verify:
                    raise Exception("Policy not confirmed ready in database before circuit breaker call")
                
                circuit_url = "https://dbttest.free.beeceptor.com/circuit"
                print(f"DEBUG: STEP 3 - Calling circuit breaker at {circuit_url}")
                
                circuit_payload = {
                    "policy_id": policy_id,
                    "execution_id": execution_id,
                    "trigger_reason": "policy_execution_completed_successfully",
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
                
                # Handle JSON parsing more gracefully
                try:
                    if response.content:
                        circuit_response = response.json()
                    else:
                        circuit_response = {"message": "Empty response", "status": "success"}
                except json.JSONDecodeError as e:
                    # If response is not JSON, create a fallback response
                    circuit_response = {
                        "message": "Non-JSON response received", 
                        "status": "success",
                        "raw_response": response.text[:200] if response.text else "No content",
                        "content_type": response.headers.get('Content-Type', 'unknown')
                    }
                    print(f"DEBUG: STEP 3 - JSON parse error: {e}, using fallback response")
                
                print(f"DEBUG: STEP 3 - Circuit breaker response: {circuit_response}")
                
                # Update state as completed successfully
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "status" = 'completed',
                        "circuit_breaker_initiated" = TRUE,
                        "circuit_breaker_completed" = TRUE,
                        "message" = 'All 3 steps completed successfully: create->poll->circuit',
                        "completed" = TRUE
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                # Update with JSON data separately to avoid parsing issues
                circuit_response_json = json.dumps(circuit_response).replace("'", "''")
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "circuit_breaker_response" = PARSE_JSON('{circuit_response_json}')
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                print(f"DEBUG: STEP 3 - Circuit breaker completed successfully")
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "completed",
                    "POLL_COUNT": poll_count,
                    "COMPLETED": True,
                    "CIRCUIT_BREAKER_INITIATED": True,
                    "CIRCUIT_BREAKER_COMPLETED": True,
                    "MESSAGE": "All 3 steps completed successfully: create->poll->circuit",
                    "SUCCESS": True
                }
                
            except requests.exceptions.RequestException as e:
                error_msg = f"STEP 3 FAILED - Circuit breaker API call failed: {str(e)}".replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "status" = 'error',
                        "error_message" = '{error_msg}',
                        "message" = 'Steps 1&2 successful, Step 3 failed',
                        "completed" = TRUE
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "error",
                    "ERROR_MESSAGE": error_msg,
                    "STEP_FAILED": 3,
                    "COMPLETED": True,
                    "SUCCESS": False
                }
            except Exception as e:
                error_msg = f"STEP 3 FAILED - {str(e)}".replace("'", "''")
                
                session.sql(f"""
                    UPDATE policy_execution_state 
                    SET "status" = 'error',
                        "error_message" = '{error_msg}',
                        "message" = 'Steps 1&2 successful, Step 3 failed',
                        "completed" = TRUE
                    WHERE "execution_id" = '{execution_id}'
                """).collect()
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "error",
                    "ERROR_MESSAGE": error_msg,
                    "STEP_FAILED": 3,
                    "COMPLETED": True,
                    "SUCCESS": False
                }
        
        # Should not reach here if workflow is correct
        print(f"DEBUG: ERROR - Reached end of workflow without policy_ready=True")
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": "Workflow completed without policy being ready",
            "COMPLETED": True,
            "SUCCESS": False
        }
            
    except Exception as e:
        error_msg = f"WORKFLOW FAILED - Unexpected error: {str(e)}"
        print(f"DEBUG: {error_msg}")
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
    
    {% do run_query('DELETE FROM ' ~ target.database ~ '.' ~ target.schema ~ '.policy_execution_state WHERE "completed" = TRUE AND "start_time" < DATEADD(hour, -1, CURRENT_TIMESTAMP())') %}
    {% do return(execute_sql) %}
{% endmacro %}