{% macro create_policy_step() %}
    {# Step 1: Create Policy Stored Procedure #}
    {% set create_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.create_policy_step(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'create_policy_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime
from snowflake.snowpark import Session

def create_policy_step(session: Session, policy_data):
    """Step 1: Create Policy and Store Policy ID in State Table."""
    try:
        print(f"DEBUG: STEP 1 - Creating policy and storing in state table")
        
        # Prepare payload
        if isinstance(policy_data, str):
            payload = json.loads(policy_data)
        else:
            payload = policy_data or {}
            
        # Add timestamp if not present
        if 'timestamp' not in payload:
            payload['timestamp'] = datetime.now().isoformat()
        
        # Call create policy API
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
        
        # Handle JSON parsing gracefully
        try:
            if response.content:
                api_response = response.json()
            else:
                api_response = {"message": "Empty response", "policy_id": f"policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
        except json.JSONDecodeError as e:
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
        
        # Store policy ID in state table
        session.sql(f"""
            INSERT INTO policy_execution_state (
                "execution_id", "policy_id", "poll_count", "status", "last_poll_time", "message"
            )
            VALUES (
                '{execution_id}', '{policy_id}', 0, 'policy_created', CURRENT_TIMESTAMP(),
                'Policy created successfully and stored in state table'
            )
        """).collect()
        
        # Update with JSON data separately
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
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "policy_created",
            "STEP": 1,
            "SUCCESS": True,
            "MESSAGE": "Policy created successfully and stored in state table"
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
            "SUCCESS": False
        }
$$
    {% endset %}
    {% do run_query(create_policy_procedure_sql) %}
{% endmacro %}

{% macro poll_policy_status_step() %}
    {# Step 2: Poll Policy Status Stored Procedure #}
    {% set poll_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.poll_policy_status_step(execution_id VARCHAR, policy_id VARCHAR)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'poll_policy_status_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime
import time
from snowflake.snowpark import Session

def poll_policy_status_step(session: Session, execution_id: str, policy_id: str):
    """Step 2: Poll Policy Status API with Max 3 Retries and Validate Database State."""
    try:
        print(f"DEBUG: STEP 2 - Starting policy status polling for execution_id: {execution_id}, policy_id: {policy_id}")
        
        policy_ready = False
        poll_count = 0
        max_polls = 3
        
        while poll_count < max_polls and not policy_ready:
            poll_count += 1
            print(f"DEBUG: STEP 2 - Poll attempt {poll_count} of {max_polls}")
            
            # Check database state first
            db_state_result = session.sql(f"""
                SELECT "policy_id", "status" FROM policy_execution_state 
                WHERE "execution_id" = '{execution_id}' AND "policy_id" = '{policy_id}'
            """).collect()
            
            if not db_state_result:
                raise Exception(f"Policy not found in database for execution_id: {execution_id}")
            
            db_status = db_state_result[0]['status']
            print(f"DEBUG: STEP 2 - Database status: {db_status}")
            
            # Call policy status API
            status_url = f"https://dbttest.free.beeceptor.com/policy/{policy_id}"
            print(f"DEBUG: STEP 2 - Calling policy status API: {status_url}")
            
            response = requests.get(
                status_url,
                headers={'Accept': 'application/json'},
                timeout=30
            )
            
            response.raise_for_status()
            
            # Handle JSON parsing gracefully
            try:
                if response.content:
                    status_response = response.json()
                else:
                    status_response = {"message": "Empty response", "status": "pending"}
            except json.JSONDecodeError as e:
                status_response = {
                    "message": "Non-JSON response received", 
                    "status": "pending",
                    "raw_response": response.text[:200] if response.text else "No content"
                }
                print(f"DEBUG: STEP 2 - JSON parse error: {e}, using fallback response")
            
            print(f"DEBUG: STEP 2 - API response: {status_response}")
            
            # Update state with poll attempt
            status_response_json = json.dumps(status_response).replace("'", "''")
            session.sql(f"""
                UPDATE policy_execution_state 
                SET "poll_count" = {poll_count},
                    "last_poll_time" = CURRENT_TIMESTAMP(),
                    "policy_status_response" = PARSE_JSON('{status_response_json}')
                WHERE "execution_id" = '{execution_id}'
            """).collect()
            
            # Check if policy is ready
            api_policy_status = status_response.get('status', 'pending').lower()
            success_statuses = ['success', 'completed', 'active', 'ready']
            api_ready = api_policy_status in success_statuses
            db_ready = db_status in ['policy_created', 'policy_ready']
            
            print(f"DEBUG: STEP 2 - API ready: {api_ready}, DB ready: {db_ready}")
            
            if api_ready and db_ready:
                policy_ready = True
                print(f"DEBUG: STEP 2 - Policy is ready! (API: {api_policy_status}, DB: {db_status})")
                
                # Update state to policy_ready
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
                
                return {
                    "EXECUTION_ID": execution_id,
                    "POLICY_ID": policy_id,
                    "STATUS": "policy_ready",
                    "POLL_COUNT": poll_count,
                    "STEP": 2,
                    "SUCCESS": True,
                    "MESSAGE": success_message
                }
            else:
                print(f"DEBUG: STEP 2 - Policy not ready yet, attempt {poll_count}")
                if poll_count < max_polls:
                    time.sleep(2)  # Wait 2 seconds before next poll
        
        # If we get here, max polls reached without success
        error_msg = f"STEP 2 FAILED - Max polling attempts ({max_polls}) reached without success"
        session.sql(f"""
            UPDATE policy_execution_state 
            SET "status" = 'error',
                "error_message" = '{error_msg}',
                "completed" = TRUE
            WHERE "execution_id" = '{execution_id}'
        """).collect()
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "POLL_COUNT": poll_count,
            "STEP_FAILED": 2,
            "SUCCESS": False
        }
        
    except Exception as e:
        error_msg = f"STEP 2 FAILED - {str(e)}".replace("'", "''")
        
        session.sql(f"""
            UPDATE policy_execution_state 
            SET "status" = 'error',
                "error_message" = '{error_msg}',
                "completed" = TRUE
            WHERE "execution_id" = '{execution_id}'
        """).collect()
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "STEP_FAILED": 2,
            "SUCCESS": False
        }
$$
    {% endset %}
    {% do run_query(poll_policy_procedure_sql) %}
{% endmacro %}

{% macro trigger_circuit_breaker_step() %}
    {# Step 3: Trigger Circuit Breaker Stored Procedure #}
    {% set trigger_circuit_breaker_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.trigger_circuit_breaker_step(execution_id VARCHAR, policy_id VARCHAR)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'trigger_circuit_breaker_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime
from snowflake.snowpark import Session

def trigger_circuit_breaker_step(session: Session, execution_id: str, policy_id: str):
    """Step 3: Call Circuit Breaker API."""
    try:
        print(f"DEBUG: STEP 3 - Triggering circuit breaker for execution_id: {execution_id}, policy_id: {policy_id}")
        
        # Final verification that policy is ready in database
        final_verify = session.sql(f"""
            SELECT "status" FROM policy_execution_state 
            WHERE "execution_id" = '{execution_id}' AND "status" = 'policy_ready'
        """).collect()
        
        if not final_verify:
            raise Exception("Policy not confirmed ready in database before circuit breaker call")
        
        # Call circuit breaker API
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
        
        # Handle JSON parsing gracefully
        try:
            if response.content:
                circuit_response = response.json()
            else:
                circuit_response = {"message": "Empty response", "status": "success"}
        except json.JSONDecodeError as e:
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
        
        # Update with JSON data separately
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
            "STEP": 3,
            "SUCCESS": True,
            "MESSAGE": "All 3 steps completed successfully: create->poll->circuit"
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
            "SUCCESS": False
        }
$$
    {% endset %}
    {% do run_query(trigger_circuit_breaker_procedure_sql) %}
{% endmacro %}

{% macro policy_execution(policy_data={}) %}
    {# Create the step procedures #}
    {{ create_policy_step() }}
    {{ poll_policy_status_step() }}
    {{ trigger_circuit_breaker_step() }}
    
    {# Clear old completed records #}
    {% do run_query('DELETE FROM ' ~ target.database ~ '.' ~ target.schema ~ '.policy_execution_state WHERE "completed" = TRUE AND "start_time" < DATEADD(hour, -1, CURRENT_TIMESTAMP())') %}
    
    {# Step 1: Create Policy #}
    {{ log("Executing Step 1: Create Policy", info=True) }}
    {% set step1_sql %}
        CALL {{ target.database }}.{{ target.schema }}.create_policy_step(
            PARSE_JSON('{{ tojson(policy_data) }}')
        )
    {% endset %}
    
    {% do run_query(step1_sql) %}
    
    {# Check if Step 1 succeeded by querying the state table #}
    {% set check_step1_sql %}
        SELECT "execution_id", "policy_id", "status"
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
        WHERE "status" = 'policy_created'
        AND "start_time" >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        ORDER BY "start_time" DESC
        LIMIT 1
    {% endset %}
    
    {% set step1_check = run_query(check_step1_sql) %}
    {% if not step1_check or not step1_check.rows or step1_check.rows|length == 0 %}
        {{ log("Step 1 failed - no policy_created record found", info=True) }}
        {% do return("Step 1 failed") %}
    {% endif %}
    
    {% set execution_id = step1_check.rows[0][0] %}
    {% set policy_id = step1_check.rows[0][1] %}
    {{ log("Step 1 completed - execution_id: " ~ execution_id ~ ", policy_id: " ~ policy_id, info=True) }}
    
    {# Step 2: Poll Policy Status #}
    {{ log("Executing Step 2: Poll Policy Status", info=True) }}
    {% set step2_sql %}
        CALL {{ target.database }}.{{ target.schema }}.poll_policy_status_step(
            '{{ execution_id }}', '{{ policy_id }}'
        )
    {% endset %}
    
    {% do run_query(step2_sql) %}
    
    {# Check if Step 2 succeeded #}
    {% set check_step2_sql %}
        SELECT "status"
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
        WHERE "execution_id" = '{{ execution_id }}'
        AND "status" = 'policy_ready'
    {% endset %}
    
    {% set step2_check = run_query(check_step2_sql) %}
    {% if not step2_check or not step2_check.rows or step2_check.rows|length == 0 %}
        {{ log("Step 2 failed - policy not ready", info=True) }}
        {% do return("Step 2 failed") %}
    {% endif %}
    
    {{ log("Step 2 completed - Policy is ready for circuit breaker", info=True) }}
    
    {# Step 3: Trigger Circuit Breaker #}
    {{ log("Executing Step 3: Trigger Circuit Breaker", info=True) }}
    {% set step3_sql %}
        CALL {{ target.database }}.{{ target.schema }}.trigger_circuit_breaker_step(
            '{{ execution_id }}', '{{ policy_id }}'
        )
    {% endset %}
    
    {% do run_query(step3_sql) %}
    
    {# Check if Step 3 succeeded #}
    {% set check_step3_sql %}
        SELECT "status", "circuit_breaker_completed"
        FROM {{ target.database }}.{{ target.schema }}.policy_execution_state
        WHERE "execution_id" = '{{ execution_id }}'
        AND "status" = 'completed'
        AND "circuit_breaker_completed" = TRUE
    {% endset %}
    
    {% set step3_check = run_query(check_step3_sql) %}
    {% if not step3_check or not step3_check.rows or step3_check.rows|length == 0 %}
        {{ log("Step 3 failed - circuit breaker not completed", info=True) }}
        {% do return("Step 3 failed") %}
    {% endif %}
    
    {{ log("Step 3 completed - All steps successful!", info=True) }}
    {{ log("Execution ID: " ~ execution_id ~ " completed successfully", info=True) }}
    
    {% do return("Workflow completed successfully") %}
{% endmacro %}