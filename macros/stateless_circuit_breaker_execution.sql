{% macro stateless_create_policy_step() %}
    {# Stateless Step 1: Create Policy without state persistence #}
    {% set stateless_create_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.stateless_create_policy_step(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'stateless_create_policy_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime

def stateless_create_policy_step(session, policy_data):
    """Stateless Step 1: Create Policy without database persistence."""
    try:
        print(f"DEBUG: STATELESS STEP 1 - Creating policy (no state persistence)")
        
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
        print(f"DEBUG: STATELESS STEP 1 - Creating policy at {create_url}")
        
        response = requests.post(
            create_url,
            json=payload,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout=30
        )
        
        print(f"DEBUG: STATELESS STEP 1 - API response status: {response.status_code}")
        response.raise_for_status()
        
        # Handle JSON parsing gracefully
        try:
            if response.content:
                api_response = response.json()
            else:
                api_response = {"message": "Empty response", "policy_id": f"stateless_policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
        except json.JSONDecodeError as e:
            api_response = {
                "message": "Non-JSON response received", 
                "policy_id": f"stateless_policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "execution_id": f"stateless_exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "raw_response": response.text[:200] if response.text else "No content"
            }
            print(f"DEBUG: STATELESS STEP 1 - JSON parse error: {e}, using fallback response")
        
        print(f"DEBUG: STATELESS STEP 1 - API response: {api_response}")
        
        # Extract policy ID from response
        policy_id = api_response.get('policy_id', api_response.get('id', f"stateless_policy_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
        execution_id = api_response.get('execution_id', f"stateless_exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        print(f"DEBUG: STATELESS STEP 1 - Policy created successfully: {policy_id}")
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "policy_created",
            "STEP": 1,
            "SUCCESS": True,
            "MESSAGE": "Policy created successfully (stateless mode)",
            "API_RESPONSE": api_response
        }
        
    except Exception as e:
        error_msg = f"STATELESS STEP 1 FAILED - {str(e)}"
        print(f"ERROR: {error_msg}")
        
        return {
            "EXECUTION_ID": f"stateless_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "STEP_FAILED": 1,
            "SUCCESS": False
        }
$$
    {% endset %}
    {% do run_query(stateless_create_policy_procedure_sql) %}
{% endmacro %}

{% macro stateless_poll_policy_status_step() %}
    {# Stateless Step 2: Poll Policy Status without state persistence #}
    {% set stateless_poll_policy_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.stateless_poll_policy_status_step(policy_id VARCHAR)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'stateless_poll_policy_status_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime
import time

def stateless_poll_policy_status_step(session, policy_id: str):
    """Stateless Step 2: Poll Policy Status without database persistence."""
    try:
        print(f"DEBUG: STATELESS STEP 2 - Starting policy status polling for policy_id: {policy_id}")
        
        policy_ready = False
        poll_count = 0
        max_polls = 3
        
        while poll_count < max_polls and not policy_ready:
            poll_count += 1
            print(f"DEBUG: STATELESS STEP 2 - Poll attempt {poll_count} of {max_polls}")
            
            # Call policy status API
            status_url = f"https://dbttest.free.beeceptor.com/policy/{policy_id}"
            print(f"DEBUG: STATELESS STEP 2 - Calling policy status API: {status_url}")
            
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
                print(f"DEBUG: STATELESS STEP 2 - JSON parse error: {e}, using fallback response")
            
            print(f"DEBUG: STATELESS STEP 2 - API response: {status_response}")
            
            # Check if policy is ready
            api_policy_status = status_response.get('status', 'pending').lower()
            success_statuses = ['success', 'completed', 'active', 'ready']
            api_ready = api_policy_status in success_statuses
            
            print(f"DEBUG: STATELESS STEP 2 - API status: {api_policy_status}, Ready: {api_ready}")
            
            if api_ready:
                policy_ready = True
                print(f"DEBUG: STATELESS STEP 2 - Policy is ready! (API: {api_policy_status})")
                
                return {
                    "POLICY_ID": policy_id,
                    "STATUS": "policy_ready",
                    "POLL_COUNT": poll_count,
                    "STEP": 2,
                    "SUCCESS": True,
                    "MESSAGE": f"Policy ready after {poll_count} attempts (API: {api_policy_status})",
                    "API_RESPONSE": status_response
                }
            else:
                print(f"DEBUG: STATELESS STEP 2 - Policy not ready yet, attempt {poll_count}")
                if poll_count < max_polls:
                    time.sleep(2)  # Wait 2 seconds before next poll
        
        # If we get here, max polls reached without success
        error_msg = f"STATELESS STEP 2 FAILED - Max polling attempts ({max_polls}) reached without success"
        print(f"ERROR: {error_msg}")
        
        return {
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "POLL_COUNT": poll_count,
            "STEP_FAILED": 2,
            "SUCCESS": False
        }
        
    except Exception as e:
        error_msg = f"STATELESS STEP 2 FAILED - {str(e)}"
        print(f"ERROR: {error_msg}")
        
        return {
            "POLICY_ID": policy_id,
            "STATUS": "error",
            "ERROR_MESSAGE": error_msg,
            "STEP_FAILED": 2,
            "SUCCESS": False
        }
$$
    {% endset %}
    {% do run_query(stateless_poll_policy_procedure_sql) %}
{% endmacro %}

{% macro stateless_trigger_circuit_breaker_step() %}
    {# Stateless Step 3: Trigger Circuit Breaker without state persistence #}
    {% set stateless_trigger_circuit_breaker_procedure_sql %}
        CREATE OR REPLACE PROCEDURE {{ target.database }}.{{ target.schema }}.stateless_trigger_circuit_breaker_step(execution_id VARCHAR, policy_id VARCHAR)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'stateless_trigger_circuit_breaker_step'
        PACKAGES = ('requests', 'snowflake-snowpark-python')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime

def stateless_trigger_circuit_breaker_step(session, execution_id: str, policy_id: str):
    """Stateless Step 3: Call Circuit Breaker API without database persistence."""
    try:
        print(f"DEBUG: STATELESS STEP 3 - Triggering circuit breaker for execution_id: {execution_id}, policy_id: {policy_id}")
        
        # Call circuit breaker API
        circuit_url = "https://dbttest.free.beeceptor.com/circuit"
        print(f"DEBUG: STATELESS STEP 3 - Calling circuit breaker at {circuit_url}")
        
        circuit_payload = {
            "policy_id": policy_id,
            "execution_id": execution_id,
            "trigger_reason": "stateless_policy_execution_completed",
            "timestamp": datetime.now().isoformat(),
            "mode": "stateless"
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
            print(f"DEBUG: STATELESS STEP 3 - JSON parse error: {e}, using fallback response")
        
        print(f"DEBUG: STATELESS STEP 3 - Circuit breaker response: {circuit_response}")
        print(f"DEBUG: STATELESS STEP 3 - Circuit breaker completed successfully (stateless mode)")
        
        return {
            "EXECUTION_ID": execution_id,
            "POLICY_ID": policy_id,
            "STATUS": "completed",
            "STEP": 3,
            "SUCCESS": True,
            "MESSAGE": "All 3 steps completed successfully (stateless mode): create->poll->circuit",
            "API_RESPONSE": circuit_response
        }
        
    except Exception as e:
        error_msg = f"STATELESS STEP 3 FAILED - {str(e)}"
        print(f"ERROR: {error_msg}")
        
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
    {% do run_query(stateless_trigger_circuit_breaker_procedure_sql) %}
{% endmacro %}

{% macro stateless_circuit_breaker_execution(policy_data={}) %}
    {# Create the stateless step procedures #}
    {{ stateless_create_policy_step() }}
    {{ stateless_poll_policy_status_step() }}
    {{ stateless_trigger_circuit_breaker_step() }}
    
    {# Step 1: Create Policy (Stateless) #}
    {{ log("Executing Stateless Step 1: Create Policy", info=True) }}
    {% set step1_sql %}
        CALL {{ target.database }}.{{ target.schema }}.stateless_create_policy_step(
            PARSE_JSON('{{ tojson(policy_data) }}')
        )
    {% endset %}
    
    {% set step1_result = run_query(step1_sql) %}
    {% if not step1_result or not step1_result.rows or step1_result.rows|length == 0 %}
        {{ log("Stateless Step 1 failed - no result returned", info=True) }}
        {% do return("Stateless Step 1 failed") %}
    {% endif %}
    
    {# Parse Step 1 result #}
    {% set step1_response_raw = step1_result.rows[0][0] %}
    {% set step1_response = fromjson(step1_response_raw) %}
    
    {% if not step1_response or not step1_response.get('SUCCESS') %}
        {% set error_msg = step1_response.get('ERROR_MESSAGE', 'unknown error') if step1_response else 'Step 1 failed' %}
        {{ log("Stateless Step 1 failed - " ~ error_msg, info=True) }}
        {% do return("Stateless Step 1 failed") %}
    {% endif %}
    
    {% set execution_id = step1_response.get('EXECUTION_ID', 'unknown') %}
    {% set policy_id = step1_response.get('POLICY_ID', 'unknown') %}
    {{ log("Stateless Step 1 completed - execution_id: " ~ execution_id ~ ", policy_id: " ~ policy_id, info=True) }}
    
    {# Step 2: Poll Policy Status (Stateless) #}
    {{ log("Executing Stateless Step 2: Poll Policy Status", info=True) }}
    {% set step2_sql %}
        CALL {{ target.database }}.{{ target.schema }}.stateless_poll_policy_status_step('{{ policy_id }}')
    {% endset %}
    
    {% set step2_result = run_query(step2_sql) %}
    {% if not step2_result or not step2_result.rows or step2_result.rows|length == 0 %}
        {{ log("Stateless Step 2 failed - no result returned", info=True) }}
        {% do return("Stateless Step 2 failed") %}
    {% endif %}
    
    {# Parse Step 2 result #}
    {% set step2_response_raw = step2_result.rows[0][0] %}
    {% set step2_response = fromjson(step2_response_raw) %}
    {% if not step2_response or not step2_response.get('SUCCESS') %}
        {% set error_msg = step2_response.get('ERROR_MESSAGE', 'unknown error') if step2_response else 'Step 2 failed' %}
        {{ log("Stateless Step 2 failed - " ~ error_msg, info=True) }}
        {% do return("Stateless Step 2 failed") %}
    {% endif %}
    
    {{ log("Stateless Step 2 completed - Policy is ready for circuit breaker", info=True) }}
    
    {# Step 3: Trigger Circuit Breaker (Stateless) #}
    {{ log("Executing Stateless Step 3: Trigger Circuit Breaker", info=True) }}
    {% set step3_sql %}
        CALL {{ target.database }}.{{ target.schema }}.stateless_trigger_circuit_breaker_step('{{ execution_id }}', '{{ policy_id }}')
    {% endset %}
    
    {% set step3_result = run_query(step3_sql) %}
    {% if not step3_result or not step3_result.rows or step3_result.rows|length == 0 %}
        {{ log("Stateless Step 3 failed - no result returned", info=True) }}
        {% do return("Stateless Step 3 failed") %}
    {% endif %}
    
    {# Parse Step 3 result #}
    {% set step3_response_raw = step3_result.rows[0][0] %}
    {% set step3_response = fromjson(step3_response_raw) %}
    {% if not step3_response or not step3_response.get('SUCCESS') %}
        {% set error_msg = step3_response.get('ERROR_MESSAGE', 'unknown error') if step3_response else 'Step 3 failed' %}
        {{ log("Stateless Step 3 failed - " ~ error_msg, info=True) }}
        {% do return("Stateless Step 3 failed") %}
    {% endif %}
    
    {{ log("Stateless Step 3 completed - All steps successful!", info=True) }}
    {{ log("Stateless Execution ID: " ~ execution_id ~ " completed successfully (no state persisted)", info=True) }}
    
    {% do return("Stateless workflow completed successfully - no state persisted") %}
{% endmacro %} 