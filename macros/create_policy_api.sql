{% macro create_policy_api(policy_data={}) %}
    {% set create_policy_udf_sql %}
        CREATE OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.create_policy_api(policy_data VARIANT)
        RETURNS VARIANT
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.9'
        HANDLER = 'create_policy_api'
        PACKAGES = ('requests')
        EXTERNAL_ACCESS_INTEGRATIONS = (API_EXTERNAL_ACCESS)
        AS
        $$
import requests
import json
from datetime import datetime

def create_policy_api(policy_data):
    """Call the createpolicy API endpoint."""
    try:
        # API endpoint
        url = "https://dbttest.free.beeceptor.com/createpolicy"
        
        # Convert Snowflake VARIANT to Python dict if needed
        if isinstance(policy_data, str):
            payload = json.loads(policy_data)
        else:
            payload = policy_data
            
        # Add timestamp if not present
        if 'timestamp' not in payload:
            payload['timestamp'] = datetime.now().isoformat()
            
        # Make the API call
        response = requests.post(
            url,
            json=payload,
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            timeout=30
        )
        
        # Check if request was successful
        response.raise_for_status()
        
        # Return the response
        result = {
            'success': True,
            'status_code': response.status_code,
            'response_data': response.json() if response.content else {},
            'execution_id': response.json().get('execution_id', f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}") if response.content else f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'timestamp': datetime.now().isoformat()
        }
        
        return result
        
    except requests.exceptions.RequestException as e:
        # Handle API errors
        return {
            'success': False,
            'error': str(e),
            'error_type': 'API_ERROR',
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        # Handle other errors
        return {
            'success': False,
            'error': str(e),
            'error_type': 'GENERAL_ERROR',
            'timestamp': datetime.now().isoformat()
        }
$$
    {% endset %}
    
    {% do run_query(create_policy_udf_sql) %}
    
    {% set call_api_sql %}
        SELECT {{ target.database }}.{{ target.schema }}.create_policy_api(
            PARSE_JSON('{{ tojson(policy_data) }}')
        ) as api_response
    {% endset %}
    
    {% do return(call_api_sql) %}
{% endmacro %} 