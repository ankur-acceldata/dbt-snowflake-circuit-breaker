# API Usage Guide - Circuit Breaker Integration

## Overview
This dbt project integrates with external APIs to implement a three-step policy execution workflow:
1. **Create Policy**: Call `POST https://httpbin.org/post` (simulating policy creation)
2. **Poll Policy Status**: Call `GET https://httpbin.org/get` (simulating status checks, up to 3 retries)
3. **Trigger Circuit Breaker**: Call `POST https://httpbin.org/post` (simulating circuit breaker trigger) when policy is ready

## Quick Start

### Execute the Workflow
```bash
# Run the complete three-step workflow
dbt run-operation run_policy_workflow

# View the results
dbt run --select policy_execution_model
```

### Check State Table
```bash
# View execution state details
dbt run-operation run_query --args '{"sql": "SELECT * FROM policy_execution_state ORDER BY start_time DESC LIMIT 1"}'
```

## Architecture

### Components
1. **Stored Procedure**: `execute_policy_workflow()` - Executes the complete API workflow
2. **State Table**: `policy_execution_state` - Tracks execution progress and results
3. **Model**: `policy_execution_model` - Shows execution results
4. **Operation**: `run_policy_workflow` - Manually triggers workflow execution

### State Table Schema
```sql
CREATE TABLE policy_execution_state (
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
```

## API Endpoints

### 1. Create Policy
**Endpoint**: `POST https://httpbin.org/post` (Test endpoint simulating policy creation)
**Request Headers**:
- `Content-Type: application/json`
- `Accept: application/json`

**Sample Request**:
```json
{
    "policy_name": "sample_policy",
    "policy_type": "data_governance",
    "description": "Sample policy for data governance with circuit breaker",
    "priority": "high",
    "created_by": "dbt_user",
    "expected_policy_id": "d74d726c-9ca0-4c96-b9e3-755396547dce",
    "parameters": {
        "retention_days": 90,
        "classification": "sensitive",
        "compliance_level": "strict",
        "circuit_breaker_enabled": true
    }
}
```

**Expected Response**:
```json
{
    "policy_id": "d74d726c-9ca0-4c96-b9e3-755396547dce",
    "execution_id": "exec_20240101_120000",
    "status": "created",
    "message": "Policy created successfully"
}
```

### 2. Poll Policy Status
**Endpoint**: `GET https://httpbin.org/get` (Test endpoint simulating status checks)
**Request Headers**:
- `Accept: application/json`

**Expected Response**:
```json
{
    "policy_id": "d74d726c-9ca0-4c96-b9e3-755396547dce",
    "status": "success",
    "message": "Policy is ready for circuit breaker"
}
```

**Status Values**:
- `success` / `completed` / `active` / `ready`: Policy is ready for circuit breaker
- `pending` / `processing`: Policy is still being processed
- `failed` / `error`: Policy creation failed

### 3. Trigger Circuit Breaker
**Endpoint**: `POST https://httpbin.org/post` (Test endpoint simulating circuit breaker trigger)
**Request Headers**:
- `Content-Type: application/json`
- `Accept: application/json`

**Request**:
```json
{
    "policy_id": "d74d726c-9ca0-4c96-b9e3-755396547dce",
    "execution_id": "exec_20240101_120000",
    "trigger_reason": "policy_execution_completed",
    "timestamp": "2024-01-01T12:00:00"
}
```

**Expected Response**:
```json
{
    "circuit_breaker_id": "cb_20240101_120000",
    "status": "initiated",
    "message": "Circuit breaker initiated successfully"
}
```

## Workflow Process

### Step 1: Policy Creation
1. Stored procedure calls `POST /createpolicy` with policy data
2. Extracts `policy_id` and `execution_id` from response
3. Inserts initial state record with status `policy_created`

### Step 2: Status Polling
1. Polls `GET /policy/{policy_id}` up to 3 times with 2-second delays
2. Checks for success status values (`success`, `completed`, `active`, `ready`)
3. Updates state table with each poll attempt
4. Sets `requires_circuit_breaker = TRUE` when policy is ready

### Step 3: Circuit Breaker Trigger
1. Calls `POST /circuit` with policy and execution details
2. Updates state table with circuit breaker response
3. Sets final status to `completed` with all flags set

## Error Handling

### HTTP Errors
- Connection timeouts (30 seconds)
- HTTP status errors (4xx, 5xx)
- Invalid JSON responses
- Network connectivity issues

### Retry Logic
- Policy status polling: 3 attempts with 2-second delays
- Other API calls: Single attempt with timeout
- All errors logged in `error_message` field

### State Tracking
- **pending**: Initial state
- **policy_created**: Policy API call successful
- **policy_ready**: Policy status polling successful
- **completed**: Circuit breaker triggered successfully
- **error**: Any step failed

## Usage Examples

### Custom Policy Data
```sql
-- Run with custom policy data
{{ policy_execution({
    "policy_name": "custom_policy",
    "policy_type": "security", 
    "description": "Custom security policy",
    "priority": "critical",
    "created_by": "security_team",
    "parameters": {
        "retention_days": 365,
        "classification": "confidential",
        "compliance_level": "strict",
        "circuit_breaker_enabled": true
    }
}) }}
```

### Monitoring Executions
```sql
-- View all executions
SELECT 
    execution_id,
    policy_id,
    status,
    poll_count,
    circuit_breaker_completed,
    start_time,
    message,
    error_message
FROM policy_execution_state
ORDER BY start_time DESC;

-- Count executions by status
SELECT 
    status,
    COUNT(*) as count
FROM policy_execution_state
GROUP BY status;
```

### Cleanup Old Executions
```sql
-- Remove completed executions older than 1 hour
DELETE FROM policy_execution_state 
WHERE completed = TRUE 
AND start_time < DATEADD(hour, -1, CURRENT_TIMESTAMP());
```

## Technical Requirements

### Snowflake Setup
- Python UDF support enabled
- `requests` package available
- `snowflake-snowpark-python` package available
- Network access to API endpoints

### API Server Requirements
- Uses `https://httpbin.org` as a reliable test service
- Endpoints: `/post` for creation/circuit breaker, `/get` for status checks
- JSON request/response format
- HTTPS support enabled
- No external server setup required

## Troubleshooting

### Common Issues
1. **Module not found errors**: Ensure Python packages are available in Snowflake
2. **Connection errors**: Check network connectivity to `https://httpbin.org`
3. **SQL compilation errors**: Verify state table exists and has correct schema
4. **Permission errors**: Ensure dbt user has CREATE/INSERT permissions

### Debug Steps
1. Check API server logs for request/response details
2. Query state table for execution progress
3. Review error messages in `error_message` field
4. Verify network connectivity from Snowflake to API server

### Manual Testing
```bash
# Test API endpoints manually
curl -X POST https://httpbin.org/post \
  -H "Content-Type: application/json" \
  -d '{"policy_name": "test_policy"}'

curl -X GET https://httpbin.org/get

curl -X POST https://httpbin.org/post \
  -H "Content-Type: application/json" \
  -d '{"policy_id": "test_policy_id", "execution_id": "test_exec_id"}'
``` 