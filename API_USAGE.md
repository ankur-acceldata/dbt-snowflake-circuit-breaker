# API Usage Guide - Circuit Breaker Integration

## Overview
This dbt project integrates with external APIs to implement a **three-step policy execution workflow**:
1. **Create Policy**: Call `POST https://dbttest.free.beeceptor.com/createpolicy` 
2. **Poll Policy Status**: Call `GET https://dbttest.free.beeceptor.com/policy/{policy_id}` (up to 3 retries)
3. **Trigger Circuit Breaker**: Call `POST https://dbttest.free.beeceptor.com/circuit` when policy is ready

## 🚀 Quick Start

### Execute the Complete Workflow
```bash
# Run the complete three-step workflow
dbt run-operation run_policy_workflow
```

### Check Execution Results
```sql
-- View recent executions
SELECT 
    "execution_id",
    "policy_id", 
    "status",
    "poll_count",
    "circuit_breaker_completed",
    "message",
    "start_time"
FROM policy_execution_state 
ORDER BY "start_time" DESC 
LIMIT 10;
```

## 🔧 Architecture

### Components
1. **Stored Procedure**: `execute_policy_workflow()` - Executes the complete API workflow
2. **State Table**: `policy_execution_state` - Tracks execution progress with full audit trail
3. **Operation**: `run_policy_workflow` - Manually triggers workflow execution
4. **Diagnostic Tools**: `diagnose_policy_execution` - System health checks

### State Table Schema
```sql
CREATE TABLE policy_execution_state (
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
```

## 📡 API Endpoints

### 1. Create Policy
**Endpoint**: `POST https://dbttest.free.beeceptor.com/createpolicy`
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
    "expected_policy_id": "744df8ed-a494-4b2e-ae19-f96ae7bfff45",
    "parameters": {
        "retention_days": 90,
        "classification": "sensitive",
        "compliance_level": "strict",
        "circuit_breaker_enabled": true
    },
    "timestamp": "2024-01-01T12:00:00"
}
```

**Expected Response**:
```json
{
    "policy_id": "744df8ed-a494-4b2e-ae19-f96ae7bfff45",
    "execution_id": "exec_20240101_120000",
    "status": "created",
    "message": "Policy created successfully"
}
```

**Error Handling**:
- ✅ Graceful JSON parsing with fallback responses
- ✅ HTTP error handling with detailed error messages
- ✅ Empty response handling with auto-generated policy IDs

### 2. Poll Policy Status
**Endpoint**: `GET https://dbttest.free.beeceptor.com/policy/{policy_id}`
**Request Headers**:
- `Accept: application/json`

**Expected Response**:
```json
{
    "policy_id": "744df8ed-a494-4b2e-ae19-f96ae7bfff45",
    "status": "success",
    "message": "Policy is ready for circuit breaker"
}
```

**Status Values**:
- ✅ **Success States**: `success`, `completed`, `active`, `ready` → Triggers circuit breaker
- ⏳ **Processing States**: `pending`, `processing` → Continues polling
- ❌ **Error States**: `failed`, `error` → Stops workflow

**Retry Logic**:
- **Max Attempts**: 3 retries
- **Delay**: 2 seconds between attempts
- **Validation**: Checks both API response AND database state
- **Fallback**: Creates meaningful fallback responses for non-JSON responses

### 3. Trigger Circuit Breaker
**Endpoint**: `POST https://dbttest.free.beeceptor.com/circuit`
**Request Headers**:
- `Content-Type: application/json`
- `Accept: application/json`

**Request**:
```json
{
    "policy_id": "744df8ed-a494-4b2e-ae19-f96ae7bfff45",
    "execution_id": "exec_20240101_120000",
    "trigger_reason": "policy_execution_completed_successfully",
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

**Error Handling**:
- ✅ Graceful handling of empty or non-JSON responses
- ✅ Creates success fallback when JSON parsing fails
- ✅ Captures raw response content for debugging

## 🔄 Workflow Process

### Step 1: Policy Creation
1. Stored procedure calls `POST /createpolicy` with policy data
2. Extracts `policy_id` and `execution_id` from response (with fallback generation)
3. Inserts initial state record with status `policy_created`
4. **Verifies** policy ID storage in database before proceeding
5. Updates with JSON response data separately to avoid SQL compilation errors

### Step 2: Status Polling (Enhanced)
1. **Database Validation**: Checks policy exists in state table
2. **API Call**: Polls `GET /policy/{policy_id}` up to 3 times with 2-second delays
3. **Dual Validation**: Checks both API response AND database state
4. **Success Check**: Looks for success status values (`success`, `completed`, `active`, `ready`)
5. **State Update**: Updates state table with each poll attempt
6. **Verification**: Confirms success status is stored in database
7. **Final Check**: Only proceeds if both API and database confirm readiness

### Step 3: Circuit Breaker Trigger (Robust)
1. **Pre-validation**: Final verification that policy is ready in database
2. **API Call**: Calls `POST /circuit` with policy and execution details
3. **Response Handling**: Gracefully handles JSON and non-JSON responses
4. **State Update**: Updates state table with circuit breaker response
5. **Completion**: Sets final status to `completed` with all flags set

## 🚨 Error Handling

### HTTP Errors
- ✅ **Connection timeouts**: 30 seconds for all API calls
- ✅ **HTTP status errors**: Proper 4xx/5xx error handling with `raise_for_status()`
- ✅ **Invalid JSON responses**: `json.JSONDecodeError` handling with fallback responses
- ✅ **Network connectivity**: Detailed error messages for connection issues

### Enhanced JSON Parsing
```python
# Example of robust JSON parsing
try:
    if response.content:
        api_response = response.json()
    else:
        api_response = {"message": "Empty response", "status": "success"}
except json.JSONDecodeError as e:
    # Fallback response for non-JSON responses
    api_response = {
        "message": "Non-JSON response received", 
        "status": "success",
        "raw_response": response.text[:200],
        "content_type": response.headers.get('Content-Type', 'unknown')
    }
```

### Retry Logic
- **Policy status polling**: 3 attempts with 2-second delays
- **Database validation**: Between each API call
- **State consistency**: Verifies each step completion before proceeding
- **Error logging**: All errors logged in `error_message` field with step information

### State Tracking
- **pending**: Initial state
- **policy_created**: Policy API call successful + verified in database
- **policy_ready**: Policy status polling successful + verified in database
- **completed**: Circuit breaker triggered successfully
- **error**: Any step failed (includes `STEP_FAILED` indicator)

## 📊 Usage Examples

### Basic Execution
```bash
# Execute the complete workflow
dbt run-operation run_policy_workflow

# Check diagnostic information
dbt run-operation diagnose_policy_execution
```

### Advanced Monitoring
```sql
-- View all executions with detailed status
SELECT 
    "execution_id",
    "policy_id",
    "status",
    "poll_count",
    "circuit_breaker_completed",
    "start_time",
    "message",
    "error_message",
    CASE 
        WHEN "status" = 'completed' THEN '✅ Success'
        WHEN "status" = 'error' THEN '❌ Failed'
        ELSE '⏳ In Progress'
    END as status_emoji
FROM policy_execution_state
ORDER BY "start_time" DESC;

-- Success rate analysis
SELECT 
    COUNT(*) as total_executions,
    SUM(CASE WHEN "status" = 'completed' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN "status" = 'error' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN "status" = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_percent
FROM policy_execution_state
WHERE "start_time" >= DATEADD(hour, -24, CURRENT_TIMESTAMP());

-- Error analysis
SELECT 
    "error_message",
    COUNT(*) as error_count,
    MIN("start_time") as first_occurrence,
    MAX("start_time") as last_occurrence
FROM policy_execution_state
WHERE "status" = 'error'
GROUP BY "error_message"
ORDER BY error_count DESC;
```

### Health Checks
```sql
-- Check for stuck executions (running > 1 hour)
SELECT "execution_id", "status", "start_time", "message"
FROM policy_execution_state
WHERE "completed" = FALSE
AND "start_time" < DATEADD(hour, -1, CURRENT_TIMESTAMP());

-- Check API response patterns
SELECT 
    "status",
    COUNT(*) as count,
    AVG("poll_count") as avg_poll_count
FROM policy_execution_state
GROUP BY "status";
```

### Cleanup Operations
```sql
-- The system automatically cleans up completed records older than 1 hour
-- Manual cleanup for older records if needed:
DELETE FROM policy_execution_state 
WHERE "completed" = TRUE 
AND "start_time" < DATEADD(day, -7, CURRENT_TIMESTAMP());
```

## 🛠 Technical Requirements

### Snowflake Setup
```sql
-- Required: External access integration for API calls
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION API_EXTERNAL_ACCESS
ALLOWED_NETWORK_RULES = ('allow_all_rule')
ENABLED = TRUE;

-- Grant necessary permissions
GRANT CREATE TABLE ON SCHEMA <your_schema> TO ROLE <your_role>;
GRANT CREATE PROCEDURE ON SCHEMA <your_schema> TO ROLE <your_role>;
GRANT USAGE ON INTEGRATION API_EXTERNAL_ACCESS TO ROLE <your_role>;
```

### Python Packages
- ✅ `requests` package (HTTP client)
- ✅ `snowflake-snowpark-python` package (Snowflake integration)
- ✅ Python 3.9 runtime
- ✅ `json` and `datetime` modules (built-in)

### API Server Requirements
- ✅ **Endpoint**: `https://dbttest.free.beeceptor.com`
- ✅ **HTTPS**: TLS/SSL support enabled
- ✅ **JSON**: Request/response format support
- ✅ **CORS**: Cross-origin requests allowed
- ✅ **Reliability**: Handles various response formats gracefully

## 🔍 Troubleshooting

### Common Issues & Solutions

1. **"API_RESPONSE invalid identifier"**
   - ✅ **Fixed**: Now uses quoted column names for Snowflake case sensitivity
   - ✅ **Solution**: All column names use double quotes: `"api_response"`

2. **"JSON parsing error"**
   - ✅ **Fixed**: Graceful handling of non-JSON responses
   - ✅ **Solution**: Fallback responses with meaningful data

3. **"STEP X FAILED"**
   - ✅ **Check**: `"error_message"` column in `policy_execution_state`
   - ✅ **Verify**: API endpoints and network connectivity
   - ✅ **Confirm**: External access integration permissions

4. **"SQL compilation error"**
   - ✅ **Fixed**: Separate INSERT and UPDATE for JSON data
   - ✅ **Solution**: Avoids `PARSE_JSON` in VALUES clause

### Debug Commands
```bash
# Check system status
dbt run-operation diagnose_policy_execution

# View detailed logs
tail -f logs/dbt.log

# Test API endpoints manually
curl -X POST https://dbttest.free.beeceptor.com/createpolicy \
  -H "Content-Type: application/json" \
  -d '{"policy_name": "test_policy"}'

curl -X GET https://dbttest.free.beeceptor.com/policy/test_policy_id

curl -X POST https://dbttest.free.beeceptor.com/circuit \
  -H "Content-Type: application/json" \
  -d '{"policy_id": "test_policy_id", "execution_id": "test_exec_id"}'
```

### Performance Monitoring
```sql
-- Monitor execution times
SELECT 
    "execution_id",
    "start_time",
    "last_poll_time",
    DATEDIFF(second, "start_time", "last_poll_time") as execution_duration_seconds
FROM policy_execution_state
WHERE "completed" = TRUE
ORDER BY execution_duration_seconds DESC;
```

## 🎯 Best Practices

### 1. Execution Monitoring
- ✅ Monitor success rates regularly
- ✅ Investigate failed executions promptly
- ✅ Set up alerts for high failure rates
- ✅ Review performance metrics weekly

### 2. Error Handling
- ✅ Check logs for detailed error information
- ✅ Verify API endpoint availability
- ✅ Monitor network connectivity
- ✅ Test with sample data first

### 3. State Management
- ✅ Don't manually delete in-progress records
- ✅ Use diagnostic operations for troubleshooting
- ✅ Let the system handle automatic cleanup
- ✅ Archive old successful executions if needed

### 4. API Integration
- ✅ Test API endpoints independently
- ✅ Monitor API response patterns
- ✅ Handle various response formats
- ✅ Implement proper timeout values

## 🚀 Advanced Features

### Custom Policy Data
To customize the policy data, modify the `run_policy_workflow` macro:
```sql
{% set policy_data = {
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
} %}
```

### CI/CD Integration
```bash
# Example CI/CD pipeline step
echo "Starting policy workflow..."
dbt run-operation run_policy_workflow

if [ $? -eq 0 ]; then
    echo "✅ Policy workflow completed successfully"
    # Check final status
    dbt run-operation diagnose_policy_execution
else
    echo "❌ Policy workflow failed"
    exit 1
fi
```

### Batch Processing
```sql
-- For multiple policy executions, consider:
-- 1. Separate execution_id for each policy
-- 2. Monitor state table for concurrent executions
-- 3. Implement queuing if needed
```

## 📈 Success Metrics

A successful workflow execution will show:
- ✅ **Step 1**: Policy created and stored in state table
- ✅ **Step 2**: Policy status confirmed as success/ready (with retries if needed)
- ✅ **Step 3**: Circuit breaker triggered successfully
- ✅ **Final State**: `"status" = 'completed'` with `"circuit_breaker_completed" = TRUE`
- ✅ **Audit Trail**: Complete history of all API calls and responses stored in JSON fields 