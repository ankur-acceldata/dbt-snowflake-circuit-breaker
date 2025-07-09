# DBT Snowflake Project with HTTP Notifications

This project demonstrates a DBT (Data Build Tool) implementation with Snowflake, featuring HTTP notifications after model execution.

## Prerequisites

- Python 3.12 or higher
- Snowflake account with ACCOUNTADMIN role
- Network access to your HTTP endpoints from Snowflake

## Setup Instructions

### 1. Python Environment Setup

```bash
# Create a Python virtual environment
python -m venv dbt_env

# Activate the virtual environment
# On macOS/Linux:
source dbt_env/bin/activate
# On Windows:
# .\dbt_env\Scripts\activate

# Install required packages
pip install dbt-core dbt-snowflake
```

### 2. Snowflake Configuration

1. Ensure you have a Snowflake account with ACCOUNTADMIN role
2. Grant necessary permissions for HTTP requests:
```sql
GRANT EXECUTE FUNCTION ON FUNCTION SYSTEM$HTTPREQUEST TO ROLE ACCOUNTADMIN;
```

### 3. DBT Configuration

1. Create profiles.yml in ~/.dbt/ directory:
```yaml
test_sf:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account-id
      user: your-username
      password: your-password
      role: ACCOUNTADMIN
      database: DBT_TEST
      warehouse: COMPUTE_WH
      schema: DBT_DEMO
      threads: 1
      client_session_keep_alive: False
      query_tag: dbt_test_sf
```

2. Update the HTTP endpoints in `models/example/my_first_dbt_model.sql`:
```sql
post_hook=[
    "{{ send_http_request(
        url='https://your-first-endpoint.com/webhook',
        method='POST',
        payload={
            'model': this.name,
            'schema': this.schema,
            'database': this.database,
            'status': 'complete'
        },
        second_url='https://your-second-endpoint.com/webhook'
    ) }}"
]
```

## Project Structure

```
test_sf/
├── README.md
├── dbt_project.yml
├── macros/
│   └── http_notification.sql      # HTTP request macro
├── models/
│   └── example/
│       ├── my_first_dbt_model.sql # Example model with HTTP notifications
│       └── schema.yml
```

## Features

### HTTP Notification System

The project includes a custom macro (`http_notification.sql`) that:
- Makes HTTP calls after model execution
- Supports sequential HTTP requests
- Handles responses and conditional execution
- Passes model metadata to external services

### Model Configuration

Models can be configured with:
- Custom database and schema settings
- Post-execution HTTP notifications
- Response handling and conditional logic

## Usage

1. Activate your Python environment:
```bash
source dbt_env/bin/activate  # On macOS/Linux
```

2. Test the connection:
```bash
dbt debug
```

3. Run the models:
```bash
dbt run
```

4. Monitor the execution:
- Check the Snowflake query history for HTTP request status
- Verify the responses in your HTTP endpoints
- Review dbt logs in the `logs/` directory

## HTTP Notification Flow

1. When a model completes execution:
   - First HTTP endpoint is called with model metadata
   - Response is captured and status code is checked
   - If status is 2xx and second URL is configured:
     - Second endpoint is called with first response and model metadata

2. Payload Structure:
   - First Endpoint Receives:
   ```json
   {
       "model": "model_name",
       "schema": "schema_name",
       "database": "database_name",
       "status": "complete"
   }
   ```
   - Second Endpoint Receives:
   ```json
   {
       "first_response": {
           // Complete response from first endpoint
       },
       "model_info": {
           "model": "model_name",
           "schema": "schema_name",
           "database": "database_name",
           "status": "complete"
       }
   }
   ```

## Troubleshooting

1. HTTP Request Issues:
   - Verify Snowflake network access to endpoints
   - Check ACCOUNTADMIN role permissions
   - Review Snowflake query history for detailed error messages

2. DBT Issues:
   - Run `dbt debug` to verify configuration
   - Check logs in `logs/dbt.log`
   - Verify database permissions and connectivity

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## License

This project is licensed under the MIT License.
