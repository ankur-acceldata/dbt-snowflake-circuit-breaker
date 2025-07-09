
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(
    materialized='table',
    database='DBT_TEST',
    schema='DBT_DEMO',
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
) }}

with source_data as (
    select 1 as id, 'US' as country, 'Retail' as class, 'New York Store' as name
    union all
    select 2 as id, 'CA' as country, 'Online' as class, 'Toronto Store' as name
    union all
    select 3 as id, 'UK' as country, 'Retail' as class, 'London Store' as name
)

select *
from source_data
