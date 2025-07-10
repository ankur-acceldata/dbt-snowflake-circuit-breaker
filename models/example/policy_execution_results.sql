{{
    config(
        materialized='view',
        tags=['policy_execution']
    )
}}

-- Show policy execution results
SELECT *
FROM {{ ref('policy_execution_model') }} 