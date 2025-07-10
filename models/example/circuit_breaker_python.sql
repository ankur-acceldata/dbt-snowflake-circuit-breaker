{{ config(
    materialized='table'
) }}

{{ circuit_breaker_python() }} 