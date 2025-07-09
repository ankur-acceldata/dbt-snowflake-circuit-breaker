{% macro send_http_request(url, method='POST', payload={}, second_url=none) %}
    {% set query %}
    DECLARE
        http_response VARIANT;
        response_status NUMBER;
    BEGIN
        -- Make the first HTTP request
        http_response := SYSTEM$HTTPREQUEST(
            '{{ url }}',
            '{{ method }}',
            PARSE_JSON('{{ tojson(payload) }}'),
            PARSE_JSON('{
                "Content-Type": "application/json"
            }')
        );
        
        -- Extract status code from response
        response_status := http_response:statusCode::NUMBER;
        
        -- If second URL is provided and first call was successful (2xx status)
        {% if second_url %}
        IF (response_status >= 200 AND response_status < 300) THEN
            -- Make the second HTTP request with the response from first call
            SYSTEM$HTTPREQUEST(
                '{{ second_url }}',
                '{{ method }}',
                OBJECT_CONSTRUCT(
                    'first_response', http_response,
                    'model_info', PARSE_JSON('{{ tojson(payload) }}')
                ),
                PARSE_JSON('{
                    "Content-Type": "application/json"
                }')
            );
        END IF;
        {% endif %}
        
        -- Return the response from first call
        RETURN http_response;
    END;
    {% endset %}
    
    {% do run_query(query) %}
{% endmacro %} 