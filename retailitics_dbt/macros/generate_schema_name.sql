{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif env_var('DBT_ENV_TYPE', 'DEV') == 'PROD' -%}
        {# In PROD, use custom schema directly #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In DEV, keep behavior of appending to target schema #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %} 