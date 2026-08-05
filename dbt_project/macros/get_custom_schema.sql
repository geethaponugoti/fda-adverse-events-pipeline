{#
  raw.cms_prescribers is loaded into pre-created `staging` and `mart`
  schemas (see scripts/init_db.sql). dbt's default generate_schema_name
  macro would otherwise concatenate the target schema with the custom
  schema (e.g. "staging_mart"), so this override makes a model's custom
  `schema:` config the literal destination schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
