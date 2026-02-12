{#-
  Cue Source Override
  Intercepts DBT's source() macro to enable configurable source references.
  
  Supports two modes based on cue_source_configs:
  
  1. BigQuery sources (connection_type: "bigquery" or no connection_id):
     - Returns direct table reference: `project.dataset.table`
     
  2. External DB sources (connection_type: "external" with connection_id):
     - Returns EXTERNAL_QUERY for Cloud SQL, etc.
  
  Uses DBT's dispatch pattern for clean override without conflicts.
-#}

{% macro default__source(source_name, table_name) -%}
    {%- set source_config = cue_get_source_config(source_name) -%}
    
    {%- if source_config and source_config.get('project') -%}
        {%- set external_project = source_config.get('project') -%}
        {%- set external_dataset = source_config.get('dataset') -%}
        {%- set connection_id = source_config.get('connection_id') -%}
        {%- set connection_type = source_config.get('connection_type', 'bigquery') -%}
        
        {%- if connection_type == 'external' and connection_id -%}
            {#- External database: use EXTERNAL_QUERY -#}
(SELECT * FROM EXTERNAL_QUERY(
    '{{ connection_id }}',
    'SELECT * FROM `{{ external_project }}.{{ external_dataset }}.{{ table_name }}`'
))
        {%- else -%}
            {#- BigQuery source: direct table reference -#}
`{{ external_project }}`.`{{ external_dataset }}`.`{{ table_name }}`
        {%- endif -%}
    {%- else -%}
        {#- Fallback: use standard DBT source behavior -#}
        {{- return(builtins.source(source_name, table_name)) -}}
    {%- endif -%}
{%- endmacro %}


{#- Register override with DBT's dispatch system -#}
{% macro source(source_name, table_name) %}
    {{ return(adapter.dispatch('source', 'dbt')(source_name, table_name)) }}
{% endmacro %}
