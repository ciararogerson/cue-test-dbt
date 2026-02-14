{#- 
  Cue Configuration Loader
  Reads federated query settings from dbt_project.yml vars
  
  Configuration is stored in vars.cue_source_configs
  Maps source names to external database connections for federated queries.
-#}

{% macro cue_get_source_config(source_name) %}
    {#- Get configuration for a specific source from vars -#}
    {% set source_configs = var('cue_source_configs', {}) %}
    {{ return(source_configs.get(source_name, {})) }}
{% endmacro %}
