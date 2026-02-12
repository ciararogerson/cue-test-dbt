{% macro get_config_value(key, default_value) %}
  {% set alerting_config = var('metrics_alerting', {}) %}
  {{ return(alerting_config.get(key, default_value)) }}
{% endmacro %}