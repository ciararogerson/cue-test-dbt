{% macro get_metrics_config() %}
  {% set alerting_config = var('metrics_alerting', {}) %}
  {{ return(alerting_config) }}
{% endmacro %}