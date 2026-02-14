{% macro get_metrics_models() %}
  {% set prefix = get_config_value('metrics_model_prefix', 'metrics_') %}
  {% set exclude_list = get_config_value('exclude_models', []) %}
  
  {% set metric_models = [] %}
  
  {# Check if graph and nodes are available #}
  {% if graph is defined and graph.nodes is defined %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' 
         and node.name.startswith(prefix) 
         and node.name not in exclude_list 
         and node.name != 'alerts_processor' %}
        {% set _ = metric_models.append(node.name) %}
      {% endif %}
    {% endfor %}
    
    {{ log("Auto-discovered metrics models: " ~ metric_models | join(", "), info=true) }}
  {% else %}
    {# Fallback to hardcoded list if graph not available #}
    {% set metric_models = ['metrics_test_revenue'] %}
    {{ log("Graph not available, using fallback metrics models: " ~ metric_models | join(", "), info=true) }}
  {% endif %}
  
  {{ return(metric_models) }}
{% endmacro %}