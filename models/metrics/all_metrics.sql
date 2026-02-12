-- =============================================================================
-- ALL METRICS: Aggregated view of all metrics from individual metric models
-- =============================================================================

{{
  config(
    materialized = 'table'
  )
}}

{% set metric_models = get_metrics_models() %}

{% for model in metric_models %}
  SELECT 
    '{{ model }}' AS source_model,
    metric_name,
    metric_date,
    alert_value,
    alert_type,
    metric_data,
    description
  FROM {{ ref(model) }}
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
