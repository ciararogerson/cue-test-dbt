{% set alerting_config = var('metrics_alerting', {}) %}

{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'metric_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    unique_key = 'alert_id'
  )
}}

-- =============================================================================
-- 1. COLLECT ALL METRICS FROM all_metrics MODEL
-- The all_metrics model unions all metrics_* models into a single table
-- =============================================================================
WITH all_metrics AS (
  SELECT * FROM {{ ref('all_metrics') }}
  {% if is_incremental() %}
    WHERE metric_date = CURRENT_DATE()
  {% endif %}
),

-- =============================================================================
-- 2. USER PREFERENCES (uid only - no PII in BigQuery for GDPR compliance)
-- Delivery details (slack_user_id, timezone, etc.) looked up from Firestore
-- =============================================================================
active_preferences AS (
  SELECT 
    uid,
    metric_name,
    threshold,
    direction,
    frequency,
    channels,
    priority,
    -- Determine if should send today based on frequency
    CASE
      WHEN frequency = 'daily' THEN true
      WHEN frequency = 'weekday' 
        AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) BETWEEN 2 AND 6 THEN true
      WHEN frequency = 'weekly' 
        AND EXTRACT(DAYOFWEEK FROM CURRENT_DATE()) = {{ alerting_config.get('weekly_run_day', 2) }} THEN true
      WHEN frequency = 'monthly' 
        AND EXTRACT(DAY FROM CURRENT_DATE()) = {{ alerting_config.get('monthly_run_day', 5) }} THEN true
      ELSE false
    END AS should_send_today
  FROM {{ ref('user_preferences') }}
  WHERE active = true
),

-- =============================================================================
-- 3. JOIN METRICS TO USER PREFERENCES
-- =============================================================================
metrics_with_preferences AS (
  SELECT 
    m.source_model,
    m.metric_name,
    m.metric_date,
    m.alert_value,
    m.alert_type,
    m.metric_data,
    m.description,
    p.uid,
    p.frequency,
    p.channels,
    p.priority,
    p.threshold,
    p.direction
  FROM all_metrics m
  INNER JOIN active_preferences p 
    ON m.metric_name = p.metric_name
    AND p.should_send_today = true
),

-- =============================================================================
-- 4. EVALUATE ALERT CONDITIONS BY TYPE
-- =============================================================================
alerts_evaluated AS (
  SELECT 
    *,
    CASE
      -- PERCENTAGE_CHANGE: alert_value is the % change (can be negative)
      -- User sets threshold as absolute %, direction controls sign
      WHEN alert_type = 'percentage_change' THEN
        CASE
          WHEN threshold IS NULL THEN true  -- No threshold = always alert
          WHEN COALESCE(direction, 'both') = 'both' 
            AND ABS(alert_value) >= threshold THEN true
          WHEN direction = 'increase' 
            AND alert_value >= threshold THEN true
          WHEN direction = 'decrease' 
            AND alert_value <= -threshold THEN true
          ELSE false
        END
      
      -- ABSOLUTE: alert_value is actual value, compare to threshold
      -- Direction: 'above' or 'below'
      WHEN alert_type = 'absolute' THEN
        CASE
          WHEN threshold IS NULL THEN true
          WHEN COALESCE(direction, 'below') = 'below' 
            AND alert_value <= threshold THEN true
          WHEN direction = 'above' 
            AND alert_value >= threshold THEN true
          ELSE false
        END
      
      -- COUNT: alert_value is a count, compare to threshold
      -- Direction: 'above' or 'below'
      WHEN alert_type = 'count' THEN
        CASE
          WHEN threshold IS NULL THEN true
          WHEN COALESCE(direction, 'above') = 'above' 
            AND alert_value >= threshold THEN true
          WHEN direction = 'below' 
            AND alert_value <= threshold THEN true
          ELSE false
        END
      
      -- BOOLEAN: alert when value is false/0 (unhealthy state)
      -- No threshold needed - treats 0 as false, non-zero as true
      WHEN alert_type = 'boolean' THEN
        CASE
          WHEN alert_value = 0 OR alert_value IS NULL THEN true
          ELSE false
        END
      
      -- UNKNOWN TYPE: default to sending alert
      ELSE true
    END AS should_alert
  FROM metrics_with_preferences
)

-- =============================================================================
-- 5. OUTPUT: Alert data for external service
-- Service looks up user delivery details (slack_user_id, timezone) from Firestore
-- =============================================================================
SELECT
  -- Alert identification
  FORMAT('%x', ABS(FARM_FINGERPRINT(
    CONCAT(metric_name, '_', CAST(CURRENT_TIMESTAMP() AS STRING), '_', uid)
  ))) AS alert_id,
  
  -- Timing
  metric_date,
  CURRENT_TIMESTAMP() AS generated_at,
  
  -- Recipient (uid only - service looks up delivery details from Firestore)
  uid,
  
  -- Delivery preferences (from user_preferences, not PII)
  frequency,
  channels,
  priority,
  
  -- Metric info for rendering
  metric_name,
  source_model,
  description,
  
  -- Alert details (service uses these to render message)
  alert_type,
  alert_value,
  threshold,
  direction,
  
  -- Full context (JSON blob with anything else)
  metric_data

FROM alerts_evaluated
WHERE should_alert = true

{% if is_incremental() %}
  AND metric_date = CURRENT_DATE()
{% endif %}
