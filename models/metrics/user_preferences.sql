-- =============================================================================
-- USER PREFERENCES: Alert configuration per user per metric (E2E test)
-- =============================================================================

SELECT * FROM UNNEST([
  STRUCT(
    'test_user_001' AS uid,
    'test_revenue_mom' AS metric_name,
    'daily' AS frequency,
    'slack' AS channels,
    'medium' AS priority,
    TRUE AS active,
    CAST(NULL AS FLOAT64) AS threshold,
    CAST(NULL AS STRING) AS direction
  )
])
