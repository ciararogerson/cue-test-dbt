{{
  config(
    materialized = 'incremental',
    partition_by = {
      'field': 'metric_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    unique_key = 'metric_name'
  )
}}

-- Minimal metric model for E2E testing.
-- Compares current month revenue to previous month.

WITH current_month AS (
  SELECT
    SUM(amount) AS total_revenue,
    MAX(sale_date) AS latest_date
  FROM {{ source('client_data', 'test_sales') }}
  WHERE sale_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
),

previous_month AS (
  SELECT SUM(amount) AS total_revenue
  FROM {{ source('client_data', 'test_sales') }}
  WHERE sale_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
    AND sale_date < DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
)

SELECT
  'test_revenue_mom' AS metric_name,
  CURRENT_DATE() AS metric_date,
  ROUND(SAFE_DIVIDE(c.total_revenue - p.total_revenue, p.total_revenue) * 100, 2) AS alert_value,
  'percentage_change' AS alert_type,
  TO_JSON_STRING(STRUCT(
    c.total_revenue AS current_value,
    p.total_revenue AS previous_value
  )) AS metric_data,
  'Test revenue month-on-month change' AS description
FROM current_month c
CROSS JOIN previous_month p
