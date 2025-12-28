CREATE OR REPLACE VIEW analytics.vw_kpi_monthly AS
WITH base AS (
  SELECT month, revenue, orders, aov
  FROM gold.kpi_monthly
),
mom AS (
  SELECT
    month,
    revenue,
    orders,
    aov,
    LAG(revenue) OVER (ORDER BY month) AS revenue_prev,
    LAG(orders)  OVER (ORDER BY month) AS orders_prev
  FROM base
)
SELECT
  month,
  revenue,
  orders,
  aov,
  CASE WHEN revenue_prev IS NULL OR revenue_prev = 0 THEN NULL
       ELSE (revenue - revenue_prev) / revenue_prev
  END AS mom_revenue_growth,
  CASE WHEN orders_prev IS NULL OR orders_prev = 0 THEN NULL
       ELSE (orders - orders_prev) / orders_prev
  END AS mom_orders_growth
FROM mom;
