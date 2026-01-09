CREATE OR REPLACE VIEW analytics.vw_pbi_unified AS
WITH base AS (
  SELECT
    f.date,
    f.month_date,
    f.customer_state,
    f.payment_type,
    COALESCE(d.category_en, f.product_category_name) AS product_category_en,
    f.revenue,
    f.orders,
    f.total_payment_value,
    f.avg_price,
    f.avg_freight,
    f.batch_id,
    f.updated_at
  FROM analytics.pbi_fact_daily f
  LEFT JOIN analytics.dim_product_category d
    ON d.category_pt = f.product_category_name
),

monthly AS (
  SELECT
    month_date,
    customer_state,
    payment_type,
    product_category_en,
    SUM(revenue) AS revenue_m,
    SUM(orders) AS orders_m,
    SUM(total_payment_value) AS payment_m
  FROM base
  GROUP BY 1,2,3,4
),

mom AS (
  SELECT
    *,
    LAG(revenue_m) OVER (PARTITION BY customer_state, payment_type, product_category_en ORDER BY month_date) AS rev_prev,
    LAG(orders_m)  OVER (PARTITION BY customer_state, payment_type, product_category_en ORDER BY month_date) AS ord_prev
  FROM monthly
)

SELECT
  b.date,
  b.month_date,
  b.customer_state,
  b.payment_type,
  b.product_category_en,
  b.revenue,
  b.orders,
  b.total_payment_value,
  b.avg_price,
  b.avg_freight,

  CASE
    WHEN m.rev_prev IS NULL OR m.rev_prev = 0 THEN NULL
    ELSE (m.revenue_m - m.rev_prev) / m.rev_prev
  END AS mom_revenue_growth,

  CASE
    WHEN m.ord_prev IS NULL OR m.ord_prev = 0 THEN NULL
    ELSE (m.orders_m - m.ord_prev) / m.ord_prev
  END AS mom_orders_growth,

  CASE
    WHEN SUM(b.total_payment_value) OVER (PARTITION BY b.month_date) = 0 THEN NULL
    ELSE b.total_payment_value
       / SUM(b.total_payment_value) OVER (PARTITION BY b.month_date)
  END AS payment_share,

  b.batch_id,
  b.updated_at

FROM base b
LEFT JOIN mom m
  ON  b.month_date = m.month_date
  AND b.customer_state = m.customer_state
  AND b.payment_type = m.payment_type
  AND b.product_category_en = m.product_category_en;
