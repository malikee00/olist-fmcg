CREATE OR REPLACE VIEW analytics.vw_top_categories AS
SELECT
  month,
  product_category_name,
  revenue,
  orders,
  avg_price,
  avg_freight,
  RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS revenue_rank
FROM gold.top_products;
