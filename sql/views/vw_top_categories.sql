CREATE OR REPLACE VIEW analytics.vw_top_categories AS
SELECT
  t.month,
  t.product_category_name,
  COALESCE(d.category_en, t.product_category_name) AS product_category_en,
  t.revenue,
  t.orders,
  t.avg_price,
  t.avg_freight,
  t.revenue_rank
FROM analytics.top_categories t
LEFT JOIN analytics.dim_product_category d
  ON d.category_pt = t.product_category_name;