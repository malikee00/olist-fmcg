-- sql/views/vw_top_categories.sql
CREATE OR REPLACE VIEW analytics.vw_top_categories AS
SELECT
  month,
  product_category_name,
  revenue,
  orders,
  avg_price,
  avg_freight,
  revenue_rank
FROM analytics.top_categories;
