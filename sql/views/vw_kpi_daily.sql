CREATE OR REPLACE VIEW analytics.vw_kpi_daily AS
SELECT
  date,
  revenue,
  orders,
  aov
FROM gold.kpi_daily;
