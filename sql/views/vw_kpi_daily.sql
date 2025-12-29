-- sql/views/vw_kpi_daily.sql
CREATE OR REPLACE VIEW analytics.vw_kpi_daily AS
SELECT
  date,
  revenue,
  orders,
  aov
FROM analytics.kpi_daily;
