CREATE OR REPLACE VIEW analytics.vw_kpi_by_state AS
SELECT
  month,
  customer_state,
  revenue,
  orders
FROM gold.kpi_by_state;
