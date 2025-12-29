-- sql/views/vw_kpi_by_state.sql
CREATE OR REPLACE VIEW analytics.vw_kpi_by_state AS
SELECT
  month,
  customer_state,
  revenue,
  orders
FROM analytics.kpi_by_state;
