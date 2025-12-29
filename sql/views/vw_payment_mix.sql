-- sql/views/vw_payment_mix.sql
CREATE OR REPLACE VIEW analytics.vw_payment_mix AS
SELECT
  month,
  payment_type,
  total_payment_value,
  share
FROM analytics.payment_mix;
