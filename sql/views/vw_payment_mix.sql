CREATE OR REPLACE VIEW analytics.vw_payment_mix AS
SELECT
  month,
  payment_type,
  total_payment_value,
  share
FROM gold.payment_mix;
