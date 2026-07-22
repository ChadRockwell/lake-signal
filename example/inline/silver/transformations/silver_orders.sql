CREATE OR REFRESH MATERIALIZED VIEW inline_silver_orders
AS SELECT
  order_id,
  customer_id,
  order_date,
  CAST(total_amount AS DECIMAL(10, 2)) AS total_amount,
  status,
  updated_at
FROM bronze_orders
