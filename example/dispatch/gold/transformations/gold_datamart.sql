CREATE OR REFRESH MATERIALIZED VIEW dispatch_gold_datamart
AS SELECT
  c.customer_id,
  c.name,
  c.email,
  COUNT(o.order_id) AS total_orders,
  SUM(o.total_amount) AS lifetime_value,
  MAX(o.order_date) AS last_order_date
FROM dispatch_silver_customers c
LEFT JOIN dispatch_silver_orders o
  ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
