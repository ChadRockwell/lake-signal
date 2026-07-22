CREATE OR REFRESH MATERIALIZED VIEW bronze_orders
AS SELECT
  id AS order_id,
  (id % 1000) AS customer_id,
  DATEADD(DAY, -(id % 90), current_date()) AS order_date,
  ROUND(10.0 + (id % 500) * 1.5, 2) AS total_amount,
  CASE id % 3
    WHEN 0 THEN 'completed'
    WHEN 1 THEN 'shipped'
    ELSE 'pending'
  END AS status,
  current_timestamp() AS updated_at
FROM RANGE(5000)
