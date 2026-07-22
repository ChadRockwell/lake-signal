CREATE OR REFRESH MATERIALIZED VIEW bronze_customers
AS SELECT
  id AS customer_id,
  CONCAT('customer_', CAST(id AS STRING)) AS name,
  CONCAT('user', CAST(id AS STRING), '@example.com') AS email,
  DATEADD(DAY, -(id % 365), current_timestamp()) AS created_at,
  current_timestamp() AS updated_at
FROM RANGE(1000)
