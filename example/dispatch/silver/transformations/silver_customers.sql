CREATE OR REFRESH MATERIALIZED VIEW dispatch_silver_customers
AS SELECT
  customer_id,
  TRIM(name) AS name,
  LOWER(email) AS email,
  created_at,
  updated_at
FROM bronze_customers
