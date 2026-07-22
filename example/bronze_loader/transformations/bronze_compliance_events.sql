CREATE OR REFRESH MATERIALIZED VIEW bronze_compliance_events
AS SELECT
  (id % 1000) AS customer_id,
  CASE id % 4
    WHEN 0 THEN 'kyc_verified'
    WHEN 1 THEN 'pii_access_review'
    WHEN 2 THEN 'consent_updated'
    ELSE 'data_retention_check'
  END AS event_type,
  DATEADD(HOUR, -(id % 720), current_timestamp()) AS event_timestamp,
  CASE id % 5
    WHEN 0 THEN 'failed'
    ELSE 'passed'
  END AS status
FROM RANGE(2000)
