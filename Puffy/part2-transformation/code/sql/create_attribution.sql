CREATE OR REPLACE TABLE analytics.attribution AS
SELECT
  purchase_id,
  client_id,
  order_value,
  first_click_source,
  last_click_source,
  TIMESTAMP(timestamp) AS purchase_ts
FROM analytics.attribution_results;
