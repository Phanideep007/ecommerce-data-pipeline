CREATE OR REPLACE TABLE analytics.funnel AS
SELECT
  session_id,
  MAX(CASE WHEN event_name = 'product_added_to_cart' THEN timestamp END) AS add_ts,
  MAX(CASE WHEN event_name = 'checkout_started' THEN timestamp END) AS checkout_ts,
  MAX(CASE WHEN event_name = 'purchase' THEN timestamp END) AS purchase_ts
FROM analytics.enriched_events
GROUP BY session_id;
