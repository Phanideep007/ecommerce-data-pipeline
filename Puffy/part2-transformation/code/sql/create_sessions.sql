CREATE OR REPLACE TABLE analytics.sessions AS
SELECT
  session_id,
  client_id,
  MIN(timestamp) AS session_start,
  MAX(timestamp) AS session_end,
  COUNTIF(event_name = 'page_viewed') AS pageviews,
  COUNTIF(event_name = 'product_added_to_cart') AS add_to_cart_count,
  COUNTIF(event_name = 'checkout_started') AS checkout_count,
  COUNTIF(event_name = 'purchase') AS purchase_count,
  ANY_VALUE(device) AS device_type,
  ANY_VALUE(source) AS source,
  DATE(session_start) AS event_date
FROM analytics.enriched_events
GROUP BY session_id, client_id;
