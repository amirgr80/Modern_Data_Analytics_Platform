-- Behavioral Gold verification queries for ClickHouse.
-- Replace the date literal with the processing date loaded by the Gold DAG.

SELECT
    count() AS total_rows,
    uniqExact(event_key) AS unique_event_keys,
    total_rows - unique_event_keys AS duplicate_event_keys
FROM lakehouse.behavioral_obt
WHERE processing_date = toDate('2026-01-01');

SELECT
    max(event_timestamp) AS latest_event_timestamp,
    max(silver_ingestion_timestamp) AS latest_silver_ingestion_timestamp,
    max(gold_loaded_at) AS latest_gold_loaded_at
FROM lakehouse.behavioral_obt;

SELECT
    event_category,
    event_type,
    count() AS events,
    uniqExact(user_id) AS users,
    uniqExact(session_key) AS sessions,
    sumIf(cart_value, cart_value IS NOT NULL) AS cart_value_sum
FROM lakehouse.behavioral_obt
WHERE processing_date = toDate('2026-01-01')
GROUP BY
    event_category,
    event_type
ORDER BY events DESC
LIMIT 50;

SELECT
    device_name,
    count() AS events,
    uniqExact(user_id) AS users,
    uniqExact(session_key) AS sessions,
    avgIf(duration_sec, duration_sec IS NOT NULL) AS avg_duration_sec
FROM lakehouse.behavioral_obt
WHERE processing_date = toDate('2026-01-01')
GROUP BY device_name
ORDER BY events DESC;

SELECT
    utm_source,
    countIf(event_category = 'browse') AS browse_events,
    countIf(event_category = 'cart') AS cart_events,
    countIf(event_category = 'checkout') AS checkout_events,
    countIf(event_type IN ('purchase', 'order_placed')) AS purchase_events
FROM lakehouse.behavioral_obt
WHERE processing_date = toDate('2026-01-01')
GROUP BY utm_source
ORDER BY browse_events DESC;
