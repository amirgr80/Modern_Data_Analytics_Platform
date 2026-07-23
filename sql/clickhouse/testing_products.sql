-- Testing:
SELECT count()
FROM lakehouse.realtime_products;

SELECT *
FROM lakehouse.realtime_products
ORDER BY ingested_at DESC
LIMIT 100;

SELECT
    kafka_partition,
    count() AS records,
    min(kafka_offset) AS first_offset,
    max(kafka_offset) AS last_offset,
    max(ingested_at) AS last_ingested_at
FROM lakehouse.realtime_products
GROUP BY kafka_partition
ORDER BY kafka_partition;

SELECT
    validation_error,
    count() AS error_count
FROM lakehouse.realtime_products
WHERE is_valid = 0
GROUP BY validation_error
ORDER BY error_count DESC;

SELECT
    count() AS total_rows,
    countIf(category IS NULL) AS null_category,
    countIf(inventory IS NULL) AS null_inventory,
    countIf(popularity_score IS NULL) AS null_popularity_score,
    countIf(is_valid = 0) AS invalid_rows
FROM lakehouse.realtime_products;