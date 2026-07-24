-- normalizing price history
CREATE OR REPLACE VIEW lakehouse.latest_products AS
SELECT
    product_id,

    argMax(name, kafka_offset) AS name,
    argMax(price, kafka_offset) AS price,
    argMax(category, kafka_offset) AS category,
    argMax(inventory, kafka_offset) AS inventory,
    argMax(popularity_score, kafka_offset) AS popularity_score,

    argMax(is_valid, kafka_offset) AS is_valid,
    argMax(validation_error, kafka_offset) AS validation_error,

    argMax(kafka_topic, kafka_offset) AS kafka_topic,
    argMax(kafka_partition, kafka_offset) AS kafka_partition,

    max(kafka_offset) AS latest_kafka_offset,
    argMax(ingested_at, kafka_offset) AS last_ingested_at

FROM lakehouse.realtime_products
GROUP BY product_id;
-- test:
SELECT
    count() AS product_count,
    uniqExact(product_id) AS unique_products,
    countIf(is_valid = 1) AS valid_products,
    countIf(is_valid = 0) AS invalid_products,
    min(price) AS minimum_price,
    max(price) AS maximum_price,
    max(last_ingested_at) AS latest_update
FROM lakehouse.latest_products;

-- get the curren price of each product:
CREATE OR REPLACE VIEW lakehouse.current_product_prices AS
SELECT
    product_id,
    argMax(
            price_history_id,
            tuple(valid_from, kafka_partition, kafka_offset)
    ) AS price_history_id,
    argMax(
            price,
            tuple(valid_from, kafka_partition, kafka_offset)
    ) AS current_price,
    max(valid_from) AS current_valid_from
FROM lakehouse.realtime_product_price_history
WHERE is_valid = 1
GROUP BY product_id;

