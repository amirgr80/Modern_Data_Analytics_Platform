-- final table
CREATE TABLE lakehouse.realtime_product_price_history
(
    price_history_id String,
    product_id String,

    price Decimal(10, 2),

    valid_from DateTime64(6),
    valid_to Nullable(DateTime64(6)),
    is_current UInt8,

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(valid_from)
        ORDER BY
            (
             valid_from,
             product_id,
             price_history_id,
             kafka_partition,
             kafka_offset
                );

-- Kafka engine

CREATE TABLE lakehouse.kafka_product_price_history
(
    price_history_id String,
    product_id String,

    price Decimal(10, 2),

    valid_from DateTime64(6),
    valid_to Nullable(DateTime64(6)),
    is_current Bool
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.product_price_history',
            kafka_group_name = 'clickhouse-realtime-product-price-history-v1',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';


-- cleaning & MV

CREATE MATERIALIZED VIEW lakehouse.mv_realtime_product_price_history
            TO lakehouse.realtime_product_price_history
AS
SELECT
    trim(price_history_id) AS price_history_id,

    trim(product_id) AS product_id,

    price,

    valid_from,

    valid_to,

    toUInt8(is_current) AS is_current,

    toUInt8(
            notEmpty(trim(price_history_id))
                AND notEmpty(trim(product_id))
                AND price >= 0
                AND valid_from > toDateTime64('1970-01-01 00:00:00', 6)
                AND (
                valid_to IS NULL
                    OR valid_to >= valid_from
                )
                AND NOT (
                is_current = true
                    AND valid_to IS NOT NULL
                )
    ) AS is_valid,

    multiIf(
            empty(trim(price_history_id)),
            'missing_price_history_id',

            empty(trim(product_id)),
            'missing_product_id',

            price < 0,
            'negative_price',

            valid_from <= toDateTime64('1970-01-01 00:00:00', 6),
            'invalid_valid_from',

            valid_to IS NOT NULL
                AND valid_to < valid_from,
            'valid_to_before_valid_from',

            is_current = true
                AND valid_to IS NOT NULL,
            'current_price_has_valid_to',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_product_price_history;
