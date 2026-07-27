-- final table:
CREATE TABLE lakehouse.realtime_orders
(
    order_id String,
    user_id String,
    order_timestamp DateTime64(6),

    total Decimal(10, 2),
    status LowCardinality(String),
    payment_method Nullable(String),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(order_timestamp)
        ORDER BY
            (
             order_timestamp,
             order_id,
             kafka_partition,
             kafka_offset
                );

-- kafka engine:
CREATE TABLE lakehouse.kafka_orders
(
    order_id String,
    user_id String,
    timestamp DateTime64(6),

    total Decimal(10, 2),
    status String,
    payment_method Nullable(String)
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.orders',
            kafka_group_name = 'clickhouse-realtime-orders-v1',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- cleanig and MV
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_orders
            TO lakehouse.realtime_orders
AS
SELECT
    trim(order_id) AS order_id,

    trim(user_id) AS user_id,

    timestamp AS order_timestamp,

    total,

    lowerUTF8(trim(status)) AS status,

    nullIf(
            lowerUTF8(trim(ifNull(payment_method, ''))),
            ''
    ) AS payment_method,

    toUInt8(
            notEmpty(trim(order_id))
                AND notEmpty(trim(user_id))
                AND timestamp > toDateTime64('1970-01-01 00:00:00', 6)
                AND total >= 0
                AND notEmpty(trim(status))
    ) AS is_valid,

    multiIf(
            empty(trim(order_id)),
            'missing_order_id',

            empty(trim(user_id)),
            'missing_user_id',

            timestamp <= toDateTime64('1970-01-01 00:00:00', 6),
            'invalid_order_timestamp',

            total < 0,
            'negative_order_total',

            empty(trim(status)),
            'missing_order_status',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_orders;