-- final table
CREATE TABLE lakehouse.realtime_order_items
(
    order_item_id String,
    order_id String,
    product_id String,

    quantity Int32,
    unit_price Decimal(10, 2),
    item_total_amount Decimal(10, 2),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        ORDER BY
            (
             order_id,
             order_item_id,
             kafka_partition,
             kafka_offset
                );

--kafka engine
CREATE TABLE lakehouse.kafka_order_items
(
    order_item_id String,
    order_id String,
    product_id String,

    quantity Int32,
    unit_price Decimal(10, 2),
    item_total_amount Decimal(10, 2)
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.order_items',
            kafka_group_name = 'clickhouse-realtime-order-items-v1',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- cleaning & MV
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_order_items
            TO lakehouse.realtime_order_items
AS
SELECT
    trim(order_item_id) AS order_item_id,

    trim(order_id) AS order_id,

    trim(product_id) AS product_id,

    quantity,

    unit_price,

    item_total_amount,

    toUInt8(
            notEmpty(trim(order_item_id))
                AND notEmpty(trim(order_id))
                AND notEmpty(trim(product_id))
                AND quantity > 0
                AND unit_price >= 0
                AND item_total_amount >= 0
    ) AS is_valid,

    multiIf(
            empty(trim(order_item_id)),
            'missing_order_item_id',

            empty(trim(order_id)),
            'missing_order_id',

            empty(trim(product_id)),
            'missing_product_id',

            quantity <= 0,
            'invalid_quantity',

            unit_price < 0,
            'negative_unit_price',

            item_total_amount < 0,
            'negative_item_total_amount',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,

    now64(3) AS ingested_at

FROM lakehouse.kafka_order_items;