-- final table:
CREATE TABLE lakehouse.realtime_products
(
    product_id String,
    name String,
    price Decimal(10, 2),

    category Nullable(String),
    inventory Nullable(Int32),
    popularity_score Nullable(Decimal(4, 2)),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(ingested_at)
        ORDER BY (product_id, ingested_at, kafka_partition, kafka_offset);

-- kafka engine:
CREATE TABLE lakehouse.kafka_products
(
    product_id String,
    name String,
    price Decimal(10, 2),

    category Nullable(String),
    inventory Nullable(Int32),
    popularity_score Nullable(Decimal(4, 2))
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.products',
            kafka_group_name = 'clickhouse-realtime-products-v2',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- simple cleaning in MV:
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_products
            TO lakehouse.realtime_products
AS
SELECT
    trim(product_id) AS product_id,

    trim(name) AS name,

    price,

    nullIf(trim(ifNull(category, '')), '') AS category,

    if(
            inventory IS NULL OR inventory >= 0,
            inventory,
            NULL
    ) AS inventory,

    if(
            popularity_score IS NULL
                OR (
                popularity_score >= 0
                    AND popularity_score <= 100
                ),
            popularity_score,
            NULL
    ) AS popularity_score,

    toUInt8(
            notEmpty(trim(product_id))
                AND notEmpty(trim(name))
                AND price >= 0
                AND (inventory IS NULL OR inventory >= 0)
                AND (
                popularity_score IS NULL
                    OR (
                    popularity_score >= 0
                        AND popularity_score <= 100
                    )
                )
    ) AS is_valid,

    multiIf(
            empty(trim(product_id)), 'missing_product_id',
            empty(trim(name)), 'missing_product_name',
            price < 0, 'negative_price',
            inventory IS NOT NULL AND inventory < 0, 'negative_inventory',
            popularity_score IS NOT NULL
                AND (
                popularity_score < 0
                    OR popularity_score > 100
                ),
            'invalid_popularity_score',
            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_products;

