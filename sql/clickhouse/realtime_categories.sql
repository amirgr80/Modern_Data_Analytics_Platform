-- final table
CREATE TABLE lakehouse.realtime_categories
(
    category_id String,
    name String,
    parent_category_id Nullable(String),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(ingested_at)
        ORDER BY
            (
             category_id,
             kafka_partition,
             kafka_offset
                );


-- kafka engine
CREATE TABLE lakehouse.kafka_categories
(
    category_id String,
    name String,
    parent_category_id Nullable(String)
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.categories',
            kafka_group_name = 'clickhouse-realtime-categories-v1',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- cleaning & MV
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_categories
            TO lakehouse.realtime_categories
AS
SELECT
    trim(category_id) AS category_id,

    trim(name) AS name,

    nullIf(
            trim(ifNull(parent_category_id, '')),
            ''
    ) AS parent_category_id,

    toUInt8(
            notEmpty(trim(category_id))
                AND notEmpty(trim(name))
    ) AS is_valid,

    multiIf(
            empty(trim(category_id)),
            'missing_category_id',

            empty(trim(name)),
            'missing_category_name',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_categories;
