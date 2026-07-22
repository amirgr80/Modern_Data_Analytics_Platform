/* 
   Topic:
       transactional.categories

   Schema:
       category_id          String
       name                 String
       parent_category_id   Nullable(String)
*/


CREATE DATABASE IF NOT EXISTS lakehouse;

-- final table
CREATE TABLE IF NOT EXISTS lakehouse.realtime_categories
(
    category_id String,
    name String,
    parent_category_id Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),

    received_at DateTime64(3, 'UTC')
        DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY
(
    category_id,
    kafka_partition,
    kafka_offset
);


-- Kafka Engine table 
CREATE TABLE IF NOT EXISTS lakehouse.kafka_categories
(
    category_id String,
    name String,
    parent_category_id Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '185.255.90.14:9092',
    kafka_topic_list = 'transactional.categories',
    kafka_group_name = 'clickhouse_realtime_categories_v1',

    kafka_format = 'AvroConfluent',

    format_avro_schema_registry_url =
        '185.255.90.14:8082',

    kafka_num_consumers = 3,

    kafka_handle_error_mode = 'stream';


-- Valid Materialized View

CREATE MATERIALIZED VIEW IF NOT EXISTS
lakehouse.mv_categories_valid
TO lakehouse.realtime_categories
AS
SELECT
    trimBoth(category_id) AS category_id,

    trimBoth(name) AS name,

    nullIf(
        trimBoth(ifNull(parent_category_id, '')),
        ''
    ) AS parent_category_id,

    _topic AS kafka_topic,
    toInt32(_partition) AS kafka_partition,
    toInt64(_offset) AS kafka_offset,

    CAST(
        _timestamp,
        'Nullable(DateTime64(3, \'UTC\'))'
    ) AS kafka_timestamp,

    now64(3) AS received_at

FROM lakehouse.kafka_categories

WHERE
    empty(_error)
    AND notEmpty(trimBoth(category_id))
    AND notEmpty(trimBoth(name));


-- Rejected Materialized View

CREATE MATERIALIZED VIEW IF NOT EXISTS
lakehouse.mv_categories_rejected
TO lakehouse.realtime_rejected_records
AS
SELECT
    _topic AS source_topic,
    'categories' AS source_entity,

    toInt32(_partition) AS kafka_partition,
    toInt64(_offset) AS kafka_offset,

    CAST(
        _timestamp,
        'Nullable(DateTime64(3, \'UTC\'))'
    ) AS kafka_timestamp,

    if(
        notEmpty(_error),

        _raw_message,

        concat(
            '{',
                '"category_id":',
                toJSONString(category_id),
                ',',
                '"name":',
                toJSONString(name),
                ',',
                '"parent_category_id":',
                toJSONString(parent_category_id),
            '}'
        )
    ) AS raw_message,

    if(
        notEmpty(_error),
        'decode_error',
        'business_validation'
    ) AS error_type,

    if(
        notEmpty(_error),

        ['avro_decode_failed'],

        arrayFilter(
            error_code -> notEmpty(error_code),
            [
                if(
                    empty(trimBoth(category_id)),
                    'missing_category_id',
                    ''
                ),

                if(
                    empty(trimBoth(name)),
                    'missing_category_name',
                    ''
                )
            ]
        )
    ) AS error_codes,

    if(
        notEmpty(_error),

        _error,

        'One or more required category fields are empty.'
    ) AS error_message,

    now64(3) AS rejected_at

FROM lakehouse.kafka_categories

WHERE
    notEmpty(_error)
    OR empty(trimBoth(category_id))
    OR empty(trimBoth(name));