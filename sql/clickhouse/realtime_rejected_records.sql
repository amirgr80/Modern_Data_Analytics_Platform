CREATE DATABASE IF NOT EXISTS lakehouse;

-- used for all topic tables and rejected will be stored here
CREATE TABLE IF NOT EXISTS lakehouse.realtime_rejected_records
(
    source_topic LowCardinality(String),
    source_entity LowCardinality(String),

    kafka_partition Int32,
    kafka_offset Int64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),

    raw_message String,

    error_type LowCardinality(String),
    error_codes Array(String),
    error_message String,

    rejected_at DateTime64(3, 'UTC')
        DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(rejected_at)
ORDER BY
(
    source_topic,
    source_entity,
    rejected_at,
    kafka_partition,
    kafka_offset
);