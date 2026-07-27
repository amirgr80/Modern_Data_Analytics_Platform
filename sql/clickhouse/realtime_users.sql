-- final table
CREATE TABLE lakehouse.realtime_users
(
    user_id String,
    username String,
    email String,
    signup_date Date,

    device Nullable(String),
    loyalty_tier Nullable(String),
    location Nullable(String),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(signup_date)
        ORDER BY
            (
             signup_date,
             user_id,
             kafka_partition,
             kafka_offset
                );

-- kafka engine
CREATE TABLE lakehouse.kafka_users
(
    user_id String,
    username String,
    email String,
    signup_date Date,

    device Nullable(String),
    loyalty_tier Nullable(String),
    location Nullable(String)
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'transactional.users',
            kafka_group_name = 'clickhouse-realtime-users-v1',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- cleaning & MV
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_users
            TO lakehouse.realtime_users
AS
SELECT
    trim(user_id) AS user_id,

    trim(username) AS username,

    lowerUTF8(trim(email)) AS email,

    signup_date,

    nullIf(
            lowerUTF8(trim(ifNull(device, ''))),
            ''
    ) AS device,

    nullIf(
            lowerUTF8(trim(ifNull(loyalty_tier, ''))),
            ''
    ) AS loyalty_tier,

    nullIf(
            trim(ifNull(location, '')),
            ''
    ) AS location,

    toUInt8(
            notEmpty(trim(user_id))
                AND notEmpty(trim(username))
                AND notEmpty(trim(email))
                AND position(trim(email), '@') > 1
                AND signup_date >= toDate('2000-01-01')
                AND signup_date <= today()
    ) AS is_valid,

    multiIf(
            empty(trim(user_id)),
            'missing_user_id',

            empty(trim(username)),
            'missing_username',

            empty(trim(email)),
            'missing_email',

            position(trim(email), '@') <= 1,
            'invalid_email',

            signup_date < toDate('2000-01-01'),
            'invalid_signup_date',

            signup_date > today(),
            'future_signup_date',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_users;