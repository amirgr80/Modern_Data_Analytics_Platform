-- final table
CREATE TABLE lakehouse.realtime_behavioral_events
(
    event_timestamp DateTime64(3),

    user_id String,
    event_type LowCardinality(String),
    device LowCardinality(String),
    session_id String,

    product_id Nullable(String),
    quantity Nullable(Int32),
    cart_total_items Nullable(Int32),

    cart_items Array(
                        Tuple(
                            product_id String,
                            price Float64,
                            quantity Int32
                            )
                        ),

    cart_value Nullable(Float64),
    shipping_method Nullable(String),

    order_id Nullable(String),
    fulfillment_speed Nullable(String),

    url_path Nullable(String),
    duration_sec Nullable(Int32),
    http_status Nullable(Int32),

    payment_type Nullable(String),
    success Nullable(Bool),
    error_code Nullable(String),

    search_query Nullable(String),
    results_count Nullable(Int32),
    clicked_position Nullable(Int32),

    rating Nullable(Int32),
    text_length Nullable(Int32),
    wishlist_name Nullable(String),

    is_valid UInt8,
    validation_error Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    ingested_at DateTime64(3)
)
    ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_timestamp)
        ORDER BY
            (
             event_timestamp,
             event_type,
             session_id,
             kafka_partition,
             kafka_offset
                );

-- kafka engine:
CREATE TABLE lakehouse.kafka_behavioral_events
(
    timestamp String,

    user_id String,
    event_type String,
    device String,
    session_id String,

    product_id Nullable(String),
    quantity Nullable(Int32),
    cart_total_items Nullable(Int32),

    cart_items Array(
                  Tuple(
                      product_id String,
                      price Float64,
                      quantity Int32
                      )
                  ),

    cart_value Nullable(Float64),
    shipping_method Nullable(String),

    order_id Nullable(String),
    fulfillment_speed Nullable(String),

    url_path Nullable(String),
    duration_sec Nullable(Int32),
    http_status Nullable(Int32),

    payment_type Nullable(String),
    success Nullable(Bool),
    error_code Nullable(String),

    query Nullable(String),
    results_count Nullable(Int32),
    clicked_position Nullable(Int32),

    rating Nullable(Int32),
    text_length Nullable(Int32),
    wishlist_name Nullable(String)
)
    ENGINE = Kafka
        SETTINGS
            kafka_broker_list = '185.255.90.14:9092',
            kafka_topic_list = 'behavioral.events',
            kafka_group_name = 'clickhouse-behavioral-groupk',
            kafka_format = 'AvroConfluent',
            kafka_num_consumers = 1,
            kafka_thread_per_consumer = 0,
            format_avro_schema_registry_url = 'http://185.255.90.14:8081';

-- cleaning & MV
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_behavioral_events
            TO lakehouse.realtime_behavioral_events
AS
WITH
    parseDateTime64BestEffortOrNull(timestamp, 3) AS parsed_timestamp
SELECT
    coalesce(
            parsed_timestamp,
            toDateTime64('1970-01-01 00:00:00', 3)
    ) AS event_timestamp,

    trim(user_id) AS user_id,
    lowerUTF8(trim(event_type)) AS event_type,
    lowerUTF8(trim(device)) AS device,
    trim(session_id) AS session_id,

    nullIf(trim(ifNull(product_id, '')), '') AS product_id,

    quantity,
    cart_total_items,
    cart_items,
    cart_value,

    nullIf(
            lowerUTF8(trim(ifNull(shipping_method, ''))),
            ''
    ) AS shipping_method,

    nullIf(trim(ifNull(order_id, '')), '') AS order_id,

    nullIf(
            lowerUTF8(trim(ifNull(fulfillment_speed, ''))),
            ''
    ) AS fulfillment_speed,

    nullIf(trim(ifNull(url_path, '')), '') AS url_path,

    duration_sec,
    http_status,

    nullIf(
            lowerUTF8(trim(ifNull(payment_type, ''))),
            ''
    ) AS payment_type,

    success,

    nullIf(trim(ifNull(error_code, '')), '') AS error_code,

    nullIf(trim(ifNull(query, '')), '') AS search_query,

    results_count,
    clicked_position,
    rating,
    text_length,

    nullIf(trim(ifNull(wishlist_name, '')), '') AS wishlist_name,

    toUInt8(
            parsed_timestamp IS NOT NULL
                AND notEmpty(trim(user_id))
                AND notEmpty(trim(event_type))
                AND notEmpty(trim(device))
                AND notEmpty(trim(session_id))

                AND (quantity IS NULL OR quantity > 0)
                AND (
                cart_total_items IS NULL
                    OR cart_total_items >= 0
                )
                AND (
                cart_value IS NULL
                    OR cart_value >= 0
                )
                AND (
                duration_sec IS NULL
                    OR duration_sec >= 0
                )
                AND (
                http_status IS NULL
                    OR http_status BETWEEN 100 AND 599
                )
                AND (
                results_count IS NULL
                    OR results_count >= 0
                )
                AND (
                clicked_position IS NULL
                    OR clicked_position >= 0
                )
                AND (
                rating IS NULL
                    OR rating BETWEEN 1 AND 5
                )
                AND (
                text_length IS NULL
                    OR text_length >= 0
                )
    ) AS is_valid,

    multiIf(
            parsed_timestamp IS NULL,
            'invalid_event_timestamp',

            empty(trim(user_id)),
            'missing_user_id',

            empty(trim(event_type)),
            'missing_event_type',

            empty(trim(device)),
            'missing_device',

            empty(trim(session_id)),
            'missing_session_id',

            quantity IS NOT NULL AND quantity <= 0,
            'invalid_quantity',

            cart_total_items IS NOT NULL
                AND cart_total_items < 0,
            'negative_cart_total_items',

            cart_value IS NOT NULL
                AND cart_value < 0,
            'negative_cart_value',

            duration_sec IS NOT NULL
                AND duration_sec < 0,
            'negative_duration',

            http_status IS NOT NULL
                AND NOT (http_status BETWEEN 100 AND 599),
            'invalid_http_status',

            results_count IS NOT NULL
                AND results_count < 0,
            'negative_results_count',

            clicked_position IS NOT NULL
                AND clicked_position < 0,
            'invalid_clicked_position',

            rating IS NOT NULL
                AND NOT (rating BETWEEN 1 AND 5),
            'invalid_rating',

            text_length IS NOT NULL
                AND text_length < 0,
            'negative_text_length',

            NULL
    ) AS validation_error,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,
    now64(3) AS ingested_at

FROM lakehouse.kafka_behavioral_events;