CREATE TABLE lakehouse.realtime_behavioral_events
(
    event_timestamp DateTime64(3),

    event_id Nullable(String),
    user_id Nullable(String),

    event_type LowCardinality(String),
    device LowCardinality(String),
    session_id String,

    ip_address Nullable(String),
    utm_source Nullable(String),

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

    calculated_cart_total_items Int64,
    calculated_cart_value Float64,

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
    validation_errors Array(String),
    validation_warnings Array(String),

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

CREATE TABLE lakehouse.kafka_behavioral_events
(
    timestamp String,

    event_id Nullable(String),
    user_id Nullable(String),

    event_type String,
    device Nullable(String),
    session_id String,

    ip_address Nullable(String),
    utm_source Nullable(String),

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
    kafka_group_name = 'clickhouse-realtime-behavioral-events-v2',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 0,
    format_avro_schema_registry_url = 'http://185.255.90.14:8081';



-- deep cleaning and validation
CREATE MATERIALIZED VIEW lakehouse.mv_realtime_behavioral_events
TO lakehouse.realtime_behavioral_events
AS
WITH
    parseDateTime64BestEffortOrNull(timestamp, 3) AS parsed_timestamp,

    replaceRegexpAll(
        lowerUTF8(trim(event_type)),
        '[\\s-]+',
        '_'
    ) AS normalized_event_type,

    multiIf(
        lowerUTF8(trim(ifNull(device, ''))) IN
            ('android', 'ios', 'iphone', 'phone'),
            'mobile',

        lowerUTF8(trim(ifNull(device, ''))) IN
            ('web', 'pc'),
            'desktop',

        empty(trim(ifNull(device, ''))),
            'unknown',

        lowerUTF8(trim(ifNull(device, '')))
    ) AS normalized_device,

    arraySum(
        item -> toInt64(item.3),
        cart_items
    ) AS calculated_items,

    arraySum(
        item -> toFloat64(item.2) * toFloat64(item.3),
        cart_items
    ) AS calculated_value,

    arrayCount(
        item ->
            empty(trim(item.1))
            OR item.2 <= 0
            OR item.3 <= 0,
        cart_items
    ) AS invalid_cart_items_count,

    normalized_event_type IN
    (
        'cart_view',
        'view_cart',
        'update_cart',
        'checkout_start'
    ) AS is_cart_snapshot_event,

    normalized_event_type IN
    (
        'add_to_cart',
        'remove_from_cart'
    ) AS is_cart_item_event,

    normalized_event_type IN
    (
        'product_view',
        'add_to_cart',
        'remove_from_cart',
        'wishlist_add',
        'wishlist_remove',
        'rating',
        'review',
        'product_rating'
    ) AS requires_product,

    normalized_event_type IN
    (
        'search',
        'search_product'
    ) AS is_search_event,

    normalized_event_type IN
    (
        'page_view',
        'home_view',
        'category_view',
        'page_error'
    ) AS is_page_event,

    normalized_event_type IN
    (
        'payment_attempt',
        'checkout_attempt'
    ) AS is_payment_event,

    normalized_event_type IN
    (
        'purchase',
        'order_placed'
    ) AS is_order_event,

    normalized_event_type IN
    (
        'rating',
        'product_rating',
        'review'
    ) AS is_rating_event,

    arrayFilter(
        error -> error != '',
        [
            if(
                parsed_timestamp IS NULL,
                'invalid_event_timestamp',
                ''
            ),

            if(
                empty(trim(session_id)),
                'missing_session_id',
                ''
            ),

            if(
                empty(normalized_event_type),
                'missing_event_type',
                ''
            ),

            if(
                quantity IS NOT NULL
                AND quantity <= 0,
                'invalid_quantity',
                ''
            ),

            if(
                cart_total_items IS NOT NULL
                AND cart_total_items < 0,
                'negative_cart_total_items',
                ''
            ),

            if(
                cart_value IS NOT NULL
                AND cart_value < 0,
                'negative_cart_value',
                ''
            ),

            if(
                duration_sec IS NOT NULL
                AND duration_sec < 0,
                'negative_duration',
                ''
            ),

            if(
                http_status IS NOT NULL
                AND (
                    http_status < 100
                    OR http_status > 599
                ),
                'invalid_http_status',
                ''
            ),

            if(
                results_count IS NOT NULL
                AND results_count < 0,
                'negative_results_count',
                ''
            ),

            if(
                clicked_position IS NOT NULL
                AND clicked_position < 0,
                'invalid_clicked_position',
                ''
            ),

            if(
                clicked_position IS NOT NULL
                AND results_count IS NOT NULL
                AND clicked_position > results_count,
                'clicked_position_exceeds_results_count',
                ''
            ),

            if(
                rating IS NOT NULL
                AND (
                    rating < 1
                    OR rating > 5
                ),
                'invalid_rating',
                ''
            ),

            if(
                text_length IS NOT NULL
                AND text_length < 0,
                'negative_text_length',
                ''
            ),

            if(
                invalid_cart_items_count > 0,
                'invalid_cart_item',
                ''
            ),

            if(
                is_cart_snapshot_event
                AND notEmpty(cart_items)
                AND cart_value IS NULL,
                'missing_cart_value',
                ''
            ),

            if(
                is_cart_snapshot_event
                AND cart_value IS NOT NULL
                AND abs(cart_value - calculated_value) > 0.01,
                'cart_value_mismatch',
                ''
            ),

            if(
                is_cart_snapshot_event
                AND cart_total_items IS NOT NULL
                AND cart_total_items != calculated_items,
                'cart_total_items_mismatch',
                ''
            ),

            if(
                is_cart_item_event
                AND empty(trim(ifNull(product_id, ''))),
                'missing_product_id',
                ''
            ),

            if(
                is_cart_item_event
                AND quantity IS NULL,
                'missing_quantity',
                ''
            ),

            if(
                requires_product
                AND empty(trim(ifNull(product_id, ''))),
                'missing_product_id_for_event',
                ''
            ),

            if(
                is_search_event
                AND empty(trim(ifNull(query, ''))),
                'missing_search_query',
                ''
            ),

            if(
                is_page_event
                AND empty(trim(ifNull(url_path, ''))),
                'missing_url_path',
                ''
            ),

            if(
                normalized_event_type = 'page_error'
                AND (
                    http_status IS NULL
                    OR http_status < 400
                )
                AND empty(trim(ifNull(error_code, ''))),
                'missing_page_error_details',
                ''
            ),

            if(
                is_payment_event
                AND empty(trim(ifNull(payment_type, ''))),
                'missing_payment_type',
                ''
            ),

            if(
                is_payment_event
                AND success IS NULL,
                'missing_payment_status',
                ''
            ),

            if(
                is_payment_event
                AND success = false
                AND empty(trim(ifNull(error_code, ''))),
                'missing_payment_error_code',
                ''
            ),

            if(
                is_order_event
                AND empty(trim(ifNull(order_id, ''))),
                'missing_order_id',
                ''
            ),

            if(
                is_rating_event
                AND rating IS NULL,
                'missing_rating',
                ''
            ),

            if(
                normalized_event_type = 'review'
                AND (
                    text_length IS NULL
                    OR text_length <= 0
                ),
                'missing_review_text',
                ''
            )
        ]
    ) AS errors,

    arrayFilter(
        warning -> warning != '',
        [
            if(
                empty(trim(ifNull(user_id, ''))),
                'anonymous_or_missing_user',
                ''
            ),

            if(
                normalized_device = 'unknown',
                'unknown_device',
                ''
            ),

            if(
                normalized_device NOT IN
                (
                    'mobile',
                    'desktop',
                    'tablet',
                    'unknown'
                ),
                'unrecognized_device',
                ''
            ),

            if(
                normalized_event_type NOT IN
                (
                    'page_view',
                    'product_view',
                    'home_view',
                    'category_view',
                    'click',
                    'search_product',
                    'search',
                    'add_to_cart',
                    'remove_from_cart',
                    'cart_view',
                    'view_cart',
                    'update_cart',
                    'wishlist_add',
                    'wishlist_remove',
                    'checkout_start',
                    'checkout_attempt',
                    'payment_attempt',
                    'purchase',
                    'order_placed',
                    'rating',
                    'review',
                    'product_rating',
                    'error',
                    'page_error'
                ),
                'unknown_event_type',
                ''
            ),

            if(
                is_cart_snapshot_event
                AND cart_total_items IS NULL,
                'missing_cart_total_items',
                ''
            ),

            if(
                is_search_event
                AND results_count IS NULL,
                'missing_results_count',
                ''
            ),

            if(
                is_payment_event
                AND success = true
                AND notEmpty(trim(ifNull(error_code, ''))),
                'successful_payment_has_error_code',
                ''
            )
        ]
    ) AS warnings

SELECT
    coalesce(
        parsed_timestamp,
        toDateTime64('1970-01-01 00:00:00', 3)
    ) AS event_timestamp,

    nullIf(trim(ifNull(event_id, '')), '') AS event_id,
    nullIf(trim(ifNull(user_id, '')), '') AS user_id,

    normalized_event_type AS event_type,
    normalized_device AS device,
    trim(session_id) AS session_id,

    nullIf(trim(ifNull(ip_address, '')), '') AS ip_address,

    nullIf(
        lowerUTF8(trim(ifNull(utm_source, ''))),
        ''
    ) AS utm_source,

    nullIf(trim(ifNull(product_id, '')), '') AS product_id,

    quantity,
    cart_total_items,
    cart_items,
    cart_value,

    calculated_items AS calculated_cart_total_items,
    calculated_value AS calculated_cart_value,

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

    toUInt8(empty(errors)) AS is_valid,

    if(
        empty(errors),
        NULL,
        errors[1]
    ) AS validation_error,

    errors AS validation_errors,
    warnings AS validation_warnings,

    _topic AS kafka_topic,
    _partition AS kafka_partition,
    _offset AS kafka_offset,

    now64(3) AS ingested_at

FROM lakehouse.kafka_behavioral_events;