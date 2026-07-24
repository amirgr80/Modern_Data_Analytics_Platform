CREATE TABLE IF NOT EXISTS lakehouse.transactional_obt
(
    order_item_id           String,
    order_id                String,
    product_id              String,
    category_id             Nullable(String),
    user_id                 String,
    price_history_id        Nullable(String),
    date_key                Int32,
    order_timestamp          DateTime,
    full_date                  Date,
    year_number                 UInt16,
    month_number                  UInt8,
    quarter_number                   UInt8,
    day_of_week                        UInt8,
    is_weekend                            UInt8,
    order_status              LowCardinality(Nullable(String)),
    payment_method               LowCardinality(Nullable(String)),
    quantity                    UInt32,
    unit_price                     Nullable(Decimal(29, 2)),
    discount_amount                   Nullable(Decimal(29, 2)),
    item_total_amount                    Nullable(Decimal(29, 2)),
    price_difference                        Nullable(Decimal(29, 2)),
    order_total_amount                         Nullable(Decimal(29, 2)),
    product_name                Nullable(String),
    category_name                  Nullable(String),
    username                     Nullable(String),
    email                           Nullable(String),
    signup_date                       Date,
    location                            Nullable(String),
    loyalty_tier                           LowCardinality(Nullable(String)),
    device                                    LowCardinality(Nullable(String)),
    is_returned                   UInt8 DEFAULT 0,
    return_reason                   Nullable(String),
    refund_amount                     Nullable(Decimal(29, 2)),
    return_timestamp                    Nullable(DateTime),
    silver_updated_at             DateTime,
    gold_loaded_at                  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(silver_updated_at)
PARTITION BY toYYYYMM(full_date)
ORDER BY (full_date, category_id, product_id, order_id, order_item_id)
SETTINGS index_granularity = 8192, allow_nullable_key = 1;