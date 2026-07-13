from typing import Final


BRONZE_BUCKET: Final[str] = "bronze"

BRONZE_TRANSACTIONAL_BASE_PATH: Final[str] = (
    "s3a://bronze/transactional"
)

SUPPORTED_TRANSACTIONAL_TABLES: Final[tuple[str, ...]] = (
    "categories",
    "order_items",
    "orders",
    "product_price_history",
    "products",
    "users",
)

BRONZE_PARTITION_COLUMN: Final[str] = "partition_date"

BRONZE_TECHNICAL_COLUMNS: Final[tuple[str, ...]] = (
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "source_table",
    "bronze_ingestion_timestamp",
    "partition_date",
)