import os
from typing import Final


BRONZE_TRANSACTIONAL_BASE_PATH: Final[str] = os.getenv(
    "BRONZE_TRANSACTIONAL_BASE_PATH",
    "s3a://bronze/transactional",
)


TRANSACTIONAL_TABLES: Final[tuple[str, ...]] = (
    "users",
    "categories",
    "products",
    "product_price_history",
    "orders",
    "order_items",
)


OPTIONAL_TRANSACTIONAL_TABLES: Final[tuple[str, ...]] = (
    "returns_refunds",
)


ALL_TRANSACTIONAL_TABLES: Final[tuple[str, ...]] = (
    *TRANSACTIONAL_TABLES,
    *OPTIONAL_TRANSACTIONAL_TABLES,
)


ORDER_STATUSES: Final[tuple[str, ...]] = (
    "created",
    "shipped",
    "delivered",
    "cancelled",
    "refunded",
)


LOYALTY_TIERS: Final[tuple[str, ...]] = (
    "bronze",
    "silver",
    "gold",
)