"""
Behavioral Bronze schema metadata.

Architecture decision:
- Confluent Schema Registry is the single source of truth for Avro schemas.
- This module MUST NOT contain an Avro schema copy.
- Local constants here are only pipeline metadata and column contracts.
"""

BEHAVIORAL_TOPIC = "behavioral.events"
BEHAVIORAL_SUBJECT = "behavioral.events-value"

BEHAVIORAL_REQUIRED_COLUMNS = [
    "event_id",
    "timestamp",
    "user_id",
    "event_type",
    "device_type",
    "session_id",
]


BEHAVIORAL_OPTIONAL_COLUMNS = [
    "ip_address",
    "utm_source",
    "product_id",
    "quantity",
    "cart_total_items",
    "cart_items",
    "cart_value",
    "shipping_method",
    "order_id",
    "fulfillment_speed",
    "url_path",
    "duration_sec",
    "http_status",
    "payment_type",
    "success",
    "error_code",
    "query",
    "results_count",
    "clicked_position",
    "rating",
    "text_length",
    "wishlist_name",
]


BEHAVIORAL_ALL_COLUMNS = (
    BEHAVIORAL_REQUIRED_COLUMNS + BEHAVIORAL_OPTIONAL_COLUMNS
)


def get_behavioral_topic() -> str:
    return BEHAVIORAL_TOPIC


def get_behavioral_subject() -> str:
    return BEHAVIORAL_SUBJECT

