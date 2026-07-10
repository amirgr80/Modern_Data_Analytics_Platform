import json


BEHAVIORAL_TOPIC = "behavioral.events"
BEHAVIORAL_SUBJECT = "behavioral.events-value"


BEHAVIORAL_AVRO_SCHEMA = {
    "type": "record",
    "name": "BehavioralEvent",
    "namespace": "behavioral",
    "fields": [
        {
            "name": "timestamp",
            "type": "string",
        },
        {
            "name": "user_id",
            "type": "string",
        },
        {
            "name": "event_type",
            "type": "string",
        },
        {
            "name": "device",
            "type": "string",
        },
        {
            "name": "session_id",
            "type": "string",
        },
        {
            "name": "product_id",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "quantity",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "cart_total_items",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "cart_items",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "cart_item",
                        "fields": [
                            {
                                "name": "product_id",
                                "type": "string",
                            },
                            {
                                "name": "price",
                                "type": "double",
                            },
                            {
                                "name": "quantity",
                                "type": "int",
                            },
                        ],
                    },
                },
            ],
            "default": None,
        },
        {
            "name": "cart_value",
            "type": ["null", "double"],
            "default": None,
        },
        {
            "name": "shipping_method",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "order_id",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "fulfillment_speed",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "url_path",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "duration_sec",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "http_status",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "payment_type",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "success",
            "type": ["null", "boolean"],
            "default": None,
        },
        {
            "name": "error_code",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "query",
            "type": ["null", "string"],
            "default": None,
        },
        {
            "name": "results_count",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "clicked_position",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "rating",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "text_length",
            "type": ["null", "int"],
            "default": None,
        },
        {
            "name": "wishlist_name",
            "type": ["null", "string"],
            "default": None,
        },
    ],
}


BEHAVIORAL_REQUIRED_COLUMNS = [
    "timestamp",
    "user_id",
    "event_type",
    "device",
    "session_id",
]


BEHAVIORAL_OPTIONAL_COLUMNS = [
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


def get_behavioral_avro_schema() -> str:
    return json.dumps(BEHAVIORAL_AVRO_SCHEMA)