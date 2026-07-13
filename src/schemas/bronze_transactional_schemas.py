from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Some nullable Avro union fields are delivered in JSON form as:
#
# {
#     "string": "actual value"
# }
#
# Therefore, they must initially be represented as a nested StructType.
NULLABLE_STRING_SCHEMA = StructType(
    [
        StructField(
            "string",
            StringType(),
            True,
        )
    ]
)


CATEGORIES_SCHEMA = StructType(
    [
        StructField("category_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "parent_category_id",
            NULLABLE_STRING_SCHEMA,
            True,
        ),
    ]
)


ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("item_total_amount", DoubleType(), True),
    ]
)


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField(
            "payment_method",
            NULLABLE_STRING_SCHEMA,
            True,
        ),
    ]
)


PRODUCT_PRICE_HISTORY_SCHEMA = StructType(
    [
        StructField("price_history_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("valid_from", StringType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)


PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", DoubleType(), True),
    ]
)


USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField(
            "loyalty_tier",
            NULLABLE_STRING_SCHEMA,
            True,
        ),
        StructField(
            "location",
            NULLABLE_STRING_SCHEMA,
            True,
        ),
    ]
)


# Logical table names are used internally and for MinIO folder names.
TRANSACTIONAL_SCHEMAS = {
    "categories": CATEGORIES_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "product_price_history": PRODUCT_PRICE_HISTORY_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "users": USERS_SCHEMA,
}


# Physical Kafka topic names are kept separately.
#
# Example:
#   logical table: orders
#   Kafka topic: transactional.orders
#   MinIO folder: bronze/transactional/orders/
TRANSACTIONAL_TOPICS = {
    "categories": "transactional.categories",
    "order_items": "transactional.order_items",
    "orders": "transactional.orders",
    "product_price_history": (
        "transactional.product_price_history"
    ),
    "products": "transactional.products",
    "users": "transactional.users",
}


def validate_transactional_configuration() -> None:
    """
    Ensure every configured logical table has both:
    - a Spark schema;
    - a physical Kafka topic name.
    """

    schema_tables = set(TRANSACTIONAL_SCHEMAS)
    topic_tables = set(TRANSACTIONAL_TOPICS)

    if schema_tables != topic_tables:
        missing_topics = schema_tables - topic_tables
        missing_schemas = topic_tables - schema_tables

        raise ValueError(
            "Transactional configuration mismatch. "
            f"Missing topic mappings: {sorted(missing_topics)}. "
            f"Missing schemas: {sorted(missing_schemas)}."
        )