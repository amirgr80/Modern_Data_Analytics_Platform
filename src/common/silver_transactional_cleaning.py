import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


NULL_STRING_VALUES = [
    "",
    "null",
    "none",
    "n/a",
    "na",
    "unknown",
    "-",
]


STRING_FIELDS = {
    "categories": [
        "category_id",
        "name",
        "parent_category_id",
    ],
    "users": [
        "user_id",
        "username",
        "email",
        "device",
        "loyalty_tier",
        "location",
    ],
    "products": [
        "product_id",
        "name",
        "category",
    ],
    "orders": [
        "order_id",
        "user_id",
        "status",
        "payment_method",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
    ],
    "product_price_history": [
        "price_history_id",
        "product_id",
    ],
}


LOWERCASE_FIELDS = {
    "categories": [],
    "users": [
        "email",
        "device",
        "loyalty_tier",
    ],
    "products": [],
    "orders": [
        "status",
        "payment_method",
    ],
    "order_items": [],
    "product_price_history": [],
}


UPPERCASE_ID_FIELDS = {
    "categories": [
        "category_id",
        "parent_category_id",
    ],
    "users": [
        "user_id",
    ],
    "products": [
        "product_id",
    ],
    "orders": [
        "order_id",
        "user_id",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
    ],
    "product_price_history": [
        "price_history_id",
        "product_id",
    ],
}


DECIMAL_FIELDS = {
    "categories": {},
    "users": {},
    "products": {
        "price": "decimal(18,2)",
        "popularity_score": "decimal(10,2)",
    },
    "orders": {
        "total": "decimal(18,2)",
    },
    "order_items": {
        "unit_price": "decimal(18,2)",
        "item_total_amount": "decimal(18,2)",
    },
    "product_price_history": {
        "price": "decimal(18,2)",
    },
}


def validate_table_name(table_name: str) -> None:
    if table_name not in STRING_FIELDS:
        raise ValueError(
            f"Unsupported transactional table: {table_name}"
        )


def clean_string_fields(
    df: DataFrame,
    table_name: str,
) -> DataFrame:

    #convert common null-like values to null

    for field_name in STRING_FIELDS[table_name]:
        if field_name not in df.columns:
            continue

        normalized_value = F.trim(
            F.col(field_name)
        )

        df = df.withColumn(
            field_name,
            F.when(
                F.lower(normalized_value).isin(
                    *NULL_STRING_VALUES
                ),
                F.lit(None),
            ).otherwise(
                normalized_value
            ),
        )

    return df


def normalize_case(
    df: DataFrame,
    table_name: str,
) -> DataFrame:

    # Standardize IDs, enums and selected text fields.


    for field_name in UPPERCASE_ID_FIELDS[table_name]:
        if field_name in df.columns:
            df = df.withColumn(
                field_name,
                F.upper(
                    F.trim(F.col(field_name))
                ),
            )

    for field_name in LOWERCASE_FIELDS[table_name]:
        if field_name in df.columns:
            df = df.withColumn(
                field_name,
                F.lower(
                    F.trim(F.col(field_name))
                ),
            )

    return df


def normalize_numeric_fields(
    df: DataFrame,
    table_name: str,
) -> DataFrame:

    for field_name, target_type in DECIMAL_FIELDS[
        table_name
    ].items():
        if field_name in df.columns:
            df = df.withColumn(
                field_name,
                F.col(field_name).cast(target_type),
            )

    return df


def clean_categories(
    df: DataFrame,
) -> DataFrame:
    return df.withColumn(
        "name",
        F.initcap(F.col("name")),
    )


def clean_users(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "username",
            F.trim(F.col("username")),
        )
        .withColumn(
            "signup_date",
            F.to_date(F.col("signup_date")),
        )
    )


def clean_products(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "name",
            F.trim(F.col("name")),
        )
        .withColumn(
            "inventory",
            F.col("inventory").cast("int"),
        )
    )


def clean_orders(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "event_timestamp",
            F.col("event_timestamp").cast("timestamp"),
        )
        .withColumn(
            "order_date",
            F.to_date(F.col("event_timestamp")),
        )
    )


def clean_order_items(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "quantity",
            F.col("quantity").cast("int"),
        )
        .withColumn(
            "calculated_item_amount",
            (
                F.col("quantity")
                * F.col("unit_price")
            ).cast("decimal(18,2)"),
        )
    )


def clean_product_price_history(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "valid_from_timestamp",
            F.col(
                "valid_from_timestamp"
            ).cast("timestamp"),
        )
        .withColumn(
            "valid_to",
            F.col("valid_to").cast("timestamp"),
        )
        .withColumn(
            "is_current",
            F.col("is_current").cast("boolean"),
        )
    )


def remove_exact_duplicates(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    # Remove duplicate business records using the natural record ID.
    # Kafka technical metadata is intentionally ignored.


    record_id_fields = {
        "categories": ["category_id"],
        "users": ["user_id"],
        "products": ["product_id"],
        "orders": ["order_id"],
        "order_items": ["order_item_id"],
        "product_price_history": [
            "price_history_id",
        ],
    }

    return df.dropDuplicates(
        record_id_fields[table_name]
    )


def add_cleaning_metadata(
    df: DataFrame,
) -> DataFrame:
    return df.withColumn(
        "silver_cleaned_at",
        F.current_timestamp(),
    )


def clean_transactional_data(
    df: DataFrame,
    table_name: str,
) -> DataFrame:

    # Clean and standardized DataFrame ready for Kimball transformations


    validate_table_name(table_name)

    cleaned_df = clean_string_fields(
        df=df,
        table_name=table_name,
    )

    cleaned_df = normalize_case(
        df=cleaned_df,
        table_name=table_name,
    )

    cleaned_df = normalize_numeric_fields(
        df=cleaned_df,
        table_name=table_name,
    )

    table_cleaners = {
        "categories": clean_categories,
        "users": clean_users,
        "products": clean_products,
        "orders": clean_orders,
        "order_items": clean_order_items,
        "product_price_history": (
            clean_product_price_history
        ),
    }

    cleaned_df = table_cleaners[table_name](
        cleaned_df
    )

    cleaned_df = remove_exact_duplicates(
        df=cleaned_df,
        table_name=table_name,
    )

    cleaned_df = add_cleaning_metadata(
        cleaned_df
    )

    logger.info(
        "Cleaning completed for table '%s'.",
        table_name,
    )

    return cleaned_df