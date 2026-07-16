import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType


logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    valid_df: DataFrame
    rejected_df: DataFrame
    quality_issues_df: DataFrame


EXPECTED_COLUMNS = {
    "categories": [
        "category_id",
        "name",
        "parent_category_id",
    ],
    "users": [
        "user_id",
        "username",
        "email",
        "signup_date",
        "device",
        "loyalty_tier",
        "location",
    ],
    "products": [
        "product_id",
        "name",
        "price",
        "category",
        "inventory",
        "popularity_score",
    ],
    "orders": [
        "order_id",
        "user_id",
        "timestamp",
        "event_timestamp",
        "total",
        "status",
        "payment_method",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "item_total_amount",
    ],
    "product_price_history": [
        "price_history_id",
        "product_id",
        "price",
        "valid_from",
        "valid_to",
        "valid_from_timestamp",
        "is_current",
    ],
}


# Only fields without which the record cannot be used.
REQUIRED_FIELDS = {
    "categories": [
        "category_id",
        "name",
    ],
    "users": [
        "user_id",
    ],
    "products": [
        "product_id",
        "name",
        "price",
    ],
    "orders": [
        "order_id",
        "user_id",
        "event_timestamp",
        "total",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    ],
    "product_price_history": [
        "price_history_id",
        "product_id",
        "price",
        "valid_from_timestamp",
    ],
}


RECORD_ID_FIELDS = {
    "categories": "category_id",
    "users": "user_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "product_price_history": "price_history_id",
}


# Casts match the actual Parquet file types.
TARGET_TYPES = {
    "categories": {
        "category_id": "string",
        "name": "string",
        "parent_category_id": "string",
    },
    "users": {
        "user_id": "string",
        "username": "string",
        "email": "string",
        "signup_date": "date",
        "device": "string",
        "loyalty_tier": "string",
        "location": "string",
    },
    "products": {
        "product_id": "string",
        "name": "string",
        "price": "decimal(18,2)",
        "category": "string",
        "inventory": "int",
        "popularity_score": "decimal(10,2)",
    },
    "orders": {
        "order_id": "string",
        "user_id": "string",
        "timestamp": "timestamp",
        "event_timestamp": "timestamp",
        "total": "decimal(18,2)",
        "status": "string",
        "payment_method": "string",
    },
    "order_items": {
        "order_item_id": "string",
        "order_id": "string",
        "product_id": "string",
        "quantity": "int",
        "unit_price": "decimal(18,2)",
        "item_total_amount": "decimal(18,2)",
    },
    "product_price_history": {
        "price_history_id": "string",
        "product_id": "string",
        "price": "decimal(18,2)",
        "valid_from": "timestamp",
        "valid_to": "timestamp",
        "valid_from_timestamp": "timestamp",
        "is_current": "boolean",
    },
}


ID_PATTERNS = {
    "category_id": r"^C[0-9]+$",
    "parent_category_id": r"^C[0-9]+$",
    "user_id": r"^U[0-9]+$",
    "product_id": r"^P[0-9]+$",
    "order_id": r"^O[0-9]+$",
    "order_item_id": r"^OI[0-9]+$",
    "price_history_id": r"^PH[0-9]+$",
}


ID_FIELDS = {
    "categories": [
        "category_id",
        "parent_category_id",
    ],
    "users": ["user_id"],
    "products": ["product_id"],
    "orders": ["order_id", "user_id"],
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


def validate_table_name(table_name: str) -> None:
    if table_name not in EXPECTED_COLUMNS:
        raise ValueError(
            f"Unsupported transactional table: {table_name}"
        )


def build_original_record(df: DataFrame):
    """
    Converts the entire original record to JSON.

    Binary columns such as _raw_value are Base64-encoded first
    to prevent to_json from failing.
    """

    json_columns = []

    for field in df.schema.fields:
        column = F.col(field.name)

        if isinstance(field.dataType, BinaryType):
            column = F.base64(column)

        json_columns.append(
            column.alias(field.name)
        )

    return F.to_json(
        F.struct(*json_columns)
    )


def prepare_columns(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Adds missing optional columns as null and performs safe casting.

    try_cast converts invalid values to null instead of failing the Spark job.
    """

    expected_columns = EXPECTED_COLUMNS[table_name]

    for field_name in expected_columns:
        if field_name not in df.columns:
            df = df.withColumn(
                field_name,
                F.lit(None),
            )

    for field_name, target_type in TARGET_TYPES[table_name].items():
        df = df.withColumn(
            field_name,
            F.expr(
                f"try_cast(`{field_name}` as {target_type})"
            ),
        )

    return df


def normalize_timestamps(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Repairs timestamps where possible.

    If an order timestamp is null or from 1970,
    it is replaced with the Kafka timestamp.
    """

    df = (
        df
        .withColumn(
            "_timestamp_repaired",
            F.lit(False),
        )
        .withColumn(
            "_timestamp_repair_reason",
            F.lit(None).cast("string"),
        )
    )

    if table_name == "orders":
        original_timestamp = F.coalesce(
            F.col("event_timestamp"),
            F.col("timestamp"),
        )

        invalid_timestamp = (
            original_timestamp.isNull()
            | (F.year(original_timestamp) <= 1970)
        )

        kafka_timestamp_valid = (
            F.col("_kafka_timestamp").isNotNull()
            & (F.year("_kafka_timestamp") > 1970)
        )

        df = (
            df
            .withColumn(
                "_timestamp_repaired",
                invalid_timestamp
                & kafka_timestamp_valid,
            )
            .withColumn(
                "_timestamp_repair_reason",
                F.when(
                    invalid_timestamp
                    & kafka_timestamp_valid,
                    F.lit(
                        "event_timestamp replaced "
                        "with kafka timestamp"
                    ),
                ),
            )
            .withColumn(
                "event_timestamp",
                F.when(
                    invalid_timestamp
                    & kafka_timestamp_valid,
                    F.col("_kafka_timestamp"),
                ).otherwise(
                    original_timestamp
                ),
            )
        )

    elif table_name == "product_price_history":
        df = df.withColumn(
            "valid_from_timestamp",
            F.coalesce(
                F.col("valid_from_timestamp"),
                F.col("valid_from"),
            ),
        )

    return df


def build_rejection_rules(
    table_name: str,
) -> list:
    """
    Critical errors that prevent the record from being used.
    """

    rules = []

    for field_name in REQUIRED_FIELDS[table_name]:
        condition = F.col(field_name).isNull()

        if TARGET_TYPES[table_name][field_name] == "string":
            condition = (
                condition
                | (F.trim(F.col(field_name)) == "")
            )

        rules.append(
            F.when(
                condition,
                F.lit(
                    f"{field_name}:required_value_missing"
                ),
            )
        )

    if table_name == "products":
        rules.append(
            F.when(
                F.col("price") < 0,
                F.lit("price:negative_value"),
            )
        )

    elif table_name == "orders":
        rules.append(
            F.when(
                F.col("total") < 0,
                F.lit("total:negative_value"),
            )
        )

    elif table_name == "order_items":
        rules.extend(
            [
                F.when(
                    F.col("quantity") <= 0,
                    F.lit("quantity:must_be_positive"),
                ),
                F.when(
                    F.col("unit_price") < 0,
                    F.lit("unit_price:negative_value"),
                ),
                F.when(
                    F.col("item_total_amount") < 0,
                    F.lit(
                        "item_total_amount:negative_value"
                    ),
                ),
            ]
        )

    elif table_name == "product_price_history":
        rules.append(
            F.when(
                F.col("price") < 0,
                F.lit("price:negative_value"),
            )
        )

    return rules


def build_warning_rules(
    table_name: str,
) -> list:
    """
    Non-critical issues; the record is not rejected.
    """

    rules = []

    for field_name in ID_FIELDS[table_name]:
        pattern = ID_PATTERNS[field_name]

        rules.append(
            F.when(
                F.col(field_name).isNotNull()
                & (
                    F.trim(F.col(field_name))
                    != ""
                )
                & ~F.trim(
                    F.col(field_name)
                ).rlike(pattern),
                F.lit(
                    f"{field_name}:unusual_id_format"
                ),
            )
        )

    if table_name == "users":
        rules.append(
            F.when(
                F.col("email").isNotNull()
                & ~F.trim(F.col("email")).rlike(
                    r"^[A-Za-z0-9._%+-]+@"
                    r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                ),
                F.lit("email:unusual_format"),
            )
        )

    if table_name == "orders":
        rules.append(
            F.when(
                F.col("_timestamp_repaired"),
                F.lit("event_timestamp:repaired"),
            )
        )

    if table_name == "product_price_history":
        rules.append(
            F.when(
                F.col("valid_to").isNotNull()
                & (
                    F.col("valid_to")
                    < F.col("valid_from_timestamp")
                ),
                F.lit("valid_to:before_valid_from"),
            )
        )

    return rules


def build_quality_issues(
    validated_df: DataFrame,
    table_name: str,
) -> DataFrame:
    record_id_field = RECORD_ID_FIELDS[table_name]

    return (
        validated_df
        .filter(
            (F.size("validation_errors") > 0)
            | (F.size("validation_warnings") > 0)
        )
        .select(
            F.lit(table_name).alias("source_table"),
            F.col(record_id_field)
            .cast("string")
            .alias("record_id"),
            F.when(
                F.size("validation_errors") > 0,
                F.lit("REJECTED"),
            )
            .when(
                F.col("_timestamp_repaired"),
                F.lit("REPAIRED"),
            )
            .otherwise(
                F.lit("WARNING"),
            )
            .alias("issue_status"),
            F.col("validation_errors"),
            F.col("validation_warnings"),
            F.col("_timestamp_repair_reason")
            .alias("repair_description"),
            F.col("_original_record")
            .alias("original_record"),
            F.current_timestamp().alias("detected_at"),
        )
    )


def validate_transactional_data(
    df: DataFrame,
    table_name: str,
) -> ValidationResult:
    validate_table_name(table_name)

    # Preserve original record before any casting or transformation
    df = df.withColumn(
        "_original_record",
        build_original_record(df),
    )

    prepared_df = prepare_columns(
        df=df,
        table_name=table_name,
    )

    prepared_df = normalize_timestamps(
        df=prepared_df,
        table_name=table_name,
    )

    rejection_rules = build_rejection_rules(
        table_name
    )

    warning_rules = build_warning_rules(
        table_name
    )

    validated_df = (
        prepared_df
        .withColumn(
            "validation_errors",
            F.array_compact(
                F.array(*rejection_rules)
            ),
        )
        .withColumn(
            "validation_warnings",
            F.array_compact(
                F.array(*warning_rules)
            ),
        )
    )

    rejected_df = validated_df.filter(
        F.size("validation_errors") > 0
    )

    valid_df = (
        validated_df
        .filter(
            F.size("validation_errors") == 0
        )
        .drop(
            "validation_errors",
            "_original_record",
            "_timestamp_repair_reason",
        )
    )

    quality_issues_df = build_quality_issues(
        validated_df=validated_df,
        table_name=table_name,
    )

    logger.info(
        "Basic validation completed for table '%s'.",
        table_name,
    )

    return ValidationResult(
        valid_df=valid_df,
        rejected_df=rejected_df,
        quality_issues_df=quality_issues_df,
    )