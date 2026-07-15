import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_compact,
    col,
    coalesce,
    current_timestamp,
    lit,
    lower,
    size,
    struct,
    to_json,
    trim,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """
    valid_df:
        Records allowed to continue to Cleaning.

    rejected_df:
        Records that cannot continue to Cleaning.

    quality_issues_df:
        Rejected and repaired records prepared for storage
        in the Iceberg data-quality table.
    """

    valid_df: DataFrame
    rejected_df: DataFrame
    quality_issues_df: DataFrame


# ------------------------------------------------------------------
# Schema expected from the actual Bronze Parquet files
# ------------------------------------------------------------------

EXPECTED_SCHEMAS = {
    "categories": {
        "category_id": StringType(),
        "name": StringType(),
        "parent_category_id": StringType(),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
    "users": {
        "user_id": StringType(),
        "username": StringType(),
        "email": StringType(),
        "signup_date": DateType(),
        "device": StringType(),
        "loyalty_tier": StringType(),
        "location": StringType(),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
    "products": {
        "product_id": StringType(),
        "name": StringType(),
        "price": DecimalType(10, 2),
        "category": StringType(),
        "inventory": IntegerType(),
        "popularity_score": DecimalType(4, 2),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
    "orders": {
        "order_id": StringType(),
        "user_id": StringType(),
        "timestamp": TimestampType(),
        "total": DecimalType(10, 2),
        "status": StringType(),
        "payment_method": StringType(),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "event_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
    "order_items": {
        "order_item_id": StringType(),
        "order_id": StringType(),
        "product_id": StringType(),
        "quantity": IntegerType(),
        "unit_price": DecimalType(10, 2),
        "item_total_amount": DecimalType(10, 2),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
    "product_price_history": {
        "price_history_id": StringType(),
        "product_id": StringType(),
        "price": DecimalType(10, 2),
        "valid_from": TimestampType(),
        "valid_to": TimestampType(),
        "is_current": BooleanType(),
        "_kafka_topic": StringType(),
        "_kafka_partition": IntegerType(),
        "_kafka_offset": LongType(),
        "_kafka_timestamp": TimestampType(),
        "valid_from_timestamp": TimestampType(),
        "source_table": StringType(),
        "bronze_ingestion_timestamp": TimestampType(),
        "partition_date": StringType(),
    },
}


REQUIRED_FIELDS = {
    "categories": [
        "category_id",
        "name",
    ],
    "users": [
        "user_id",
        "username",
        "email",
        "signup_date",
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
        "status",
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
        "valid_from_timestamp",
        "is_current",
    ],
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


RECORD_ID_FIELDS = {
    "categories": "category_id",
    "users": "user_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "product_price_history": "price_history_id",
}


VALID_ORDER_STATUSES = [
    "created",
    "shipped",
    "delivered",
    "cancelled",
    "refunded",
]


VALID_LOYALTY_TIERS = [
    "bronze",
    "silver",
    "gold",
    "platinum",
]


def validate_table_name(table_name: str) -> None:
    if table_name not in EXPECTED_SCHEMAS:
        raise ValueError(
            f"Unsupported transactional table: '{table_name}'."
        )


def validate_parquet_schema(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Validate the DataFrame structure against the real Bronze
    Parquet structure.

    Missing columns or incompatible column types stop the job,
    because these are file-level/schema-level errors rather than
    individual bad records.
    """

    validate_table_name(table_name)

    expected_schema = EXPECTED_SCHEMAS[table_name]
    actual_schema = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing_columns = (
        set(expected_schema) - set(actual_schema)
    )

    if missing_columns:
        raise ValueError(
            f"Table '{table_name}' is missing columns: "
            f"{sorted(missing_columns)}"
        )

    incompatible_types = []

    for field_name, expected_type in expected_schema.items():
        actual_type = actual_schema[field_name]

        if actual_type != expected_type:
            incompatible_types.append(
                f"{field_name}: expected "
                f"{expected_type.simpleString()}, got "
                f"{actual_type.simpleString()}"
            )

    if incompatible_types:
        raise ValueError(
            f"Table '{table_name}' has incompatible Parquet "
            f"types: {incompatible_types}"
        )


def normalize_order_timestamp(
    df: DataFrame,
) -> DataFrame:
    """
    Repair the known Orders timestamp problem.

    Current Bronze files may contain:
        1970-01-01 00:00:00

    When this happens, Kafka timestamp is used as a temporary
    fallback and the record is marked as repaired.

    This preserves pipeline continuity while keeping the quality
    problem visible for later analysis.
    """

    invalid_event_timestamp = (
        col("event_timestamp").isNull()
        | (year(col("event_timestamp")) <= 1970)
    )

    kafka_timestamp_available = (
        col("_kafka_timestamp").isNotNull()
        & (year(col("_kafka_timestamp")) > 1970)
    )

    return (
        df
        .withColumn(
            "_timestamp_repaired",
            invalid_event_timestamp
            & kafka_timestamp_available,
        )
        .withColumn(
            "_timestamp_repair_reason",
            when(
                invalid_event_timestamp
                & kafka_timestamp_available,
                lit(
                    "event_timestamp replaced with "
                    "_kafka_timestamp"
                ),
            ),
        )
        .withColumn(
            "event_timestamp",
            when(
                invalid_event_timestamp
                & kafka_timestamp_available,
                col("_kafka_timestamp"),
            ).otherwise(
                col("event_timestamp")
            ),
        )
    )


def prepare_validation_dataframe(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Perform repair operations that must happen before validation.
    """

    if table_name == "orders":
        return normalize_order_timestamp(df)

    return (
        df
        .withColumn(
            "_timestamp_repaired",
            lit(False),
        )
        .withColumn(
            "_timestamp_repair_reason",
            lit(None).cast("string"),
        )
    )


def required_field_errors(
    table_name: str,
) -> list:
    errors = []

    for field_name in REQUIRED_FIELDS[table_name]:
        condition = col(field_name).isNull()

        if isinstance(
            EXPECTED_SCHEMAS[table_name][field_name],
            StringType,
        ):
            condition = (
                condition
                | (trim(col(field_name)) == "")
            )

        errors.append(
            when(
                condition,
                lit(f"{field_name}:required_value_missing"),
            )
        )

    return errors


def id_format_errors(
    table_name: str,
) -> list:
    errors = []

    for field_name in ID_FIELDS[table_name]:
        pattern = ID_PATTERNS[field_name]

        errors.append(
            when(
                col(field_name).isNotNull()
                & (
                    trim(col(field_name)) != ""
                )
                & ~trim(col(field_name)).rlike(pattern),
                lit(f"{field_name}:invalid_id_format"),
            )
        )

    return errors


def numeric_value_errors(
    table_name: str,
) -> list:
    errors = []

    if table_name == "products":
        errors.append(
            when(
                col("price").isNotNull()
                & (col("price") < 0),
                lit("price:negative_value"),
            )
        )

        errors.append(
            when(
                col("inventory").isNotNull()
                & (col("inventory") < 0),
                lit("inventory:negative_value"),
            )
        )

        errors.append(
            when(
                col("popularity_score").isNotNull()
                & (
                    (col("popularity_score") < 0)
                    | (col("popularity_score") > 100)
                ),
                lit("popularity_score:outside_valid_range"),
            )
        )

    elif table_name == "orders":
        errors.append(
            when(
                col("total").isNotNull()
                & (col("total") < 0),
                lit("total:negative_value"),
            )
        )

    elif table_name == "order_items":
        errors.extend(
            [
                when(
                    col("quantity").isNotNull()
                    & (col("quantity") <= 0),
                    lit("quantity:must_be_positive"),
                ),
                when(
                    col("unit_price").isNotNull()
                    & (col("unit_price") < 0),
                    lit("unit_price:negative_value"),
                ),
                when(
                    col("item_total_amount").isNotNull()
                    & (col("item_total_amount") < 0),
                    lit("item_total_amount:negative_value"),
                ),
                when(
                    col("quantity").isNotNull()
                    & col("unit_price").isNotNull()
                    & col("item_total_amount").isNotNull()
                    & (
                        col("item_total_amount")
                        > (
                            col("quantity")
                            * col("unit_price")
                        )
                    ),
                    lit(
                        "item_total_amount:"
                        "greater_than_quantity_times_price"
                    ),
                ),
            ]
        )

    elif table_name == "product_price_history":
        errors.append(
            when(
                col("price").isNotNull()
                & (col("price") < 0),
                lit("price:negative_value"),
            )
        )

    return errors


def business_value_errors(
    table_name: str,
) -> list:
    errors = []

    if table_name == "users":
        errors.extend(
            [
                when(
                    col("email").isNotNull()
                    & ~trim(col("email")).rlike(
                        r"^[A-Za-z0-9._%+-]+@"
                        r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                    ),
                    lit("email:invalid_format"),
                ),
                when(
                    col("loyalty_tier").isNotNull()
                    & ~lower(
                        trim(col("loyalty_tier"))
                    ).isin(*VALID_LOYALTY_TIERS),
                    lit("loyalty_tier:unsupported_value"),
                ),
            ]
        )

    elif table_name == "orders":
        errors.extend(
            [
                when(
                    col("event_timestamp").isNull()
                    | (
                        year(col("event_timestamp"))
                        <= 1970
                    ),
                    lit("event_timestamp:invalid_value"),
                ),
                when(
                    col("status").isNotNull()
                    & ~lower(
                        trim(col("status"))
                    ).isin(*VALID_ORDER_STATUSES),
                    lit("status:unsupported_value"),
                ),
            ]
        )

    elif table_name == "product_price_history":
        errors.extend(
            [
                when(
                    col("valid_from_timestamp").isNull(),
                    lit("valid_from_timestamp:invalid_value"),
                ),
                when(
                    col("valid_to").isNotNull()
                    & (
                        col("valid_to")
                        < col("valid_from_timestamp")
                    ),
                    lit("valid_to:before_valid_from"),
                ),
                when(
                    col("is_current")
                    & col("valid_to").isNotNull(),
                    lit(
                        "valid_to:must_be_null_when_current"
                    ),
                ),
            ]
        )

    return errors


def build_validation_errors(
    table_name: str,
) -> list:
    return (
        required_field_errors(table_name)
        + id_format_errors(table_name)
        + numeric_value_errors(table_name)
        + business_value_errors(table_name)
    )


def build_quality_issues_dataframe(
    validated_df: DataFrame,
    original_columns: list[str],
    table_name: str,
) -> DataFrame:
    """
    Build a uniform structure for the Iceberg quality table.

    The entire original Bronze record is kept as JSON so no bad
    data is lost.
    """

    record_id_field = RECORD_ID_FIELDS[table_name]

    rejected_issues = (
        validated_df
        .filter(size(col("validation_errors")) > 0)
        .select(
            lit(table_name).alias("source_table"),
            col(record_id_field).cast("string").alias(
                "record_id"
            ),
            lit("REJECTED").alias("issue_status"),
            col("validation_errors"),
            lit(None).cast("string").alias(
                "repair_description"
            ),
            to_json(
                struct(
                    *[
                        col(field_name)
                        for field_name in original_columns
                    ]
                )
            ).alias("original_record"),
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("bronze_ingestion_timestamp"),
            current_timestamp().alias("detected_at"),
        )
    )

    repaired_issues = (
        validated_df
        .filter(
            col("_timestamp_repaired")
            & (size(col("validation_errors")) == 0)
        )
        .select(
            lit(table_name).alias("source_table"),
            col(record_id_field).cast("string").alias(
                "record_id"
            ),
            lit("REPAIRED").alias("issue_status"),
            array(
                lit("event_timestamp:epoch_zero")
            ).alias("validation_errors"),
            col("_timestamp_repair_reason").alias(
                "repair_description"
            ),
            to_json(
                struct(
                    *[
                        col(field_name)
                        for field_name in original_columns
                    ]
                )
            ).alias("original_record"),
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("bronze_ingestion_timestamp"),
            current_timestamp().alias("detected_at"),
        )
    )

    return rejected_issues.unionByName(
        repaired_issues
    )


def validate_transactional_data(
    df: DataFrame,
    table_name: str,
) -> ValidationResult:
    """
    Main validation entry point.
    """

    validate_parquet_schema(
        df=df,
        table_name=table_name,
    )

    original_columns = df.columns

    prepared_df = prepare_validation_dataframe(
        df=df,
        table_name=table_name,
    )

    error_rules = build_validation_errors(
        table_name=table_name,
    )

    validated_df = prepared_df.withColumn(
        "validation_errors",
        array_compact(
            array(*error_rules)
        ),
    )

    rejected_df = validated_df.filter(
        size(col("validation_errors")) > 0
    )

    valid_df = (
        validated_df
        .filter(
            size(col("validation_errors")) == 0
        )
        .drop(
            "validation_errors",
            "_timestamp_repair_reason",
        )
    )

    quality_issues_df = build_quality_issues_dataframe(
        validated_df=validated_df,
        original_columns=original_columns,
        table_name=table_name,
    )

    logger.info(
        "Validation completed for table '%s'.",
        table_name,
    )

    return ValidationResult(
        valid_df=valid_df,
        rejected_df=rejected_df,
        quality_issues_df=quality_issues_df,
    )


def write_quality_issues(
    quality_issues_df: DataFrame,
    catalog_name: str = "lakekeeper",
    namespace: str = "silver_quality",
    table_name: str = "transactional_validation_issues",
) -> None:
    """
    Append rejected and repaired records to one Iceberg table.

    Final table:
        lakekeeper.silver_quality.transactional_validation_issues
    """

    if quality_issues_df.rdd.isEmpty():
        logger.info(
            "No validation issues found. Nothing to write."
        )
        return

    full_table_name = (
        f"{catalog_name}.{namespace}.{table_name}"
    )

    spark = quality_issues_df.sparkSession

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {catalog_name}.{namespace}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            source_table STRING,
            record_id STRING,
            issue_status STRING,
            validation_errors ARRAY<STRING>,
            repair_description STRING,
            original_record STRING,
            _kafka_topic STRING,
            _kafka_partition INT,
            _kafka_offset BIGINT,
            _kafka_timestamp TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            detected_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_table)
        """
    )

    (
        quality_issues_df
        .writeTo(full_table_name)
        .append()
    )

    logger.info(
        "Validation issues written to '%s'.",
        full_table_name,
    )