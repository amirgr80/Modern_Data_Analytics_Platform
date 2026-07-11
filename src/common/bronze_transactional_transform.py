import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    from_json,
    lit,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


NULLABLE_STRING_FIELDS_BY_TABLE = {
    "categories": ["parent_category_id"],
    "orders": ["payment_method"],
    "users": ["loyalty_tier", "location"],
    "order_items": [],
    "product_price_history": [],
    "products": [],
}


PARTITION_SOURCE_BY_TABLE = {
    "orders": "event_timestamp",
    "product_price_history": "valid_from_timestamp",
    "users": "signup_date",
    "categories": "_kafka_timestamp",
    "order_items": "_kafka_timestamp",
    "products": "_kafka_timestamp",
}


def parse_kafka_json(
    kafka_df: DataFrame,
    schema: StructType,
) -> DataFrame:
    """
    Parse the Kafka message value using the exact schema
    defined for the source topic.

    Kafka timestamp is retained because some transactional
    topics do not contain a business timestamp in their payload.
    """

    return (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                schema,
            ).alias("data"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            "data.*",
            "_kafka_timestamp",
        )
    )


def flatten_nullable_string_fields(
    df: DataFrame,
    fields: list[str],
) -> DataFrame:
    """
    Convert nullable union-like objects such as:

        {"string": "value"}

    to a normal nullable string column.

    If the object or its inner value is null,
    the resulting column remains null.
    """

    for field_name in fields:
        if field_name in df.columns:
            df = df.withColumn(
                field_name,
                col(f"{field_name}.string"),
            )

    return df


def standardize_transactional_dates(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Apply only the initial date and timestamp conversions
    required in the Bronze layer.
    """

    if table_name == "orders":
        df = df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp")),
        )

    elif table_name == "product_price_history":
        df = df.withColumn(
            "valid_from_timestamp",
            to_timestamp(col("valid_from")),
        )

    elif table_name == "users":
        df = df.withColumn(
            "signup_date",
            to_date(col("signup_date")),
        )

    return df


def add_bronze_metadata(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Add minimal technical metadata for lineage and ingestion tracking.
    """

    return (
        df
        .withColumn("source_table", lit(table_name))
        .withColumn(
            "bronze_ingestion_timestamp",
            current_timestamp(),
        )
    )


def add_partition_date(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Create a single partition date column in yyyyMMdd format.

    Examples:
        20250213
        20260710
    """

    partition_source = PARTITION_SOURCE_BY_TABLE.get(
        table_name,
        "_kafka_timestamp",
    )

    return df.withColumn(
        "partition_date",
        date_format(
            col(partition_source),
            "yyyyMMdd",
        ),
    )


def transform_bronze_transactional(
    kafka_df: DataFrame,
    schema: StructType,
    table_name: str,
) -> DataFrame:
    """
    Execute the complete initial Bronze transformation
    for one transactional Kafka topic.
    """

    if table_name not in NULLABLE_STRING_FIELDS_BY_TABLE:
        raise ValueError(
            f"Unsupported transactional table: {table_name}"
        )

    parsed_df = parse_kafka_json(
        kafka_df=kafka_df,
        schema=schema,
    )

    flattened_df = flatten_nullable_string_fields(
        df=parsed_df,
        fields=NULLABLE_STRING_FIELDS_BY_TABLE[table_name],
    )

    standardized_df = standardize_transactional_dates(
        df=flattened_df,
        table_name=table_name,
    )

    metadata_df = add_bronze_metadata(
        df=standardized_df,
        table_name=table_name,
    )

    final_df = add_partition_date(
        df=metadata_df,
        table_name=table_name,
    )

    logger.info(
        "Bronze transformation configured for table '%s'",
        table_name,
    )

    return final_df