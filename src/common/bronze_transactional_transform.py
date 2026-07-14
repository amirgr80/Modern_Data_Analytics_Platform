from __future__ import annotations
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
from common.bronze_transactional_avro import decode_confluent_avro


logger = logging.getLogger(__name__)


NULLABLE_STRING_FIELDS_BY_TABLE = {
    "categories": ["parent_category_id"],
    "order_items": [],
    "orders": ["payment_method"],
    "product_price_history": [],
    "products": [],
    "users": ["loyalty_tier", "location"],
}


PARTITION_SOURCE_BY_TABLE = {
    "categories": "_kafka_timestamp",
    "order_items": "_kafka_timestamp",
    "orders": "event_timestamp",
    "product_price_history": "valid_from_timestamp",
    "products": "_kafka_timestamp",
    "users": "signup_date",
}


def parse_kafka_json(
    kafka_df: DataFrame,
    schema: StructType,
) -> DataFrame:
    """
    Parse the Kafka binary payload from the `value` column
    using the exact schema assigned to the source topic.

    Kafka metadata is retained for lineage and for determining
    the partition date when the payload has no business timestamp.
    """

    return (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                schema,
            ).alias("data"),
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .select(
            "data.*",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
        )
    )


def flatten_nullable_string_fields(
    df: DataFrame,
    fields: list[str],
) -> DataFrame:
    """
    Flatten nullable union-like structures.

    Input example:
        {"string": "Gold"}

    Output:
        "Gold"

    If the outer struct or inner value is null, Spark preserves
    the value as null without raising an error.
    """

    for field_name in fields:
        if field_name not in df.columns:
            raise ValueError(
                f"Nullable field '{field_name}' does not exist "
                "in the parsed DataFrame."
            )

        field_type = dict(df.dtypes)[field_name]

        if field_type.startswith("struct"):
            df = df.withColumn(
                field_name,
                col(f"{field_name}.string"),
            )
        elif field_type == "string":
            df = df.withColumn(
                field_name,
                col(field_name),
            )
        else:
            raise ValueError(
                f"Unsupported nullable field type for '{field_name}': "
                f"{field_type}"
            )

    return df


def standardize_transactional_dates(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """
    Perform only the lightweight date conversions required
    by the Bronze layer.
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
        .withColumn(
            "source_table",
            lit(table_name),
        )
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
    Create one technical partition field in yyyyMMdd format.

    The writer uses this value to construct paths such as:

        bronze/transactional/orders/20260712/
    """

    if table_name not in PARTITION_SOURCE_BY_TABLE:
        raise ValueError(
            f"No partition source configured for table "
            f"'{table_name}'."
        )

    partition_source = PARTITION_SOURCE_BY_TABLE[table_name]

    if partition_source not in df.columns:
        raise ValueError(
            f"Partition source column '{partition_source}' "
            f"does not exist for table '{table_name}'."
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
    avro_schema: str,
    schema_id: int,
    table_name: str,
) -> DataFrame:
    """
    Execute the complete Bronze transformation for one
    transactional Kafka topic.

    Processing order:
        1. Parse Kafka value as JSON.
        2. Apply the topic schema.
        3. Flatten nullable string fields.
        4. Standardize technical dates and timestamps.
        5. Add Bronze metadata.
        6. Generate the yyyyMMdd partition date.
    """

    if table_name not in NULLABLE_STRING_FIELDS_BY_TABLE:
        raise ValueError(
            f"Unsupported transactional table: '{table_name}'."
        )

    parsed_df = decode_confluent_avro(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
        expected_schema_id=schema_id,
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
        "Bronze transformation configured for table '%s'.",
        table_name,
    )

    return final_df
