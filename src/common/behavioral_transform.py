from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    dayofmonth,
    expr,
    lit,
    lower,
    month,
    to_date,
    to_timestamp,
    trim,
    year,
)

from schemas.behavioral_schemas import BEHAVIORAL_ALL_COLUMNS


STRING_COLUMNS_TO_TRIM = [
    "timestamp",
    "user_id",
    "event_type",
    "device",
    "session_id",
    "product_id",
    "shipping_method",
    "order_id",
    "fulfillment_speed",
    "url_path",
    "payment_type",
    "error_code",
    "query",
    "wishlist_name",
]


def decode_confluent_avro(kafka_df: DataFrame, avro_schema: str) -> DataFrame:
    """
    Decode Kafka messages that use Confluent Avro wire format.

    Confluent Avro format:
    - byte 0: magic byte
    - bytes 1-4: schema id
    - bytes 5-end: Avro payload

    Spark from_avro needs only the Avro payload, so the first 5 bytes are removed.
    """

    avro_payload = expr("substring(value, 6, length(value) - 5)")
    decoded_value = from_avro(avro_payload, avro_schema)

    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            decoded_value.alias("data"),
        )
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "data.*",
        )
    )


def trim_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim leading and trailing spaces from string columns.
    """

    for column_name in STRING_COLUMNS_TO_TRIM:
        if column_name in df.columns:
            df = df.withColumn(column_name, trim(col(column_name)))

    return df


def standardize_behavioral_events(df: DataFrame) -> DataFrame:
    """
    Apply basic Bronze transformations:
    - trim string fields
    - normalize event_type and device
    - convert timestamp string to Spark TimestampType
    - create event_date
    """

    df = trim_string_columns(df)

    return (
        df
        .withColumn("event_type", lower(col("event_type")))
        .withColumn("device", lower(col("device")))
        .withColumn(
            "event_timestamp",
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("timestamp")),
            ),
        )
        .withColumn("event_date", to_date(col("event_timestamp")))
    )


def add_bronze_metadata(df: DataFrame) -> DataFrame:
    """
    Add technical metadata required for traceability in the Bronze layer.
    """

    return (
        df
        .withColumn("source_topic", lit("behavioral.events"))
        .withColumn("source_subject", lit("behavioral.events-value"))
        .withColumn("bronze_ingestion_timestamp", current_timestamp())
    )


def add_validation_columns(df: DataFrame) -> DataFrame:
    """
    Add simple data quality checks.

    Invalid rows are not dropped in Bronze.
    They are kept with validation metadata so Silver can decide how to clean them.
    """

    validation_expr = """
        filter(
            array(
                case
                    when timestamp is null or trim(timestamp) = ''
                    then 'missing_timestamp'
                end,
                case
                    when user_id is null or trim(user_id) = ''
                    then 'missing_user_id'
                end,
                case
                    when event_type is null or trim(event_type) = ''
                    then 'missing_event_type'
                end,
                case
                    when device is null or trim(device) = ''
                    then 'missing_device'
                end,
                case
                    when session_id is null or trim(session_id) = ''
                    then 'missing_session_id'
                end,
                case
                    when event_timestamp is null
                    then 'invalid_event_timestamp'
                end
            ),
            x -> x is not null
        )
    """

    return (
        df
        .withColumn("validation_errors", expr(validation_expr))
        .withColumn("is_valid", expr("size(validation_errors) = 0"))
    )


def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add date-based partition columns for Bronze Parquet storage.
    """

    return (
        df
        .withColumn("year", year(col("event_timestamp")))
        .withColumn("month", month(col("event_timestamp")))
        .withColumn("day", dayofmonth(col("event_timestamp")))
    )


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Select a stable column order for Bronze output.
    """

    metadata_columns = [
        "source_topic",
        "source_subject",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingestion_timestamp",
        "event_timestamp",
        "event_date",
        "is_valid",
        "validation_errors",
        "year",
        "month",
        "day",
    ]

    behavioral_columns = [
        column_name
        for column_name in BEHAVIORAL_ALL_COLUMNS
        if column_name in df.columns
    ]

    final_columns = metadata_columns + behavioral_columns

    return df.select(*final_columns)


def transform_bronze_behavioral(kafka_df: DataFrame, avro_schema: str) -> DataFrame:
    """
    Full Bronze transformation pipeline for behavioral events.

    Input:
        Raw Kafka streaming DataFrame

    Output:
        Bronze-ready DataFrame with decoded, standardized, validated,
        and partition-ready behavioral events.
    """

    decoded_df = decode_confluent_avro(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
    )

    standardized_df = standardize_behavioral_events(decoded_df)

    metadata_df = add_bronze_metadata(standardized_df)

    validated_df = add_validation_columns(metadata_df)

    partitioned_df = add_partition_columns(validated_df)

    return select_final_columns(partitioned_df)