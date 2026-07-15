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
    sha2,
    concat_ws,
)
from schemas.bronze_behavioral_schemas import BEHAVIORAL_ALL_COLUMNS


STRING_COLUMNS_TO_TRIM = [
    "event_id",
    "timestamp",
    "user_id",
    "event_type",
    "device_type",
    "session_id",
    "ip_address",
    "utm_source",
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


def decode_confluent_avro(
    kafka_df: DataFrame,
    avro_schema: str,
    expected_schema_id: int = None,
) -> DataFrame:
    """
    Decode Kafka messages that use Confluent Avro wire format.

    Confluent Avro format:
    - byte 0: magic byte (should be 0x0)
    - bytes 1-4: schema id (big-endian int)
    - bytes 5-end: Avro payload

    We keep the raw magic byte and schema id as columns so:
    - a mismatch against the schema actually fetched from the registry
      (expected_schema_id) can be flagged rather than silently decoded
      with the wrong schema, and
    - decode failures are quarantined instead of crashing the whole
      streaming query (mode="PERMISSIVE" -> malformed rows get NULL
      fields instead of failing the query).
    """

    magic_byte = expr("substring(value, 1, 1)")
    wire_schema_id = expr(
        "cast(conv(hex(substring(value, 2, 4)), 16, 10) as int)"
    )
    avro_payload = expr("substring(value, 6, length(value) - 5)")
    decoded_value = from_avro(avro_payload, avro_schema, {"mode": "PERMISSIVE"})

    decoded_df = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").alias("raw_value"),
            magic_byte.alias("magic_byte"),
            wire_schema_id.alias("wire_schema_id"),
            decoded_value.alias("data"),
        )
    )

    decode_success_expr = col("data").isNotNull()
    if expected_schema_id is not None:
        schema_id_ok_expr = col("wire_schema_id") == expected_schema_id
    else:
        schema_id_ok_expr = lit(True)

    decoded_df = (
        decoded_df
        .withColumn("decode_success", decode_success_expr)
        .withColumn("schema_id_matches", schema_id_ok_expr)
        .withColumn(
            "decode_error",
            expr(
                """
                case
                   when length(raw_value) < 5 then 'message_too_short'
                    when magic_byte != X'00' then 'invalid_magic_byte'
                    when not decode_success then 'avro_decode_failed'
                    when not schema_id_matches then 'schema_id_mismatch'
                    else null
                end
                """
            ),
        )
    )

    return (
        decoded_df
        .select(
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "raw_value",
            "wire_schema_id",
            "decode_success",
            "schema_id_matches",
            "decode_error",
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
    .withColumn(
        "event_id",
        sha2(
            concat_ws(
                "||",
                col("kafka_topic"),
                col("kafka_partition").cast("string"),
                col("kafka_offset").cast("string"),
            ),
            256,
        ),
    )
    .withColumn("event_type", lower(col("event_type")))
    .withColumn("device_type", lower(col("device")))
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


def add_bronze_metadata(
    df: DataFrame,
    source_subject: str,
    schema_id: int = None,
) -> DataFrame:
    """
    Add technical metadata required for traceability in the Bronze layer.

    source_topic is derived from the actual Kafka `kafka_topic` column
    (populated from the record's own topic field) rather than hardcoded,
    so metadata stays correct even if the job later reads a different topic.
    source_subject and schema_id are passed in explicitly, since they are
    resolved once per job run from the Schema Registry / job configuration.
    """

    return (
        df
        .withColumn("source_topic", col("kafka_topic"))
        .withColumn("source_subject", lit(source_subject))
        .withColumn("schema_id", lit(schema_id))
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
                    when event_id is null or trim(event_id) = ''
                    then 'missing_event_id'
                end,
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
                    when device_type is null or trim(device_type) = ''
                    then 'missing_device_type'
                end,
                case
                    when session_id is null or trim(session_id) = ''
                    then 'missing_session_id'
                end,
                case
                    when event_timestamp is null
                    then 'invalid_event_timestamp'
                end,
                case
                    when not decode_success
                    then 'avro_decode_failed'
                end,
                case
                    when not schema_id_matches
                    then 'schema_id_mismatch'
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

    Partitioning is based on `event_timestamp` when it parsed successfully.
    If it's null/invalid, we fall back to the Kafka broker's own
    `kafka_timestamp` instead of letting year/month/day go null - a null
    partition value is hard to find and clean up later, whereas
    `kafka_timestamp` is always present and still lands the record in a
    reasonable, discoverable partition. The original invalid value is not
    lost: it is preserved in `timestamp` and flagged via
    `invalid_event_timestamp` in `validation_errors`.
    """

    partition_source = coalesce(col("event_timestamp"), col("kafka_timestamp"))

    return (
        df
        .withColumn("year", year(partition_source))
        .withColumn("month", month(partition_source))
        .withColumn("day", dayofmonth(partition_source))
    )


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Select a stable column order for Bronze output.
    """

    metadata_columns = [
        "source_topic",
        "source_subject",
        "schema_id",
        "wire_schema_id",
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "raw_value",
        "decode_success",
        "schema_id_matches",
        "decode_error",
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


def transform_bronze_behavioral(
    kafka_df: DataFrame,
    avro_schema: str,
    source_subject: str,
    schema_id: int = None,
) -> DataFrame:
    """
    Full Bronze transformation pipeline for behavioral events.

    Input:
        Raw Kafka streaming DataFrame, the Avro schema fetched from the
        registry, the subject it was fetched for, and (when available)
        the numeric schema id, so both can be recorded as metadata and
        the numeric id can be cross-checked against each message's own
        wire-format schema id.

    Output:
        Bronze-ready DataFrame with decoded, standardized, validated,
        and partition-ready behavioral events. Records that failed to
        decode or whose wire schema id doesn't match are kept (not
        dropped) with decode_success/decode_error/is_valid set, so
        downstream consumers can route them to quarantine instead of
        the whole stream failing.
    """

    decoded_df = decode_confluent_avro(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
        expected_schema_id=schema_id,
    )

    standardized_df = standardize_behavioral_events(decoded_df)

    metadata_df = add_bronze_metadata(
        standardized_df,
        source_subject=source_subject,
        schema_id=schema_id,
    )

    validated_df = add_validation_columns(metadata_df)

    partitioned_df = add_partition_columns(validated_df)

    return select_final_columns(partitioned_df)
