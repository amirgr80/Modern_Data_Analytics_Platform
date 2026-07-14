import logging

from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr


logger = logging.getLogger(__name__)


def decode_confluent_avro(
    kafka_df: DataFrame,
    avro_schema: str,
    expected_schema_id: int = None,
) -> DataFrame:

    magic_byte = expr("substring(value, 1, 1)")

    wire_schema_id = expr(
        "cast(conv(hex(substring(value, 2, 4)), 16, 10) as int)"
    )

    avro_payload = expr(
        "substring(value, 6, length(value) - 5)"
    )

    decoded_value = from_avro(
        avro_payload,
        avro_schema,
        {"mode": "PERMISSIVE"},
    )

    return (
        kafka_df
        .select(
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            col("value").alias("_raw_value"),
            magic_byte.alias("_magic_byte"),
            wire_schema_id.alias("_wire_schema_id"),
            decoded_value.alias("data"),
        )
        .select(
            "data.*",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_raw_value",
            "_magic_byte",
            "_wire_schema_id",
        )
    )
