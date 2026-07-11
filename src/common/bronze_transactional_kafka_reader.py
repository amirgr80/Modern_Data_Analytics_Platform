import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType


logger = logging.getLogger(__name__)


def get_required_env(variable_name: str) -> str:

    value = os.getenv(variable_name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable '{variable_name}' is not set."
        )

    return value.strip()


def read_kafka_topic(
    spark: SparkSession,
    topic_name: str,
) -> DataFrame:

    kafka_bootstrap_servers = get_required_env(
        "KAFKA_BOOTSTRAP_SERVERS"
    )

    starting_offsets = os.getenv(
        "KAFKA_STARTING_OFFSETS",
        "earliest",
    )

    logger.info(
        "Creating Kafka stream for topic '%s'",
        topic_name,
    )

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            kafka_bootstrap_servers,
        )
        .option(
            "subscribe",
            topic_name,
        )
        .option(
            "startingOffsets",
            starting_offsets,
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )


def parse_kafka_value(
    kafka_df: DataFrame,
    schema: StructType,
) -> DataFrame:
    """
    Convert Kafka message value from binary to JSON
    and apply the provided schema.
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


def read_and_parse_kafka_topic(
    spark: SparkSession,
    topic_name: str,
    schema: StructType,
) -> DataFrame:
    """
    Read one Kafka topic and parse its JSON value
    using the specified schema.
    """

    kafka_df = read_kafka_topic(
        spark=spark,
        topic_name=topic_name,
    )

    parsed_df = parse_kafka_value(
        kafka_df=kafka_df,
        schema=schema,
    )

    logger.info(
        "Kafka stream created and parsed for topic '%s'",
        topic_name,
    )

    return parsed_df