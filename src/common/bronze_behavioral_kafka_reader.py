import logging
import os

from pyspark.sql import DataFrame, SparkSession


logger = logging.getLogger(__name__)


def get_required_env(variable_name: str) -> str:
    value = os.getenv(variable_name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable '{variable_name}' is not set."
        )

    return value.strip()


def read_behavioral_kafka_stream(
    spark: SparkSession,
) -> DataFrame:

    kafka_bootstrap_servers = get_required_env(
        "KAFKA_BOOTSTRAP_SERVERS"
    )

    topic_name = os.getenv(
        "BEHAVIORAL_KAFKA_TOPIC",
        "behavioral.events",
    )

    starting_offsets = os.getenv(
        "KAFKA_STARTING_OFFSETS",
        "earliest",
    )

    logger.info(
        "Creating Kafka stream for behavioral topic '%s'",
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
