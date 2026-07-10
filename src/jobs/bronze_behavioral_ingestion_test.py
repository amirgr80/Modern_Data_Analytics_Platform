import argparse
import os
import sys

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr


CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from common.schema_registry import get_latest_schema
from common.spark_session import create_spark_session
from schemas.behavioral_schemas import BEHAVIORAL_SUBJECT, BEHAVIORAL_TOPIC


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "185.255.90.14:9092"


def get_kafka_bootstrap_servers() -> str:
    return (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or os.getenv("KAFKA_BOOTSTRAP")
        or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    )


def read_behavioral_kafka_stream(spark):
    """
    Reads raw messages from the behavioral Kafka topic.

    Output columns from Kafka include:
    key, value, topic, partition, offset, timestamp
    """

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_servers())
        .option("subscribe", BEHAVIORAL_TOPIC)
        .option("startingOffsets", os.getenv("BEHAVIORAL_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )


def decode_confluent_avro(kafka_df, avro_schema: str):
    """
    Kafka value uses Confluent Avro wire format:

    byte 0      = magic byte
    bytes 1-4   = schema id
    bytes 5-end = real Avro payload

    Spark from_avro needs only the Avro payload, so we remove the first 5 bytes.
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


def run_ingestion_test(duration_seconds: int):
    avro_schema = get_latest_schema(BEHAVIORAL_SUBJECT)

    if not avro_schema:
        raise RuntimeError(
            f"Could not fetch Avro schema for subject: {BEHAVIORAL_SUBJECT}"
        )

    spark = create_spark_session(app_name="bronze-behavioral-ingestion-test")

    kafka_df = read_behavioral_kafka_stream(spark)

    decoded_df = decode_confluent_avro(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
    )

    query = (
        decoded_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "20")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination(duration_seconds)
    query.stop()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Test Bronze behavioral Kafka ingestion"
    )

    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=60,
        help="How long the ingestion test should run.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_ingestion_test(duration_seconds=args.duration_seconds)