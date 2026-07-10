import argparse
import os
import sys


CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from common.behavioral_transform import transform_bronze_behavioral
from common.bronze_writer import write_bronze_stream_to_parquet
from common.schema_registry import get_latest_schema
from common.spark_session import create_spark_session
from schemas.behavioral_schemas import BEHAVIORAL_SUBJECT, BEHAVIORAL_TOPIC


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "185.255.90.14:9092"

DEFAULT_BRONZE_BASE_PATH = "s3a://bronze"
DEFAULT_CHECKPOINT_BASE_PATH = "s3a://bronze/checkpoints"


def get_kafka_bootstrap_servers() -> str:
    return (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or os.getenv("KAFKA_BOOTSTRAP")
        or DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    )


def get_behavioral_output_path() -> str:
    bronze_base_path = os.getenv("BRONZE_BASE_PATH", DEFAULT_BRONZE_BASE_PATH)

    return os.getenv(
        "BEHAVIORAL_BRONZE_OUTPUT_PATH",
        f"{bronze_base_path}/behavioral/events",
    )


def get_behavioral_checkpoint_path() -> str:
    checkpoint_base_path = os.getenv(
        "CHECKPOINT_BASE_PATH",
        DEFAULT_CHECKPOINT_BASE_PATH,
    )

    return os.getenv(
        "BEHAVIORAL_BRONZE_CHECKPOINT_PATH",
        f"{checkpoint_base_path}/behavioral/events",
    )


def read_behavioral_kafka_stream(spark):
    """
    Read behavioral events from Kafka as a Spark streaming DataFrame.

    Kafka output columns:
    - key
    - value
    - topic
    - partition
    - offset
    - timestamp
    """

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_servers())
        .option("subscribe", os.getenv("BEHAVIORAL_KAFKA_TOPIC", BEHAVIORAL_TOPIC))
        .option("startingOffsets", os.getenv("BEHAVIORAL_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )


def run_bronze_behavioral_job() -> None:
    """
    Main Bronze behavioral streaming job.

    Steps:
    1. Fetch latest Avro schema from Schema Registry.
    2. Create Spark session.
    3. Read behavioral Kafka topic.
    4. Decode Avro messages.
    5. Apply Bronze standardization and validation.
    6. Write Parquet files to MinIO, partitioned by year/month/day.
    """

    schema_subject = os.getenv(
        "BEHAVIORAL_SCHEMA_SUBJECT",
        BEHAVIORAL_SUBJECT,
    )

    avro_schema = get_latest_schema(schema_subject)

    if not avro_schema:
        raise RuntimeError(
            f"Could not fetch Avro schema for subject: {schema_subject}"
        )

    spark = create_spark_session(app_name="bronze-behavioral-events")

    kafka_df = read_behavioral_kafka_stream(spark)

    bronze_df = transform_bronze_behavioral(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
    )

    query = write_bronze_stream_to_parquet(
        df=bronze_df,
        output_path=get_behavioral_output_path(),
        checkpoint_path=get_behavioral_checkpoint_path(),
        trigger_interval=os.getenv("BEHAVIORAL_TRIGGER_INTERVAL", "30 seconds"),
    )

    query.awaitTermination()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Bronze behavioral streaming job"
    )

    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
    run_bronze_behavioral_job()