import argparse
import os
import sys


CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_FILE_DIR)

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)


from common.bronze_behavioral_kafka_reader import read_behavioral_kafka_stream
from common.bronze_behavioral_transform import transform_bronze_behavioral
from common.bronze_behavioral_minio_writer import write_bronze_stream_to_parquet
from common.bronze_behavioral_spark_session import create_bronze_behavioral_spark_session
from common.registry_client import get_latest_schema_with_id
from schemas.bronze_behavioral_schemas import BEHAVIORAL_SUBJECT


DEFAULT_BRONZE_BASE_PATH = "s3a://bronze"

DEFAULT_CHECKPOINT_BASE_PATH = (
    "s3a://checkpoints/bronze"
)


def get_behavioral_output_path() -> str:
    """
    Returns Bronze Behavioral output path.
    """

    bronze_base_path = os.getenv(
        "BRONZE_BASE_PATH",
        DEFAULT_BRONZE_BASE_PATH,
    )

    return os.getenv(
        "BEHAVIORAL_BRONZE_OUTPUT_PATH",
        f"{bronze_base_path}/behavioral/events",
    )


def get_behavioral_checkpoint_path() -> str:
    """
    Returns checkpoint location.

    Checkpoints are stored separately from Bronze data
    to avoid accidental deletion during data lifecycle
    operations.
    """

    checkpoint_base_path = os.getenv(
        "CHECKPOINT_BASE_PATH",
        DEFAULT_CHECKPOINT_BASE_PATH,
    )

    return os.getenv(
        "BEHAVIORAL_BRONZE_CHECKPOINT_PATH",
        f"{checkpoint_base_path}/behavioral/events",
    )


def run_bronze_behavioral_job() -> None:
    """
    Main Bronze Behavioral Streaming Pipeline.

    Flow:

    1. Load Avro schema from Schema Registry.
    2. Create Spark Session.
    3. Read raw events from Kafka.
    4. Decode and transform events.
    5. Validate and enrich Bronze data.
    6. Write Parquet files into MinIO.
    """

    schema_subject = os.getenv(
        "BEHAVIORAL_SCHEMA_SUBJECT",
        BEHAVIORAL_SUBJECT,
    )

    avro_schema, schema_id = (
        get_latest_schema_with_id(schema_subject)
    )

    if not avro_schema:
        raise RuntimeError(
            f"Could not fetch Avro schema for subject: "
            f"{schema_subject}"
        )

    spark = create_bronze_behavioral_spark_session(
        app_name="bronze-behavioral-events"
    )

    kafka_df = read_behavioral_kafka_stream(
        spark
    )

    bronze_df = transform_bronze_behavioral(
        kafka_df=kafka_df,
        avro_schema=avro_schema,
        source_subject=schema_subject,
        schema_id=schema_id,
    )

    query = write_bronze_stream_to_parquet(
        df=bronze_df,
        output_path=get_behavioral_output_path(),
        checkpoint_path=get_behavioral_checkpoint_path(),
        trigger_interval=os.getenv(
            "BEHAVIORAL_TRIGGER_INTERVAL",
            "30 seconds",
        ),
    )

    query.awaitTermination()


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run Bronze Behavioral Streaming Job"
        )
    )

    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
    run_bronze_behavioral_job()