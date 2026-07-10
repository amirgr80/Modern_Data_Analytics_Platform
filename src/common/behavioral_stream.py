import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.avro.functions import from_avro

from src.common.schema_registry import get_schema


KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "185.255.90.14:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "behavioral.events"
)

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://185.255.90.14:8081"
)

SCHEMA_SUBJECT = os.getenv(
    "SCHEMA_SUBJECT",
    "behavioral.events-value"
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]

MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "s3a://bronze/behavioral_events"
)

CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "s3a://checkpoints/behavioral_events"
)


def create_spark():

    return (
        SparkSession.builder
        .appName(
            "bronze-behavioral-stream"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .getOrCreate()
    )


def main():

    spark = create_spark()

    schema = get_schema(
        SCHEMA_SUBJECT,
        SCHEMA_REGISTRY_URL
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .load()
    )

    behavioral_events = (
        kafka_stream
        .select(
            from_avro(
                col("value"),
                schema
            ).alias("data")
        )
        .select(
            "data.*"
        )
        .withColumn(
            "_ingest_time",
            current_timestamp()
        )
        .withColumn(
            "event_date",
            to_date(
                col("timestamp")
            )
        )
    )

    query = (
        behavioral_events.writeStream
        .format("parquet")
        .option(
            "path",
            BRONZE_PATH
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH
        )
        .partitionBy(
            "event_date"
        )
        .outputMode(
            "append"
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
