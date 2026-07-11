import logging
import os

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


def get_required_env(variable_name: str) -> str:

    value = os.getenv(variable_name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable '{variable_name}' is not set."
        )

    return value.strip()


def create_spark_session() -> SparkSession:

    spark_master_url = os.getenv(
        "SPARK_MASTER_URL",
        "spark://spark-master:7077",
    )

    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )

    minio_access_key = get_required_env("MINIO_ROOT_USER")
    minio_secret_key = get_required_env("MINIO_ROOT_PASSWORD")

    logger.info("Creating SparkSession")
    logger.info("Spark master URL: %s", spark_master_url)
    logger.info("MinIO endpoint: %s", minio_endpoint)

    spark = (
        SparkSession.builder
        .appName("bronze-transactional-streaming")
        .master(spark_master_url)
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        os.getenv("SPARK_LOG_LEVEL", "WARN")
    )

    logger.info(
        "SparkSession created successfully. Spark version: %s",
        spark.version,
    )

    return spark