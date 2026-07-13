import os

from pyspark.sql import SparkSession


DEFAULT_SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)


def create_bronze_transactional_spark_session(
    app_name: str = "bronze-transactional-job",
) -> SparkSession:
    """
    Create a SparkSession for the Bronze transactional streaming job.

    This session is configured for:
    - Reading transactional topics from Kafka
    - Parsing JSON payloads with predefined Spark schemas
    - Writing Parquet files to MinIO through the s3a filesystem
    """

    spark_master = os.getenv(
        "SPARK_MASTER_URL",
        "spark://spark-master:7077",
    )

    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )

    minio_access_key = (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
    )

    minio_secret_key = (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
    )

    if not minio_access_key or not minio_secret_key:
        raise RuntimeError(
            "MinIO credentials are not configured. "
            "Set MINIO_ACCESS_KEY/MINIO_SECRET_KEY "
            "or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD."
        )

    spark_packages = os.getenv(
        "SPARK_PACKAGES",
        DEFAULT_SPARK_PACKAGES,
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)

        # Required packages for Kafka and MinIO/S3A
        .config(
            "spark.jars.packages",
            spark_packages,
        )

        # MinIO / S3A configuration
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
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )

        # Spark SQL configuration
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv(
                "SPARK_SQL_SHUFFLE_PARTITIONS",
                "4",
            ),
        )

        # Avoid generating _SUCCESS files
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        os.getenv("SPARK_LOG_LEVEL", "WARN")
    )

    return spark