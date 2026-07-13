import os

from pyspark.sql import SparkSession


DEFAULT_CATALOG_NAME = "lakekeeper"
DEFAULT_REST_URI = "http://lakekeeper:8181/catalog"
DEFAULT_WAREHOUSE = "s3://warehouse"
DEFAULT_MINIO_ENDPOINT = "http://minio:9000"


def get_required_env(name: str) -> str:
    """
    Return a required environment variable.

    Raises:
        RuntimeError: If the variable is missing or blank.
    """

    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Required environment variable '{name}' is not configured."
        )

    return value.strip()


def create_iceberg_spark_session(
    app_name: str,
) -> SparkSession:
    """
    Create a Spark session configured for:

    - Apache Iceberg;
    - Lakekeeper REST Catalog;
    - MinIO through Iceberg S3FileIO.

    Iceberg and Hadoop AWS packages must be supplied through spark-submit.
    """

    catalog_name = os.getenv(
        "ICEBERG_CATALOG_NAME",
        DEFAULT_CATALOG_NAME,
    )

    rest_uri = os.getenv(
        "ICEBERG_REST_URI",
        DEFAULT_REST_URI,
    )

    warehouse = os.getenv(
        "ICEBERG_WAREHOUSE",
        DEFAULT_WAREHOUSE,
    )

    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT",
        DEFAULT_MINIO_ENDPOINT,
    )

    minio_access_key = get_required_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_required_env("MINIO_SECRET_KEY")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.type",
            "rest",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.uri",
            rest_uri,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            warehouse,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.s3.endpoint",
            minio_endpoint,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}."
            "s3.path-style-access",
            "true",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}."
            "s3.access-key-id",
            minio_access_key,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}."
            "s3.secret-access-key",
            minio_secret_key,
        )
        .config(
            f"spark.sql.catalog.{catalog_name}."
            "s3.region",
            "us-east-1",
        )
        .config(
            "spark.sql.defaultCatalog",
            catalog_name,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark