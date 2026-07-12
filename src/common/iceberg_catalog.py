from __future__ import annotations

import os

from pyspark.sql import SparkSession


DEFAULT_ICEBERG_CATALOG_NAME = "lakehouse"

# These two match the env var names already wired into docker-compose.yml
# (x-pipeline-env, passed to spark-master/spark-worker, and also present
# in x-airflow-common-env) -- no new env vars needed for Spark itself.
DEFAULT_ICEBERG_REST_URI = "http://iceberg-rest:8181"
DEFAULT_ICEBERG_WAREHOUSE = "s3://warehouse"

DEFAULT_SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "org.apache.spark:spark-avro_2.12:3.5.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    ]
)


def get_iceberg_catalog_name() -> str:
    """
    Local alias Spark uses for the catalog (spark.sql.catalog.<name>).
    Doesn't need to match anything in docker-compose -- it's just how
    Spark refers to it, e.g. lakehouse.silver.fact_behavioral_events.
    """
    return os.getenv("ICEBERG_CATALOG_NAME", DEFAULT_ICEBERG_CATALOG_NAME)


def get_iceberg_rest_uri() -> str:
    return os.getenv("ICEBERG_REST_URI", DEFAULT_ICEBERG_REST_URI)


def get_iceberg_warehouse_path() -> str:
    return os.getenv("ICEBERG_WAREHOUSE", DEFAULT_ICEBERG_WAREHOUSE)


def _get_minio_endpoint() -> str:
    return os.getenv("MINIO_ENDPOINT", "http://minio:9000")


def _get_minio_access_key() -> str:
    return (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
        or "minioadmin"
    )


def _get_minio_secret_key() -> str:
    return (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
        or "minioadmin"
    )


def qualified_table(table_name: str, schema: str = "silver") -> str:
    """
    e.g. qualified_table("fact_behavioral_events") ->
         "lakehouse.silver.fact_behavioral_events"
    """
    return f"{get_iceberg_catalog_name()}.{schema}.{table_name}"


def create_iceberg_spark_session(app_name: str = "silver-behavioral-job") -> SparkSession:
    """
    Create a SparkSession configured to talk to the project's existing
    `iceberg-rest` catalog service (not a direct Spark-to-Postgres JDBC
    catalog -- that service already exists in docker-compose.yml and is
    the single source of truth other tools/jobs would also use).

    The REST server handles catalog metadata/locking; Spark still talks
    to MinIO directly for the actual Parquet/manifest data files, so S3
    credentials are configured on both the Iceberg catalog (io-impl) and
    plain Hadoop s3a (for reading Bronze Parquet) sides.
    """

    catalog_name = get_iceberg_catalog_name()
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    spark_packages = os.getenv("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", spark_packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", get_iceberg_rest_uri())
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", get_iceberg_warehouse_path())
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", _get_minio_endpoint())
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", _get_minio_access_key())
        .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", _get_minio_secret_key())
        .config("spark.hadoop.fs.s3a.endpoint", _get_minio_endpoint())
        .config("spark.hadoop.fs.s3a.access.key", _get_minio_access_key())
        .config("spark.hadoop.fs.s3a.secret.key", _get_minio_secret_key())
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "Asia/Tehran")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    spark.sql(f"USE {catalog_name}")

    return spark


# Kept in sync with sql/iceberg/silver_behavioral_schema.sql by hand --
# that file is the reviewable reference, this is what actually runs.
SILVER_DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_date (
        date_key INT, full_date DATE, year INT, quarter INT, month INT,
        month_name STRING, day INT, day_of_week INT, day_name STRING,
        week_of_year INT, is_weekend BOOLEAN
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_user (
        user_key STRING, user_id STRING, first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP, total_events_seen BIGINT,
        is_placeholder BOOLEAN, silver_updated_at TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_device (
        device_key STRING, device_name STRING, first_seen_at TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_event_type (
        event_type_key STRING, event_type STRING, event_category STRING,
        first_seen_at TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_session (
        session_key STRING, session_id STRING, user_id STRING,
        session_start_at TIMESTAMP, session_end_at TIMESTAMP,
        session_duration_sec BIGINT, primary_device STRING,
        event_count BIGINT, silver_updated_at TIMESTAMP
    ) USING iceberg
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.fact_behavioral_events (
        event_key STRING, date_key INT, user_id STRING, session_id STRING,
        device STRING, event_type STRING, event_timestamp TIMESTAMP,
        product_id STRING, order_id STRING, url_path STRING, query STRING,
        wishlist_name STRING, payment_type STRING, shipping_method STRING,
        fulfillment_speed STRING, error_code STRING, success BOOLEAN,
        http_status INT, quantity INT, cart_total_items INT,
        cart_value DOUBLE, duration_sec INT, results_count INT,
        clicked_position INT, rating INT, text_length INT,
        cart_items ARRAY<STRUCT<product_id: STRING, price: DOUBLE, quantity: INT>>,
        dq_flags ARRAY<STRING>, kafka_partition INT, kafka_offset BIGINT,
        bronze_ingestion_timestamp TIMESTAMP, silver_ingestion_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_timestamp))
    """,
    """
    CREATE TABLE IF NOT EXISTS {catalog}.silver.behavioral_events_quarantine (
        kafka_partition INT, kafka_offset BIGINT, validation_errors ARRAY<STRING>,
        raw_user_id STRING, raw_session_id STRING, raw_event_type STRING,
        raw_timestamp STRING, raw_device STRING,
        bronze_ingestion_timestamp TIMESTAMP, silver_quarantine_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(silver_quarantine_timestamp))
    """,
]


def ensure_silver_behavioral_tables(spark: SparkSession) -> None:
    """
    Idempotently creates the Silver database + all behavioral star-schema
    tables. Safe to call on every job run.
    """

    catalog_name = get_iceberg_catalog_name()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.silver")

    for statement in SILVER_DDL_STATEMENTS:
        spark.sql(statement.format(catalog=catalog_name))
