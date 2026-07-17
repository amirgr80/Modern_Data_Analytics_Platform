"""Spark/Iceberg session factory owned by Silver Behavioral.

It deliberately does not import or modify ``common.iceberg_catalog`` because
that file is currently contested by the Transactional team and mixes generic
catalog settings with Behavioral DDL.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from pyspark.sql import SparkSession

from common.silver_behavioral_config import BehavioralRuntimeConfig


logger = logging.getLogger(__name__)


def create_silver_behavioral_spark_session(
    app_name: str,
    config: Optional[BehavioralRuntimeConfig] = None,
) -> SparkSession:
    cfg = config or BehavioralRuntimeConfig.from_env()

    catalog = cfg.catalog_name
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(cfg.spark_master)
        .config("spark.jars.packages", cfg.packages_csv)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", cfg.rest_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", cfg.warehouse)
        .config(
            f"spark.sql.catalog.{catalog}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", cfg.minio_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(
            f"spark.sql.catalog.{catalog}.s3.access-key-id",
            cfg.minio_access_key,
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.secret-access-key",
            cfg.minio_secret_key,
        )
        .config(f"spark.sql.catalog.{catalog}.s3.region", cfg.aws_region)
        .config("spark.sql.defaultCatalog", catalog)
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", cfg.timezone)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    driver_memory = os.getenv("BEHAVIORAL_SPARK_DRIVER_MEMORY")
    executor_memory = os.getenv("BEHAVIORAL_SPARK_EXECUTOR_MEMORY")
    executor_cores = os.getenv("BEHAVIORAL_SPARK_EXECUTOR_CORES")
    if driver_memory:
        builder = builder.config("spark.driver.memory", driver_memory)
    if executor_memory:
        builder = builder.config("spark.executor.memory", executor_memory)
    if executor_cores:
        builder = builder.config("spark.executor.cores", executor_cores)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    # Do not issue ``USE <catalog>``. USE selects a namespace/database, while
    # the catalog has already been selected through spark.sql.defaultCatalog.
    spark.sql(f"SHOW NAMESPACES IN {catalog}").limit(1).collect()
    logger.info(
        "Silver Behavioral Spark session ready: master=%s catalog=%s warehouse=%s",
        cfg.spark_master,
        cfg.catalog_name,
        cfg.warehouse,
    )
    return spark
