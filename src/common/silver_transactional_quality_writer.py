from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


INPUT_COLUMNS = [
    "source_table",
    "record_id",
    "issue_status",
    "validation_errors",
    "validation_warnings",
    "repair_description",
    "original_record",
    "_source_file",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "bronze_ingestion_timestamp",
    "detected_at",
]


OUTPUT_COLUMNS = [
    "issue_key",
    *INPUT_COLUMNS,
]


IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*$"
)


def validate_identifier(
    identifier: str,
    label: str,
) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(
        identifier
    ):
        raise ValueError(
            f"Invalid {label}: {identifier!r}"
        )

    return identifier


def quote(identifier: str) -> str:
    return (
        "`"
        + identifier.replace("`", "``")
        + "`"
    )


def validate_quality_dataframe(
    quality_issues_df: DataFrame,
) -> None:
    missing_columns = [
        column_name
        for column_name in INPUT_COLUMNS
        if column_name
        not in quality_issues_df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Quality DataFrame is missing "
            f"required columns: {missing_columns}"
        )


def prepare_quality_dataframe(
    quality_issues_df: DataFrame,
) -> DataFrame:
    validate_quality_dataframe(
        quality_issues_df
    )

    prepared = quality_issues_df.select(
        F.col("source_table")
        .cast("string")
        .alias("source_table"),

        F.col("record_id")
        .cast("string")
        .alias("record_id"),

        F.col("issue_status")
        .cast("string")
        .alias("issue_status"),

        F.col("validation_errors")
        .cast("array<string>")
        .alias("validation_errors"),

        F.col("validation_warnings")
        .cast("array<string>")
        .alias("validation_warnings"),

        F.col("repair_description")
        .cast("string")
        .alias("repair_description"),

        F.col("original_record")
        .cast("string")
        .alias("original_record"),

        F.col("_source_file")
        .cast("string")
        .alias("_source_file"),

        F.col("_kafka_topic")
        .cast("string")
        .alias("_kafka_topic"),

        F.col("_kafka_partition")
        .cast("int")
        .alias("_kafka_partition"),

        F.col("_kafka_offset")
        .cast("bigint")
        .alias("_kafka_offset"),

        F.col("_kafka_timestamp")
        .cast("timestamp")
        .alias("_kafka_timestamp"),

        F.col("bronze_ingestion_timestamp")
        .cast("timestamp")
        .alias(
            "bronze_ingestion_timestamp"
        ),

        F.col("detected_at")
        .cast("timestamp")
        .alias("detected_at"),
    )

    # original_record is intentionally excluded from
    # issue_key because it contains volatile cleaning metadata
    # such as silver_cleaned_at. Business identity and immutable
    # source lineage are used for retry-safe idempotency.
    issue_key = F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(
                F.col("source_table"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.col("record_id"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.col("issue_status"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.to_json(
                    F.col("validation_errors")
                ),
                F.lit("[]"),
            ),
            F.coalesce(
                F.to_json(
                    F.col("validation_warnings")
                ),
                F.lit("[]"),
            ),
            F.coalesce(
                F.col("repair_description"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.col("_kafka_topic"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.col("_kafka_partition")
                .cast("string"),
                F.lit("__NULL__"),
            ),
            F.coalesce(
                F.col("_kafka_offset")
                .cast("string"),
                F.lit("__NULL__"),
            ),
        ),
        256,
    )

    return (
        prepared
        .withColumn(
            "issue_key",
            issue_key,
        )
        .select(*OUTPUT_COLUMNS)
        .dropDuplicates(["issue_key"])
    )


def create_quality_table_if_not_exists(
    quality_issues_df: DataFrame,
    catalog_name: str,
    namespace: str,
    table_name: str,
) -> str:
    catalog_name = validate_identifier(
        catalog_name,
        "catalog name",
    )

    namespace = validate_identifier(
        namespace,
        "namespace",
    )

    table_name = validate_identifier(
        table_name,
        "table name",
    )

    spark = quality_issues_df.sparkSession

    namespace_name = (
        f"{quote(catalog_name)}."
        f"{quote(namespace)}"
    )

    full_table_name = (
        f"{namespace_name}."
        f"{quote(table_name)}"
    )

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {namespace_name}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS
        {full_table_name} (
            issue_key STRING,
            source_table STRING,
            record_id STRING,
            issue_status STRING,
            validation_errors ARRAY<STRING>,
            validation_warnings ARRAY<STRING>,
            repair_description STRING,
            original_record STRING,
            _source_file STRING,
            _kafka_topic STRING,
            _kafka_partition INT,
            _kafka_offset BIGINT,
            _kafka_timestamp TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            detected_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (
            source_table,
            days(detected_at)
        )
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd'
        )
        """
    )

    return full_table_name


def write_transactional_quality_issues(
    quality_issues_df: DataFrame,
    catalog_name: Optional[str] = None,
    namespace: Optional[str] = None,
    table_name: Optional[str] = None,
) -> None:
    catalog_name = (
        catalog_name
        or os.getenv(
            "ICEBERG_CATALOG_NAME",
            "lakekeeper",
        )
    )

    namespace = (
        namespace
        or os.getenv(
            "TRANSACTIONAL_QUALITY_NAMESPACE",
            "transactional_quality",
        )
    )

    table_name = (
        table_name
        or os.getenv(
            "TRANSACTIONAL_QUALITY_TABLE",
            "transactional_validation_issues",
        )
    )

    if quality_issues_df.isEmpty():
        logger.info(
            "No transactional validation issues."
        )
        return

    prepared_df = (
        prepare_quality_dataframe(
            quality_issues_df
        )
        .persist()
    )

    try:
        full_table_name = (
            create_quality_table_if_not_exists(
                quality_issues_df=prepared_df,
                catalog_name=catalog_name,
                namespace=namespace,
                table_name=table_name,
            )
        )

        source_view = (
            "_transactional_quality_"
            + uuid.uuid4().hex
        )

        prepared_df.createOrReplaceTempView(
            source_view
        )

        insert_columns = ", ".join(
            quote(column_name)
            for column_name in OUTPUT_COLUMNS
        )

        insert_values = ", ".join(
            f"source.{quote(column_name)}"
            for column_name in OUTPUT_COLUMNS
        )

        try:
            quality_issues_df.sparkSession.sql(
                f"""
                MERGE INTO
                {full_table_name} AS target

                USING {quote(source_view)} AS source

                ON target.issue_key
                   = source.issue_key

                WHEN NOT MATCHED THEN INSERT (
                    {insert_columns}
                )
                VALUES (
                    {insert_values}
                )
                """
            )

        finally:
            quality_issues_df.sparkSession.catalog.dropTempView(
                source_view
            )

    finally:
        prepared_df.unpersist()

    logger.info(
        "Transactional quality issues "
        "merged into %s.",
        full_table_name,
    )
