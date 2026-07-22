from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.silver_transactional_validation import (
    TARGET_TYPES,
)


logger = logging.getLogger(__name__)


BRONZE_TRANSACTIONAL_BASE_PATH = os.getenv(
    "BRONZE_TRANSACTIONAL_BASE_PATH",
    "s3a://bronze/transactional",
).rstrip("/")


SUPPORTED_TRANSACTIONAL_TABLES = [
    "categories",
    "users",
    "products",
    "orders",
    "order_items",
    "product_price_history",
]


BUSINESS_KEYS = {
    "categories": "category_id",
    "users": "user_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "product_price_history": "price_history_id",
}


TECHNICAL_TYPES = {
    "_kafka_topic": "string",
    "_kafka_partition": "int",
    "_kafka_offset": "bigint",
    "_kafka_timestamp": "timestamp",
    "_wire_schema_id": "int",
    "source_table": "string",
    "bronze_ingestion_timestamp": "timestamp",
    "partition_date": "string",
    "_source_file": "string",
}


def validate_table_name(
    table_name: str,
) -> None:
    if table_name not in SUPPORTED_TRANSACTIONAL_TABLES:
        raise ValueError(
            "Unsupported transactional table: "
            f"{table_name}"
        )


def validate_partition_date(
    partition_date: str,
) -> None:
    try:
        parsed = datetime.strptime(
            partition_date,
            "%Y%m%d",
        )
    except ValueError as exc:
        raise ValueError(
            "Partition date must use YYYYMMDD format: "
            f"{partition_date!r}"
        ) from exc

    if parsed.strftime("%Y%m%d") != partition_date:
        raise ValueError(
            "Partition date must use YYYYMMDD format: "
            f"{partition_date!r}"
        )


def get_available_partitions(
    spark: SparkSession,
    table_path: str,
) -> list[str]:
    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    path = spark._jvm.org.apache.hadoop.fs.Path(
        table_path
    )

    filesystem = path.getFileSystem(
        hadoop_conf
    )

    if not filesystem.exists(path):
        return []

    partitions = []

    for status in filesystem.listStatus(path):
        if not status.isDirectory():
            continue

        partition_name = (
            status.getPath()
            .getName()
        )

        if (
            len(partition_name) == 8
            and partition_name.isdigit()
        ):
            partitions.append(
                partition_name
            )

    return sorted(partitions)


def list_parquet_files(
    spark: SparkSession,
    partition_paths: Sequence[str],
) -> list[str]:
    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    Path = spark._jvm.org.apache.hadoop.fs.Path

    files: list[str] = []

    for path_string in partition_paths:
        path = Path(path_string)

        filesystem = path.getFileSystem(
            hadoop_conf
        )

        if not filesystem.exists(path):
            continue

        iterator = filesystem.listFiles(
            path,
            True,
        )

        while iterator.hasNext():
            file_path = (
                iterator.next()
                .getPath()
                .toString()
            )

            if file_path.endswith(".parquet"):
                files.append(file_path)

    return sorted(set(files))


def normalize_schema(
    dataframe: DataFrame,
    table_name: str,
) -> DataFrame:
    result = dataframe

    if "_source_file" not in result.columns:
        result = result.withColumn(
            "_source_file",
            F.input_file_name(),
        )

    target_types = {
        **TARGET_TYPES[table_name],
        **TECHNICAL_TYPES,
    }

    for column_name, target_type in target_types.items():
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.col(column_name).cast(
                    target_type
                ),
            )
        else:
            result = result.withColumn(
                column_name,
                F.lit(None).cast(
                    target_type
                ),
            )

    return result.select(
        *target_types.keys()
    )


def parquet_footer_fingerprint(
    spark: SparkSession,
    file_path: str,
) -> str:
    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    Path = (
        spark._jvm
        .org.apache.hadoop.fs.Path
    )

    ParquetFileReader = (
        spark._jvm
        .org.apache.parquet.hadoop
        .ParquetFileReader
    )

    ParquetMetadataConverter = (
        spark._jvm
        .org.apache.parquet.format.converter
        .ParquetMetadataConverter
    )

    footer = ParquetFileReader.readFooter(
        hadoop_conf,
        Path(file_path),
        ParquetMetadataConverter.NO_FILTER,
    )

    file_metadata = (
        footer.getFileMetaData()
    )

    parquet_schema = (
        file_metadata
        .getSchema()
        .toString()
    )

    key_value_metadata = (
        file_metadata
        .getKeyValueMetaData()
    )

    spark_schema = key_value_metadata.get(
        "org.apache.spark.sql.parquet.row.metadata"
    )

    return (
        parquet_schema
        + "\nSPARK_SCHEMA="
        + str(spark_schema)
    )


def read_schema_aware_files(
    spark: SparkSession,
    table_name: str,
    files: Sequence[str],
) -> DataFrame:
    if not files:
        raise RuntimeError(
            "No Bronze Parquet files found for "
            f"table '{table_name}'."
        )

    schema_groups: dict[
        str,
        list[str],
    ] = defaultdict(list)

    for file_path in files:
        fingerprint = (
            parquet_footer_fingerprint(
                spark=spark,
                file_path=file_path,
            )
        )

        schema_groups[
            fingerprint
        ].append(file_path)

    frames: list[DataFrame] = []

    for group_files in (
        schema_groups.values()
    ):
        representative_file = (
            group_files[0]
        )

        spark_schema = (
            spark.read
            .parquet(
                representative_file
            )
            .schema
        )

        frame = (
            spark.read
            .schema(spark_schema)
            .parquet(*group_files)
        )

        frames.append(
            normalize_schema(
                dataframe=frame,
                table_name=table_name,
            )
        )

    result = frames[0]

    for frame in frames[1:]:
        result = result.unionByName(
            frame,
            allowMissingColumns=True,
        )

    logger.info(
        "Read %s schema group(s) from %s "
        "Parquet file(s) for %s.",
        len(frames),
        len(files),
        table_name,
    )

    return result


def deduplicate_kafka_coordinates(
    dataframe: DataFrame,
    table_name: str,
) -> DataFrame:
    coordinate_columns = [
        "_kafka_topic",
        "_kafka_partition",
        "_kafka_offset",
    ]

    if any(
        column_name not in dataframe.columns
        for column_name in coordinate_columns
    ):
        logger.warning(
            "Kafka coordinates are unavailable for %s.",
            table_name,
        )

        return dataframe

    business_key = BUSINESS_KEYS[table_name]

    complete_coordinate = F.lit(True)

    for column_name in coordinate_columns:
        complete_coordinate = (
            complete_coordinate
            & F.col(column_name).isNotNull()
        )

    complete_rows = dataframe.where(
        complete_coordinate
    )

    incomplete_rows = dataframe.where(
        ~complete_coordinate
    )

    non_null_business_columns = F.lit(0)

    for column_name in TARGET_TYPES[table_name]:
        non_null_business_columns = (
            non_null_business_columns
            + F.when(
                F.col(column_name).isNotNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        )

    ordering = [
        F.when(
            F.col(business_key).isNotNull(),
            F.lit(1),
        ).otherwise(F.lit(0)).desc(),
        non_null_business_columns.desc(),
        F.col(
            "bronze_ingestion_timestamp"
        ).desc_nulls_last(),
        F.col(
            "_kafka_timestamp"
        ).desc_nulls_last(),
    ]

    coordinate_window = (
        Window
        .partitionBy(*coordinate_columns)
        .orderBy(*ordering)
    )

    deduplicated_complete = (
        complete_rows
        .withColumn(
            "__kafka_coordinate_rank",
            F.row_number().over(
                coordinate_window
            ),
        )
        .where(
            F.col(
                "__kafka_coordinate_rank"
            )
            == 1
        )
        .drop(
            "__kafka_coordinate_rank"
        )
    )

    return deduplicated_complete.unionByName(
        incomplete_rows,
        allowMissingColumns=True,
    )


def read_bronze_transactional_table(
    spark: SparkSession,
    table_name: str,
    partition_dates: list[str] | None = None,
) -> DataFrame:
    validate_table_name(table_name)

    table_path = (
        f"{BRONZE_TRANSACTIONAL_BASE_PATH}/"
        f"{table_name}"
    )

    if partition_dates is None:
        partition_dates = get_available_partitions(
            spark=spark,
            table_path=table_path,
        )

    if not partition_dates:
        raise RuntimeError(
            "No Bronze partitions found for "
            f"table '{table_name}'."
        )

    for partition_date in partition_dates:
        validate_partition_date(
            partition_date
        )

    partition_paths = [
        f"{table_path}/{partition_date}"
        for partition_date in partition_dates
    ]

    logger.info(
        "Reading Bronze %s partitions: %s",
        table_name,
        partition_dates,
    )

    files = list_parquet_files(
        spark=spark,
        partition_paths=partition_paths,
    )

    dataframe = read_schema_aware_files(
        spark=spark,
        table_name=table_name,
        files=files,
    )

    return deduplicate_kafka_coordinates(
        dataframe=dataframe,
        table_name=table_name,
    )


def resolve_transactional_partitions(
    spark: SparkSession,
    table_name: str,
    process_date: str,
) -> list[str]:
    validate_table_name(table_name)
    validate_partition_date(process_date)

    table_path = (
        f"{BRONZE_TRANSACTIONAL_BASE_PATH}/"
        f"{table_name}"
    )

    available = get_available_partitions(
        spark=spark,
        table_path=table_path,
    )

    if table_name == "categories":
        eligible = [
            partition
            for partition in available
            if (
                partition <= process_date
                and partition != "19700101"
            )
        ]

        return (
            [max(eligible)]
            if eligible
            else []
        )

    if table_name in {
        "users",
        "products",
        "product_price_history",
    }:
        return [
            partition
            for partition in available
            if (
                partition <= process_date
                and partition != "19700101"
            )
        ]

    if table_name == "orders":
        selected = []

        if process_date in available:
            selected.append(process_date)

        if "19700101" in available:
            selected.append("19700101")

        return selected

    if process_date in available:
        return [process_date]

    return []


def read_bronze_transactional_for_date(
    spark: SparkSession,
    table_name: str,
    process_date: str,
) -> DataFrame:
    partitions = resolve_transactional_partitions(
        spark=spark,
        table_name=table_name,
        process_date=process_date,
    )

    if not partitions:
        raise RuntimeError(
            "No eligible Bronze partitions found for "
            f"table '{table_name}' at {process_date}."
        )

    dataframe = read_bronze_transactional_table(
        spark=spark,
        table_name=table_name,
        partition_dates=partitions,
    )

    if table_name != "orders":
        return dataframe

    original_timestamp = F.coalesce(
        F.col("event_timestamp"),
        F.col("timestamp"),
    )

    original_invalid = (
        original_timestamp.isNull()
        | (F.year(original_timestamp) <= 1970)
    )

    kafka_timestamp_valid = (
        F.col("_kafka_timestamp").isNotNull()
        & (
            F.year(
                F.col("_kafka_timestamp")
            )
            > 1970
        )
    )

    resolved_timestamp = F.when(
        original_invalid & kafka_timestamp_valid,
        F.col("_kafka_timestamp"),
    ).otherwise(original_timestamp)

    target_date = datetime.strptime(
        process_date,
        "%Y%m%d",
    ).strftime("%Y-%m-%d")

    return (
        dataframe
        .withColumn(
            "__resolved_order_timestamp",
            resolved_timestamp,
        )
        .where(
            F.to_date(
                F.col(
                    "__resolved_order_timestamp"
                )
            )
            == F.to_date(
                F.lit(target_date)
            )
        )
        .drop(
            "__resolved_order_timestamp"
        )
    )
