import argparse
import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    input_file_name,
    min as spark_min,
    max as spark_max,
)
from common.silver_transactional_bronze_reader import (
    read_bronze_transactional_table,
)
from common.iceberg_session import (
    create_iceberg_spark_session,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_METADATA_COLUMNS = (
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test Bronze Parquet reading and compare its record count "
            "with the expected Kafka message count."
        )
    )

    parser.add_argument(
        "--table",
        required=True,
        help=(
            "Logical transactional table name, for example: "
            "orders, users or products."
        ),
    )

    parser.add_argument(
        "--partition-date",
        action="append",
        dest="partition_dates",
        help=(
            "Optional Bronze folder date in yyyyMMdd format. "
            "This option can be supplied multiple times."
        ),
    )

    parser.add_argument(
        "--expected-kafka-messages",
        type=int,
        default=None,
        help=(
            "Expected Kafka message count for the exact range being tested."
        ),
    )

    parser.add_argument(
        "--show-files",
        action="store_true",
        help="Show the record count for every Parquet file.",
    )

    return parser.parse_args()


def validate_metadata_columns(dataframe: DataFrame) -> None:
    missing_columns = [
        column_name
        for column_name in KAFKA_METADATA_COLUMNS
        if column_name not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "The Bronze DataFrame does not contain the required "
            f"Kafka metadata columns: {missing_columns}"
        )


def build_file_statistics(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Return one row per physical Parquet file.
    """

    return (
        dataframe
        .withColumn(
            "_source_file",
            input_file_name(),
        )
        .groupBy("_source_file")
        .agg(
            count("*").alias("record_count"),
            countDistinct(
                "_kafka_topic",
                "_kafka_partition",
                "_kafka_offset",
            ).alias("distinct_kafka_messages"),
            spark_min(
                "_kafka_timestamp",
            ).alias("minimum_kafka_timestamp"),
            spark_max(
                "_kafka_timestamp",
            ).alias("maximum_kafka_timestamp"),
        )
        .orderBy("_source_file")
    )


def build_kafka_partition_statistics(
    dataframe: DataFrame,
) -> DataFrame:
    """
    Show record and offset statistics for each Kafka partition.
    """

    return (
        dataframe
        .groupBy(
            "_kafka_topic",
            "_kafka_partition",
        )
        .agg(
            count("*").alias("record_count"),
            countDistinct(
                "_kafka_offset",
            ).alias("distinct_offset_count"),
            spark_min(
                "_kafka_offset",
            ).alias("minimum_offset"),
            spark_max(
                "_kafka_offset",
            ).alias("maximum_offset"),
        )
        .orderBy(
            "_kafka_topic",
            "_kafka_partition",
        )
    )


def calculate_summary(
    dataframe: DataFrame,
    file_statistics_df: DataFrame,
) -> dict[str, int]:
    total_records = dataframe.count()

    parquet_file_count = file_statistics_df.count()

    distinct_kafka_messages = (
        dataframe
        .select(
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
        )
        .distinct()
        .count()
    )

    null_kafka_metadata_records = (
        dataframe
        .filter(
            col("_kafka_topic").isNull()
            | col("_kafka_partition").isNull()
            | col("_kafka_offset").isNull()
        )
        .count()
    )

    duplicate_kafka_records = (
        total_records - distinct_kafka_messages
    )

    return {
        "parquet_file_count": parquet_file_count,
        "total_records": total_records,
        "distinct_kafka_messages": distinct_kafka_messages,
        "duplicate_kafka_records": duplicate_kafka_records,
        "null_kafka_metadata_records": null_kafka_metadata_records,
    }


def print_summary(
    table_name: str,
    summary: dict[str, int],
    expected_kafka_messages: int | None,
) -> bool:
    print("\n" + "=" * 70)
    print(f"BRONZE READ TEST RESULT — {table_name}")
    print("=" * 70)

    print(
        f"Parquet files read          : "
        f"{summary['parquet_file_count']}"
    )
    print(
        f"Total Parquet records       : "
        f"{summary['total_records']}"
    )
    print(
        f"Distinct Kafka messages     : "
        f"{summary['distinct_kafka_messages']}"
    )
    print(
        f"Duplicate Kafka records     : "
        f"{summary['duplicate_kafka_records']}"
    )
    print(
        f"Rows with null Kafka metadata: "
        f"{summary['null_kafka_metadata_records']}"
    )

    test_passed = True

    if summary["duplicate_kafka_records"] != 0:
        print("Duplicate check             : FAILED")
        test_passed = False
    else:
        print("Duplicate check             : PASSED")

    if summary["null_kafka_metadata_records"] != 0:
        print("Kafka metadata check        : FAILED")
        test_passed = False
    else:
        print("Kafka metadata check        : PASSED")

    if expected_kafka_messages is not None:
        difference = (
            summary["distinct_kafka_messages"]
            - expected_kafka_messages
        )

        print(
            f"Expected Kafka messages     : "
            f"{expected_kafka_messages}"
        )
        print(
            f"Difference                  : "
            f"{difference}"
        )

        if difference == 0:
            print("Kafka/Parquet count check   : PASSED")
        else:
            print("Kafka/Parquet count check   : FAILED")
            test_passed = False
    else:
        print(
            "Kafka/Parquet count check   : SKIPPED "
            "(expected count not supplied)"
        )

    print("=" * 70)
    print(
        "FINAL RESULT                : "
        + ("PASSED" if test_passed else "FAILED")
    )
    print("=" * 70 + "\n")

    return test_passed


def main() -> None:
    args = parse_arguments()

    spark = create_iceberg_spark_session(
        app_name=f"test-bronze-reader-{args.table}",
    )

    try:
        bronze_df = read_bronze_transactional_table(
            spark=spark,
            table_name=args.table,
            partition_dates=args.partition_dates,
        )

        validate_metadata_columns(bronze_df)

        logger.info(
            "Schema of Bronze table '%s':",
            args.table,
        )
        bronze_df.printSchema()

        file_statistics_df = build_file_statistics(
            bronze_df,
        )

        partition_statistics_df = (
            build_kafka_partition_statistics(
                bronze_df,
            )
        )

        summary = calculate_summary(
            dataframe=bronze_df,
            file_statistics_df=file_statistics_df,
        )

        print("\nKafka partition statistics:")

        partition_statistics_df.show(
            n=100,
            truncate=False,
        )

        if args.show_files:
            print("\nParquet file statistics:")

            file_statistics_df.show(
                n=1000,
                truncate=False,
            )

        test_passed = print_summary(
            table_name=args.table,
            summary=summary,
            expected_kafka_messages=args.expected_kafka_messages,
        )

        if not test_passed:
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()