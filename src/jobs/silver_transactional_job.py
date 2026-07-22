from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from common.silver_transactional_bronze_reader import (
    read_bronze_transactional_for_date,
)
from common.silver_transactional_cleaning import (
    clean_transactional_data,
)
from common.silver_transactional_iceberg_writer import (
    SilverTransactionalIcebergWriter,
)
from common.silver_transactional_kimball import (
    build_all_kimball_tables,
)
from common.silver_transactional_quality_writer import (
    write_transactional_quality_issues,
)
from common.silver_transactional_cross_validation import (
    split_order_items_by_parent,
)
from common.silver_transactional_spark_session import (
    create_iceberg_spark_session,
)
from common.silver_transactional_run_context import (
    initialize_run_timestamp,
)
from common.silver_transactional_validation import (
    validate_transactional_data,
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)

logger = logging.getLogger(__name__)


TABLES = [
    "categories",
    "users",
    "products",
    "orders",
    "order_items",
    "product_price_history",
]


def valid_partition_date(
    value: str,
) -> str:
    try:
        parsed = datetime.strptime(
            value,
            "%Y%m%d",
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "partition date must use YYYYMMDD"
        ) from exc

    if parsed.strftime("%Y%m%d") != value:
        raise argparse.ArgumentTypeError(
            "partition date must use YYYYMMDD"
        )

    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Transactional Silver "
            "Kimball tables in Iceberg."
        )
    )

    parser.add_argument(
        "--partition-date",
        required=True,
        type=valid_partition_date,
    )

    parser.add_argument(
        "--catalog",
        default=os.getenv(
            "ICEBERG_CATALOG_NAME",
            "lakekeeper",
        ),
    )

    parser.add_argument(
        "--warehouse",
        default=os.getenv(
            "ICEBERG_WAREHOUSE",
            "silver",
        ),
    )

    parser.add_argument(
        "--namespace",
        default=os.getenv(
            "TRANSACTIONAL_NAMESPACE",
            "transactional",
        ),
    )

    parser.add_argument(
        "--quality-namespace",
        default=os.getenv(
            "TRANSACTIONAL_QUALITY_NAMESPACE",
            "transactional_quality",
        ),
    )

    parser.add_argument(
        "--dim-date-start",
        default=os.getenv(
            "DIM_DATE_START",
            "2020-01-01",
        ),
    )

    parser.add_argument(
        "--dim-date-end",
        default=os.getenv(
            "DIM_DATE_END",
            "2035-12-31",
        ),
    )

    return parser.parse_args()


def process_table(
    spark: SparkSession,
    table_name: str,
    process_date: str,
    catalog_name: str,
    quality_namespace: str,
) -> DataFrame:
    logger.info("=" * 60)

    logger.info(
        "Processing table=%s process_date=%s",
        table_name,
        process_date,
    )

    bronze_df = (
        read_bronze_transactional_for_date(
            spark=spark,
            table_name=table_name,
            process_date=process_date,
        )
        .persist()
    )

    valid_df = None
    quality_issues_df = None

    try:
        bronze_count = bronze_df.count()

        if bronze_count == 0:
            raise RuntimeError(
                "No Bronze records were found for "
                f"required table '{table_name}'."
            )

        logger.info(
            "Bronze records for %s: %s",
            table_name,
            bronze_count,
        )

        validation_result = (
            validate_transactional_data(
                bronze_df,
                table_name,
            )
        )

        valid_df = (
            validation_result
            .valid_df
            .persist()
        )

        quality_issues_df = (
            validation_result
            .quality_issues_df
            .persist()
        )

        valid_count = valid_df.count()

        rejected_count = (
            validation_result
            .rejected_df
            .count()
        )

        quality_count = (
            quality_issues_df
            .count()
        )

        logger.info(
            "Validation result for %s | "
            "valid=%s rejected=%s issues=%s",
            table_name,
            valid_count,
            rejected_count,
            quality_count,
        )

        if quality_count > 0:
            write_transactional_quality_issues(
                quality_issues_df=quality_issues_df,
                catalog_name=catalog_name,
                namespace=quality_namespace,
            )

        if valid_count == 0:
            raise RuntimeError(
                "No valid records remain for "
                f"required table '{table_name}'."
            )

        cleaned_df = (
            clean_transactional_data(
                valid_df,
                table_name,
            )
            .persist()
        )

        cleaned_count = cleaned_df.count()

        if cleaned_count == 0:
            cleaned_df.unpersist()

            raise RuntimeError(
                "Cleaning produced no records for "
                f"required table '{table_name}'."
            )

        logger.info(
            "Cleaned deterministic records "
            "for %s: %s",
            table_name,
            cleaned_count,
        )

        return cleaned_df

    finally:
        bronze_df.unpersist()

        if valid_df is not None:
            valid_df.unpersist()

        if quality_issues_df is not None:
            quality_issues_df.unpersist()


def configure_environment(
    args: argparse.Namespace,
) -> None:
    os.environ[
        "ICEBERG_CATALOG_NAME"
    ] = args.catalog

    os.environ[
        "ICEBERG_WAREHOUSE"
    ] = args.warehouse

    os.environ[
        "TRANSACTIONAL_NAMESPACE"
    ] = args.namespace

    os.environ[
        "TRANSACTIONAL_QUALITY_NAMESPACE"
    ] = args.quality_namespace


def main() -> None:
    args = parse_args()

    configure_environment(args)

    logger.info(
        "Transactional configuration | "
        "date=%s catalog=%s warehouse=%s "
        "namespace=%s quality_namespace=%s",
        args.partition_date,
        args.catalog,
        args.warehouse,
        args.namespace,
        args.quality_namespace,
    )

    spark = create_iceberg_spark_session(
        app_name="SilverTransactionalJob"
    )

    run_timestamp = initialize_run_timestamp(
        spark.sql(
            "SELECT current_timestamp() AS run_timestamp"
        ).first()["run_timestamp"]
    )

    logger.info(
        "Transactional fixed run timestamp=%s",
        run_timestamp.isoformat(),
    )

    cleaned_tables: dict[
        str,
        DataFrame,
    ] = {}

    try:
        for table_name in TABLES:
            cleaned_tables[
                table_name
            ] = process_table(
                spark=spark,
                table_name=table_name,
                process_date=args.partition_date,
                catalog_name=args.catalog,
                quality_namespace=(
                    args.quality_namespace
                ),
            )

        missing_tables = [
            table_name
            for table_name in TABLES
            if table_name not in cleaned_tables
        ]

        if missing_tables:
            raise RuntimeError(
                "Kimball build was blocked because "
                "required tables are missing: "
                f"{missing_tables}"
            )

        logger.info(
            "Running cross-table validation for "
            "order_items -> orders."
        )

        parent_validation = (
            split_order_items_by_parent(
                order_items_df=(
                    cleaned_tables["order_items"]
                ),
                orders_df=(
                    cleaned_tables["orders"]
                ),
            )
        )

        valid_order_items_df = (
            parent_validation
            .valid_order_items_df
            .persist()
        )

        referential_issues_df = (
            parent_validation
            .quality_issues_df
            .persist()
        )

        try:
            valid_order_item_count = (
                valid_order_items_df.count()
            )

            orphan_order_item_count = (
                referential_issues_df.count()
            )

            logger.info(
                "Cross-table validation result for "
                "order_items | valid=%s rejected=%s",
                valid_order_item_count,
                orphan_order_item_count,
            )

            if orphan_order_item_count > 0:
                write_transactional_quality_issues(
                    quality_issues_df=(
                        referential_issues_df
                    ),
                    catalog_name=args.catalog,
                    namespace=(
                        args.quality_namespace
                    ),
                )

            if valid_order_item_count == 0:
                raise RuntimeError(
                    "No valid order_items remain after "
                    "parent-order validation."
                )

            previous_order_items_df = (
                cleaned_tables["order_items"]
            )

            cleaned_tables["order_items"] = (
                valid_order_items_df
            )

            previous_order_items_df.unpersist()

        except Exception:
            valid_order_items_df.unpersist()
            raise

        finally:
            referential_issues_df.unpersist()

        logger.info(
            "Building Transactional Kimball tables."
        )

        kimball_tables = (
            build_all_kimball_tables(
                users_df=(
                    cleaned_tables["users"]
                ),
                categories_df=(
                    cleaned_tables["categories"]
                ),
                products_df=(
                    cleaned_tables["products"]
                ),
                orders_df=(
                    cleaned_tables["orders"]
                ),
                order_items_df=(
                    cleaned_tables["order_items"]
                ),
                product_price_history_df=(
                    cleaned_tables[
                        "product_price_history"
                    ]
                ),
                dim_date_start=(
                    args.dim_date_start
                ),
                dim_date_end=(
                    args.dim_date_end
                ),
            )
        )

        logger.info(
            "Writing Kimball tables to %s.%s",
            args.catalog,
            args.namespace,
        )

        writer = (
            SilverTransactionalIcebergWriter(
                spark=spark,
                catalog=args.catalog,
                namespace=args.namespace,
            )
        )

        writer.write_all(
            kimball_tables
        )

        logger.info(
            "Silver Transactional Job completed "
            "successfully for process_date=%s.",
            args.partition_date,
        )

    except Exception:
        logger.exception(
            "Silver Transactional Job failed."
        )

        raise

    finally:
        for dataframe in (
            cleaned_tables.values()
        ):
            try:
                dataframe.unpersist()
            except Exception:
                pass

        spark.stop()

        logger.info(
            "SparkSession stopped."
        )


if __name__ == "__main__":
    main()
