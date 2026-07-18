import logging
import os
import traceback

from pyspark.sql import DataFrame, SparkSession

from common.silver_transactional_spark_session import (
    create_iceberg_spark_session,
)
from common.silver_transactional_bronze_reader import (
    read_bronze_transactional_table,
)
from common.silver_transactional_validation import (
    validate_transactional_data,
)
from common.silver_transactional_cleaning import (
    clean_transactional_data,
)
from common.silver_transactional_quality_writer import (
    write_transactional_quality_issues,
)
from common.silver_transactional_kimball import (
    build_all_kimball_tables,
)
from common.silver_transactional_iceberg_writer import (
    SilverTransactionalIcebergWriter,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
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


def process_table(
    spark: SparkSession,
    table_name: str,
    silver_bucket: str,
) -> DataFrame:
    logger.info("=" * 60)
    logger.info("Processing table: %s", table_name)

    logger.info("Reading Bronze data for: %s", table_name)
    bronze_df = read_bronze_transactional_table(
        spark,
        table_name,
        partition_dates=["20260712"],
    )

    if bronze_df.isEmpty():
        raise RuntimeError(
            f"No Bronze data found for required table: {table_name}"
        )

    bronze_count = bronze_df.count()
    logger.info(
        "Read %s records from Bronze for %s",
        bronze_count,
        table_name,
    )

    logger.info("Validating table: %s", table_name)
    result = validate_transactional_data(
        bronze_df,
        table_name,
    )

    valid_df = result.valid_df.persist()
    quality_issues_df = result.quality_issues_df.persist()

    valid_count = valid_df.count()
    rejected_count = result.rejected_df.count()
    issues_count = quality_issues_df.count()

    logger.info(
        "Validation result for %s | valid=%s rejected=%s issues=%s",
        table_name,
        valid_count,
        rejected_count,
        issues_count,
    )

    if issues_count > 0:
        logger.info(
            "Writing quality issues for: %s",
            table_name,
        )
        write_transactional_quality_issues(
            quality_issues_df
        )

    quality_issues_df.unpersist()

    if valid_count == 0:
        valid_df.unpersist()
        raise RuntimeError(
            f"No valid records remain for required table: {table_name}"
        )

    logger.info("Cleaning table: %s", table_name)
    cleaned_df = clean_transactional_data(
        valid_df,
        table_name,
    ).persist()

    cleaned_count = cleaned_df.count()
    valid_df.unpersist()

    silver_path = (
        f"s3a://{silver_bucket}/transactional/{table_name}/"
    )
    logger.info(
        "Writing %s cleaned records to: %s",
        cleaned_count,
        silver_path,
    )

    (
        cleaned_df.write
        .mode("overwrite")
        .format("parquet")
        .save(silver_path)
    )

    logger.info(
        "Successfully processed table: %s",
        table_name,
    )

    return cleaned_df


def main() -> None:
    spark = create_iceberg_spark_session(
        app_name="SilverTransactionalJob",
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_bucket = os.getenv(
        "MINIO_BUCKET_SILVER",
        "silver",
    )
    catalog = os.getenv(
        "ICEBERG_CATALOG_NAME",
        "lakekeeper",
    )
    namespace = os.getenv(
        "SILVER_NAMESPACE",
        "silver",
    )
    dim_date_start = os.getenv(
        "DIM_DATE_START",
        "2020-01-01",
    )
    dim_date_end = os.getenv(
        "DIM_DATE_END",
        "2035-12-31",
    )

    cleaned_tables: dict[str, DataFrame] = {}

    try:
        for table_name in TABLES:
            try:
                cleaned_tables[table_name] = process_table(
                    spark=spark,
                    table_name=table_name,
                    silver_bucket=silver_bucket,
                )
            except Exception as exc:
                logger.error(
                    "Failed to process table %s: %s",
                    table_name,
                    str(exc),
                )
                logger.error(traceback.format_exc())

        missing_tables = [
            table_name
            for table_name in TABLES
            if table_name not in cleaned_tables
        ]
        if missing_tables:
            raise RuntimeError(
                "Kimball build was not started because these required "
                f"tables failed or were empty: {missing_tables}"
            )

        logger.info("=" * 60)
        logger.info("Building Kimball dimensions and facts.")

        kimball_tables = build_all_kimball_tables(
            users_df=cleaned_tables.get('users'),
            categories_df=cleaned_tables.get('categories'),
            products_df=cleaned_tables.get('products'),
            orders_df=cleaned_tables.get('orders'),
            order_items_df=cleaned_tables.get('order_items'),
            product_price_history_df=cleaned_tables.get('product_price_history'),
            dim_date_start=dim_date_start,
            dim_date_end=dim_date_end,
        )

        logger.info(
            "Writing Kimball tables to %s.%s",
            catalog,
            namespace,
        )

        writer = SilverTransactionalIcebergWriter(
            spark=spark,
            catalog=catalog,
            namespace=namespace,
        )
        writer.write_all(kimball_tables)

        logger.info("=" * 60)
        logger.info(
            "Silver Transactional Job completed successfully."
        )

    except Exception:
        logger.exception(
            "Silver Transactional Job failed."
        )
        raise

    finally:
        for cleaned_df in cleaned_tables.values():
            try:
                cleaned_df.unpersist()
            except Exception:
                pass

        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
