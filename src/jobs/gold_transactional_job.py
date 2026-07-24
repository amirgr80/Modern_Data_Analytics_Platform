from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession

from common.gold_transactional_config import GoldTransactionalConfig, OBT_COLUMNS
from common.gold_transactional_transform import build_transactional_obt
from common.gold_transactional_clickhouse import write_partition, TransactionalObtWriteError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


KEY_COLUMNS = ("order_item_id", "order_id", "product_id", "user_id")


class ContractValidationError(RuntimeError):
    pass


def validate_obt(obt_df: DataFrame) -> None:
    actual_columns = tuple(obt_df.columns)
    if actual_columns != OBT_COLUMNS:
        raise ContractValidationError(
            f"Column contract mismatch.\nexpected={OBT_COLUMNS}\nactual={actual_columns}"
        )

    null_key_condition = None
    for key in KEY_COLUMNS:
        condition = obt_df[key].isNull()
        null_key_condition = (
            condition if null_key_condition is None else null_key_condition | condition
        )

    null_key_count = obt_df.where(null_key_condition).limit(1).count()
    if null_key_count:
        raise ContractValidationError(f"Found NULL values in key columns: {KEY_COLUMNS}")

    duplicate_count = (
        obt_df.groupBy("order_item_id").count().where("count > 1").limit(1).count()
    )
    if duplicate_count:
        raise ContractValidationError("Found duplicate order_item_id values.")

    logger.info("Contract validation passed.")


def run(order_date: str) -> None:
    # 🔧 استفاده از SparkSession.builder به‌جای create_iceberg_spark_session
    spark = SparkSession.builder.appName("GoldTransactionalJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    config = GoldTransactionalConfig.from_env()

    try:
        obt_df = build_transactional_obt(spark, config).persist()
        validate_obt(obt_df)

        loaded_count = write_partition(obt_df, order_date, config)
        logger.info(
            "Gold Transactional job finished | order_date=%s loaded=%s",
            order_date,
            loaded_count,
        )
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold Transactional OBT job")
    parser.add_argument("--order-date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    try:
        run(args.order_date)
    except (ContractValidationError, TransactionalObtWriteError) as exc:
        logger.error("Gold Transactional job failed: %s", exc)
        sys.exit(1)
    except Exception:
        logger.exception("Gold Transactional job failed with an unexpected error.")
        sys.exit(1)


if __name__ == "__main__":
    main()