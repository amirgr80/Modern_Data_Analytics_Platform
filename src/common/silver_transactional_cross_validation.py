from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class OrderItemParentValidationResult:
    """Result of validating order-item references to orders."""

    valid_order_items_df: DataFrame
    quality_issues_df: DataFrame


def _optional_column(
    dataframe: DataFrame,
    column_name: str,
    data_type: str,
) -> Column:
    if column_name in dataframe.columns:
        return F.col(column_name).cast(data_type)

    return F.lit(None).cast(data_type)


def split_order_items_by_parent(
    order_items_df: DataFrame,
    orders_df: DataFrame,
) -> OrderItemParentValidationResult:
    """
    Separate order items whose parent order does not exist.

    Valid rows continue to the Kimball fact builder.
    Orphan rows are represented as REJECTED quality records.
    """

    required_item_columns = {
        "order_item_id",
        "order_id",
    }

    missing_item_columns = sorted(
        required_item_columns
        - set(order_items_df.columns)
    )

    if missing_item_columns:
        raise ValueError(
            "order_items_df is missing required columns: "
            f"{missing_item_columns}"
        )

    if "order_id" not in orders_df.columns:
        raise ValueError(
            "orders_df is missing required column: order_id"
        )

    if "silver_cleaned_at" not in order_items_df.columns:
        raise ValueError(
            "order_items_df must contain silver_cleaned_at "
            "before cross-table validation."
        )

    parent_orders = (
        orders_df
        .select("order_id")
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )

    valid_order_items_df = (
        order_items_df
        .join(
            parent_orders,
            on=["order_id"],
            how="left_semi",
        )
    )

    orphan_order_items_df = (
        order_items_df
        .join(
            parent_orders,
            on=["order_id"],
            how="left_anti",
        )
    )

    original_record = F.to_json(
        F.struct(
            *[
                F.col(column_name)
                for column_name
                in order_items_df.columns
            ]
        )
    )

    quality_issues_df = (
        orphan_order_items_df
        .select(
            F.lit("order_items").alias(
                "source_table"
            ),
            F.col("order_item_id")
            .cast("string")
            .alias("record_id"),
            F.lit("REJECTED").alias(
                "issue_status"
            ),
            F.array(
                F.lit("MISSING_PARENT_ORDER")
            )
            .cast("array<string>")
            .alias("validation_errors"),
            F.array()
            .cast("array<string>")
            .alias("validation_warnings"),
            F.lit(
                "Excluded from fact_order_item because "
                "the referenced order_id does not exist "
                "in cleaned orders."
            ).alias("repair_description"),
            original_record.alias(
                "original_record"
            ),
            _optional_column(
                order_items_df,
                "_source_file",
                "string",
            ).alias("_source_file"),
            _optional_column(
                order_items_df,
                "_kafka_topic",
                "string",
            ).alias("_kafka_topic"),
            _optional_column(
                order_items_df,
                "_kafka_partition",
                "int",
            ).alias("_kafka_partition"),
            _optional_column(
                order_items_df,
                "_kafka_offset",
                "bigint",
            ).alias("_kafka_offset"),
            _optional_column(
                order_items_df,
                "_kafka_timestamp",
                "timestamp",
            ).alias("_kafka_timestamp"),
            _optional_column(
                order_items_df,
                "bronze_ingestion_timestamp",
                "timestamp",
            ).alias(
                "bronze_ingestion_timestamp"
            ),
            F.col("silver_cleaned_at")
            .cast("timestamp")
            .alias("detected_at"),
        )
    )

    return OrderItemParentValidationResult(
        valid_order_items_df=valid_order_items_df,
        quality_issues_df=quality_issues_df,
    )
