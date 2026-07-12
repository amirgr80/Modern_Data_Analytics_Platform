from tuple import tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from silver.transactional.config import (
    LOYALTY_TIERS,
    ORDER_STATUSES,
)


def add_validation_result(
    df: DataFrame,
    valid_condition: Column,
    error_message: str,
) -> tuple[DataFrame, DataFrame]:
    valid_df = df.filter(valid_condition)

    invalid_df = (
        df
        .filter(~valid_condition)
        .withColumn(
            "_validation_error",
            F.lit(error_message),
        )
        .withColumn(
            "_rejected_at",
            F.current_timestamp(),
        )
    )

    return valid_df, invalid_df


def validate_users(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("username").isNotNull()
        & F.col("email").isNotNull()
        & F.col("loyalty_tier").isin(*LOYALTY_TIERS)
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_user_record",
    )


def validate_categories(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("name").isNotNull()
        & (
            F.col("parent_category_id").isNull()
            | (F.col("parent_category_id") != F.col("id"))
        )
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_category_record",
    )


def validate_products(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("name").isNotNull()
        & F.col("category_id").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_product_record",
    )


def validate_product_price_history(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") >= 0)
        & F.col("valid_from").isNotNull()
        & (
            F.col("valid_to").isNull()
            | (F.col("valid_to") > F.col("valid_from"))
        )
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_product_price_history_record",
    )


def validate_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("order_timestamp").isNotNull()
        & F.col("status").isin(*ORDER_STATUSES)
        & F.col("total").isNotNull()
        & (F.col("total") >= 0)
        & F.col("discount_amount").isNotNull()
        & (F.col("discount_amount") >= 0)
        & F.col("final_amount").isNotNull()
        & (F.col("final_amount") >= 0)
        & F.col("shipping_amount").isNotNull()
        & (F.col("shipping_amount") >= 0)
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_order_record",
    )


def validate_order_items(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0)
        & F.col("discount_amount").isNotNull()
        & (F.col("discount_amount") >= 0)
        & F.col("item_total_amount").isNotNull()
        & (F.col("item_total_amount") >= 0)
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_order_item_record",
    )


def validate_returns_refunds(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    condition = (
        F.col("id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("order_item_id").isNotNull()
        & F.col("return_timestamp").isNotNull()
        & F.col("refund_amount").isNotNull()
        & (F.col("refund_amount") >= 0)
        & F.col("return_reason").isNotNull()
    )

    return add_validation_result(
        df=df,
        valid_condition=condition,
        error_message="invalid_return_refund_record",
    )


def validate_transactional_table(
    df: DataFrame,
    table_name: str,
) -> tuple[DataFrame, DataFrame]:
    validators = {
        "users": validate_users,
        "categories": validate_categories,
        "products": validate_products,
        "product_price_history": validate_product_price_history,
        "orders": validate_orders,
        "order_items": validate_order_items,
        "returns_refunds": validate_returns_refunds,
    }

    validator = validators.get(table_name)

    if validator is None:
        raise ValueError(
            f"No validation function configured for table '{table_name}'."
        )

    return validator(df)