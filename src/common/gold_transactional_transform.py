from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.gold_transactional_config import GoldTransactionalConfig, OBT_COLUMNS


def build_transactional_obt(
    spark: SparkSession,
    config: GoldTransactionalConfig,
) -> DataFrame:
    fact_order_item = spark.table(config.silver_table("fact_order_item"))
    fact_order = spark.table(config.silver_table("fact_order"))
    dim_product = spark.table(config.silver_table("dim_product"))
    dim_user = spark.table(config.silver_table("dim_user"))
    dim_date = spark.table(config.silver_table("dim_date"))
    dim_product_price_scd = spark.table(
        config.silver_table("dim_product_price_scd")
    )
    dim_category = spark.table(config.silver_table("dim_category"))

    order_item = fact_order_item.alias("oi").join(
        fact_order.alias("o"),
        on=F.col("oi.order_id") == F.col("o.order_id"),
        how="inner",
    )

    priced = order_item.join(
        dim_product_price_scd.alias("price"),
        on=F.col("oi.product_price_key") == F.col("price.product_price_key"),
        how="left",
    )
    priced = priced.join(
        dim_product.alias("p"),
        on=F.col("oi.product_id") == F.col("p.product_id"),
        how="left",
    )
    priced = priced.join(
        dim_category.alias("c"),
        on=F.col("p.category_id") == F.col("c.category_id"),
        how="left",
    )
    priced = priced.join(
        dim_user.alias("u"),
        on=F.col("o.user_id") == F.col("u.user_id"),
        how="left",
    )
    priced = priced.join(
        dim_date.alias("d"),
        on=F.col("oi.order_date_key") == F.col("d.date_key"),
        how="left",
    )

    obt = priced.select(
        F.col("oi.order_item_id").alias("order_item_id"),
        F.col("oi.order_id").alias("order_id"),
        F.col("oi.product_id").alias("product_id"),
        F.col("c.category_id").alias("category_id"),
        F.col("o.user_id").alias("user_id"),
        F.col("price.price_history_id").alias("price_history_id"),
        F.col("oi.order_date_key").alias("date_key"),
        F.col("o.order_timestamp").alias("order_timestamp"),
        F.col("d.full_date").alias("full_date"),
        F.col("d.year_number").cast("int").alias("year_number"),
        F.col("d.month_number").cast("int").alias("month_number"),
        F.col("d.quarter_number").cast("int").alias("quarter_number"),
        F.col("d.day_of_week").cast("int").alias("day_of_week"),
        F.col("d.is_weekend").cast("int").alias("is_weekend"),
        F.col("o.status").alias("order_status"),
        F.col("o.payment_method").alias("payment_method"),
        F.col("oi.quantity").cast("int").alias("quantity"),
        F.col("oi.unit_price").cast("decimal(29,2)").alias("unit_price"),
        F.col("oi.discount_amount").cast("decimal(29,2)").alias("discount_amount"),
        F.col("oi.item_total_amount").cast("decimal(29,2)").alias("item_total_amount"),
        F.col("oi.price_difference").cast("decimal(29,2)").alias("price_difference"),
        F.col("o.total_amount").cast("decimal(29,2)").alias("order_total_amount"),
        F.col("p.product_name").alias("product_name"),
        F.col("c.category_name").alias("category_name"),
        F.col("u.username").alias("username"),
        F.col("u.email").alias("email"),
        F.col("u.signup_date").cast("date").alias("signup_date"),
        F.col("u.location").alias("location"),
        F.col("u.loyalty_tier").alias("loyalty_tier"),
        F.col("u.device").alias("device"),
        F.lit(0).cast("int").alias("is_returned"),
        F.lit(None).cast("string").alias("return_reason"),
        F.lit(None).cast("decimal(29,2)").alias("refund_amount"),
        F.lit(None).cast("timestamp").alias("return_timestamp"),
        F.greatest(
            F.col("oi.silver_updated_at"), F.col("o.silver_updated_at")
        ).alias("silver_updated_at"),
    )

    return obt.select(*OBT_COLUMNS)