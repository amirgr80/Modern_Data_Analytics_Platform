from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_users(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("username", F.trim(F.col("username")))
        .withColumn(
            "email",
            F.lower(F.trim(F.col("email"))),
        )
        .withColumn("country", F.trim(F.col("country")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("location", F.trim(F.col("location")))
        .withColumn(
            "loyalty_tier",
            F.lower(F.trim(F.col("loyalty_tier"))),
        )
        .withColumn(
            "device_default",
            F.lower(F.trim(F.col("device_default"))),
        )
        .dropDuplicates(["id"])
    )


def clean_categories(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn(
            "parent_category_id",
            F.col("parent_category_id").cast("long"),
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("name", F.trim(F.col("name")))
        .dropDuplicates(["id"])
    )


def clean_products(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("category_id", F.col("category_id").cast("long"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        .withColumn("price", F.col("price").cast("decimal(18,2)"))
        .dropDuplicates(["id"])
    )


def clean_product_price_history(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("price", F.col("price").cast("decimal(18,2)"))
        .withColumn("valid_from", F.to_timestamp("valid_from"))
        .withColumn("valid_to", F.to_timestamp("valid_to"))
        .withColumn("is_current", F.col("is_current").cast("boolean"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .dropDuplicates(["id"])
    )


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "order_timestamp",
            F.to_timestamp("order_timestamp"),
        )
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("total", F.col("total").cast("decimal(18,2)"))
        .withColumn(
            "discount_amount",
            F.col("discount_amount").cast("decimal(18,2)"),
        )
        .withColumn(
            "final_amount",
            F.col("final_amount").cast("decimal(18,2)"),
        )
        .withColumn(
            "shipping_amount",
            F.col("shipping_amount").cast("decimal(18,2)"),
        )
        .withColumn(
            "status",
            F.lower(F.trim(F.col("status"))),
        )
        .withColumn(
            "payment_method",
            F.lower(F.trim(F.col("payment_method"))),
        )
        .dropDuplicates(["id"])
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn(
            "unit_price",
            F.col("unit_price").cast("decimal(18,2)"),
        )
        .withColumn(
            "discount_amount",
            F.col("discount_amount").cast("decimal(18,2)"),
        )
        .withColumn(
            "item_total_amount",
            F.col("item_total_amount").cast("decimal(18,2)"),
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .dropDuplicates(["id"])
    )


def clean_returns_refunds(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn(
            "order_item_id",
            F.col("order_item_id").cast("long"),
        )
        .withColumn(
            "return_timestamp",
            F.to_timestamp("return_timestamp"),
        )
        .withColumn(
            "refund_amount",
            F.col("refund_amount").cast("decimal(18,2)"),
        )
        .withColumn(
            "return_reason",
            F.lower(F.trim(F.col("return_reason"))),
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .dropDuplicates(["id"])
    )


def clean_transactional_table(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    cleaners = {
        "users": clean_users,
        "categories": clean_categories,
        "products": clean_products,
        "product_price_history": clean_product_price_history,
        "orders": clean_orders,
        "order_items": clean_order_items,
        "returns_refunds": clean_returns_refunds,
    }

    cleaner = cleaners.get(table_name)

    if cleaner is None:
        raise ValueError(
            f"No cleaning function configured for table '{table_name}'."
        )

    return cleaner(df)