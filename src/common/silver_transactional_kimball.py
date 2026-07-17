from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


@dataclass(frozen=True)
class KimballTables:
    dim_date: DataFrame
    dim_user: DataFrame
    dim_category: DataFrame
    dim_product: DataFrame
    dim_product_price_scd: DataFrame
    fact_order: DataFrame
    fact_order_item: DataFrame


def _require_columns(df: DataFrame, required: list[str], dataframe_name: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"{dataframe_name} is missing required columns: {missing}. "
            f"Available columns: {sorted(df.columns)}"
        )


def _deterministic_key(prefix: str, *columns: str):
    """Generate a stable surrogate key from business-key columns."""
    values = [
        F.coalesce(F.col(column).cast("string"), F.lit("__NULL__"))
        for column in columns
    ]
    return F.concat(F.lit(f"{prefix}_"), F.sha2(F.concat_ws("||", *values), 256))


def _select_existing_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.select(*[column for column in columns if column in df.columns])


def _add_audit_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("silver_created_at", F.current_timestamp())
        .withColumn("silver_updated_at", F.current_timestamp())
    )


def build_dim_date(
    orders_df: DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DataFrame:
    """Build one row per calendar date."""
    _require_columns(orders_df, ["event_timestamp"], "orders_df")

    if bool(start_date) != bool(end_date):
        raise ValueError(
            "start_date and end_date must either both be supplied or both be omitted."
        )

    if start_date and end_date:
        boundaries_df = orders_df.sparkSession.range(1).select(
            F.to_date(F.lit(start_date)).alias("start_date"),
            F.to_date(F.lit(end_date)).alias("end_date"),
        )
    else:
        boundaries_df = (
            orders_df.select(F.to_date("event_timestamp").alias("order_date"))
            .where(F.col("order_date").isNotNull())
            .agg(
                F.min("order_date").alias("start_date"),
                F.max("order_date").alias("end_date"),
            )
        )

    dates_df = (
        boundaries_df.where(
            F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
            & (F.col("start_date") <= F.col("end_date"))
        )
        .select(
            F.explode(
                F.sequence(
                    F.col("start_date"),
                    F.col("end_date"),
                    F.expr("interval 1 day"),
                )
            ).alias("full_date")
        )
    )

    return (
        dates_df.withColumn(
            "date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType())
        )
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("month_number", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("quarter_number", F.quarter("full_date"))
        .withColumn("year_number", F.year("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select(
            "date_key",
            "full_date",
            "day_of_month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "month_number",
            "month_name",
            "quarter_number",
            "year_number",
            "is_weekend",
        )
        .dropDuplicates(["date_key"])
        .orderBy("date_key")
    )


def build_dim_user(users_df: DataFrame) -> DataFrame:
    _require_columns(users_df, ["user_id"], "users_df")

    columns = [
        "user_id",
        "username",
        "email",
        "signup_date",
        "device",
        "loyalty_tier",
        "location",
    ]

    dim_df = (
        _select_existing_columns(users_df, columns)
        .dropDuplicates(["user_id"])
        .withColumn("user_key", _deterministic_key("USR", "user_id"))
    )

    return _add_audit_columns(
        dim_df.select("user_key", *[c for c in columns if c in dim_df.columns])
    )


def build_dim_category(categories_df: DataFrame) -> DataFrame:
    _require_columns(
        categories_df, ["category_id", "category_name"], "categories_df"
    )

    dim_df = (
        categories_df.select(
            "category_id", "category_name", "parent_category_id"
        )
        .dropDuplicates(["category_id"])
        .withColumn("category_key", _deterministic_key("CAT", "category_id"))
        .withColumn(
            "parent_category_key",
            F.when(
                F.col("parent_category_id").isNotNull(),
                _deterministic_key("CAT", "parent_category_id"),
            ),
        )
    )

    return _add_audit_columns(
        dim_df.select(
            "category_key",
            "category_id",
            "category_name",
            "parent_category_key",
            "parent_category_id",
        )
    )


def build_dim_product(
    products_df: DataFrame,
    dim_category_df: DataFrame,
) -> DataFrame:
    _require_columns(
        products_df,
        ["product_id", "product_name", "category_id"],
        "products_df",
    )
    _require_columns(
        dim_category_df, ["category_key", "category_id"], "dim_category_df"
    )

    product_columns = [
        "product_id",
        "product_name",
        "category_id",
        "price",
        "inventory",
        "popularity_score",
    ]

    products = (
        _select_existing_columns(products_df, product_columns)
        .dropDuplicates(["product_id"])
        .alias("p")
    )
    categories = dim_category_df.select(
        "category_key", "category_id", "category_name"
    ).alias("c")

    joined = products.join(
        categories,
        F.col("p.category_id") == F.col("c.category_id"),
        "left",
    )

    result = (
        joined.withColumn("product_key", _deterministic_key("PRD", "p.product_id"))
        .select(
            "product_key",
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("c.category_key").alias("category_key"),
            F.col("p.category_id").alias("category_id"),
            F.col("c.category_name").alias("category_name"),
            *[
                F.col(f"p.{column}").alias(column)
                for column in ["price", "inventory", "popularity_score"]
                if column in products_df.columns
            ],
        )
    )

    return _add_audit_columns(result)


def build_dim_product_price_scd(
    product_price_history_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    _require_columns(
        product_price_history_df,
        [
            "price_history_id",
            "product_id",
            "price",
            "valid_from",
            "valid_to",
            "is_current",
        ],
        "product_price_history_df",
    )
    _require_columns(
        dim_product_df, ["product_key", "product_id"], "dim_product_df"
    )

    history = (
        product_price_history_df.select(
            "price_history_id",
            "product_id",
            "price",
            "valid_from",
            "valid_to",
            "is_current",
        )
        .dropDuplicates(["price_history_id"])
        .alias("h")
    )
    products = dim_product_df.select("product_key", "product_id").alias("p")

    result = (
        history.join(
            products,
            F.col("h.product_id") == F.col("p.product_id"),
            "left",
        )
        .withColumn(
            "product_price_key", _deterministic_key("PPR", "h.price_history_id")
        )
        .select(
            "product_price_key",
            F.col("h.price_history_id").alias("price_history_id"),
            F.col("p.product_key").alias("product_key"),
            F.col("h.product_id").alias("product_id"),
            F.col("h.price").alias("price"),
            F.col("h.valid_from").alias("valid_from"),
            F.col("h.valid_to").alias("valid_to"),
            F.col("h.is_current").alias("is_current"),
        )
    )

    return _add_audit_columns(result)


def build_fact_order(
    orders_df: DataFrame,
    dim_user_df: DataFrame,
) -> DataFrame:
    _require_columns(
        orders_df,
        [
            "order_id",
            "user_id",
            "event_timestamp",
            "status",
            "payment_method",
            "total",
        ],
        "orders_df",
    )
    _require_columns(dim_user_df, ["user_key", "user_id"], "dim_user_df")

    order_columns = [
        "order_id",
        "user_id",
        "event_timestamp",
        "status",
        "payment_method",
        "total",
        "_source_file",
    ]

    orders = (
        _select_existing_columns(orders_df, order_columns)
        .dropDuplicates(["order_id"])
        .alias("o")
    )
    users = dim_user_df.select("user_key", "user_id").alias("u")

    result = (
        orders.join(users, F.col("o.user_id") == F.col("u.user_id"), "left")
        .withColumn(
            "order_date_key",
            F.date_format(F.to_date(F.col("o.event_timestamp")), "yyyyMMdd").cast(
                IntegerType()
            ),
        )
        .withColumn("order_count", F.lit(1))
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("u.user_key").alias("user_key"),
            F.col("o.user_id").alias("user_id"),
            "order_date_key",
            F.col("o.event_timestamp").alias("order_timestamp"),
            F.col("o.status").alias("status"),
            F.col("o.payment_method").alias("payment_method"),
            F.col("o.total").alias("total_amount"),
            "order_count",
            *(
                [F.col("o._source_file").alias("source_file")]
                if "_source_file" in orders_df.columns
                else []
            ),
        )
    )

    return _add_audit_columns(result)


def build_fact_order_item(
    order_items_df: DataFrame,
    orders_df: DataFrame,
    fact_order_df: DataFrame,
    dim_product_df: DataFrame,
    dim_product_price_scd_df: DataFrame,
) -> DataFrame:
    _require_columns(
        order_items_df,
        ["order_item_id", "order_id", "product_id", "quantity", "price"],
        "order_items_df",
    )
    _require_columns(orders_df, ["order_id", "event_timestamp"], "orders_df")
    _require_columns(
        fact_order_df, ["order_id", "order_date_key"], "fact_order_df"
    )
    _require_columns(
        dim_product_df, ["product_key", "product_id"], "dim_product_df"
    )
    _require_columns(
        dim_product_price_scd_df,
        [
            "product_price_key",
            "product_id",
            "price",
            "valid_from",
            "valid_to",
        ],
        "dim_product_price_scd_df",
    )

    item_columns = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "price",
        "item_total_amount",
        "calculated_item_amount",
        "_source_file",
    ]

    items = (
        _select_existing_columns(order_items_df, item_columns)
        .dropDuplicates(["order_item_id"])
        .alias("i")
    )
    order_times = (
        orders_df.select(
            "order_id", F.col("event_timestamp").alias("order_timestamp")
        )
        .dropDuplicates(["order_id"])
        .alias("ot")
    )
    fact_orders = fact_order_df.select("order_id", "order_date_key").alias("fo")
    products = dim_product_df.select("product_key", "product_id").alias("p")
    prices = dim_product_price_scd_df.select(
        "product_price_key",
        "product_id",
        F.col("price").alias("historical_product_price"),
        "valid_from",
        "valid_to",
    ).alias("ph")

    base = (
        items.join(
            order_times,
            F.col("i.order_id") == F.col("ot.order_id"),
            "left",
        )
        .join(
            fact_orders,
            F.col("i.order_id") == F.col("fo.order_id"),
            "left",
        )
        .join(
            products,
            F.col("i.product_id") == F.col("p.product_id"),
            "left",
        )
    )

    price_condition = (
        (F.col("i.product_id") == F.col("ph.product_id"))
        & (F.col("ot.order_timestamp") >= F.col("ph.valid_from"))
        & (
            F.col("ph.valid_to").isNull()
            | (F.col("ot.order_timestamp") < F.col("ph.valid_to"))
        )
    )

    joined = base.join(prices, price_condition, "left")

    item_total_expression = (
        F.col("i.item_total_amount")
        if "item_total_amount" in order_items_df.columns
        else F.col("i.quantity") * F.col("i.price")
    )
    calculated_expression = (
        F.col("i.calculated_item_amount")
        if "calculated_item_amount" in order_items_df.columns
        else F.col("i.quantity") * F.col("i.price")
    )

    result = (
        joined.withColumn(
            "price_difference",
            F.col("i.price") - F.col("ph.historical_product_price"),
        )
        .select(
            F.col("i.order_item_id").alias("order_item_id"),
            F.col("i.order_id").alias("order_id"),
            F.col("p.product_key").alias("product_key"),
            F.col("i.product_id").alias("product_id"),
            F.col("ph.product_price_key").alias("product_price_key"),
            F.col("fo.order_date_key").alias("order_date_key"),
            F.col("i.quantity").alias("quantity"),
            F.col("i.price").alias("unit_price"),
            item_total_expression.alias("item_total_amount"),
            calculated_expression.alias("calculated_item_amount"),
            F.col("ph.historical_product_price").alias(
                "historical_product_price"
            ),
            "price_difference",
            *(
                [F.col("i._source_file").alias("source_file")]
                if "_source_file" in order_items_df.columns
                else []
            ),
        )
    )

    return _add_audit_columns(result)


def build_all_kimball_tables(
    users_df: DataFrame,
    categories_df: DataFrame,
    products_df: DataFrame,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    product_price_history_df: DataFrame,
    dim_date_start: Optional[str] = None,
    dim_date_end: Optional[str] = None,
) -> KimballTables:
    """Build all Silver Kimball DataFrames without writing to Iceberg."""
    dim_date = build_dim_date(
        orders_df,
        start_date=dim_date_start,
        end_date=dim_date_end,
    )
    dim_user = build_dim_user(users_df)
    dim_category = build_dim_category(categories_df)
    dim_product = build_dim_product(products_df, dim_category)
    dim_product_price_scd = build_dim_product_price_scd(
        product_price_history_df,
        dim_product,
    )
    fact_order = build_fact_order(orders_df, dim_user)
    fact_order_item = build_fact_order_item(
        order_items_df,
        orders_df,
        fact_order,
        dim_product,
        dim_product_price_scd,
    )

    return KimballTables(
        dim_date=dim_date,
        dim_user=dim_user,
        dim_category=dim_category,
        dim_product=dim_product,
        dim_product_price_scd=dim_product_price_scd,
        fact_order=fact_order,
        fact_order_item=fact_order_item,
    )
