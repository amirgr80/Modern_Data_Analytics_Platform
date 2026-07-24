from __future__ import annotations

import os
from dataclasses import dataclass, field


OBT_COLUMNS: tuple[str, ...] = (
    "order_item_id",
    "order_id",
    "product_id",
    "category_id",
    "user_id",
    "price_history_id",
    "date_key",
    "order_timestamp",
    "full_date",
    "year_number",
    "month_number",
    "quarter_number",
    "day_of_week",
    "is_weekend",
    "order_status",
    "payment_method",
    "quantity",
    "unit_price",
    "discount_amount",
    "item_total_amount",
    "price_difference",
    "order_total_amount",
    "product_name",
    "category_name",
    "username",
    "email",
    "signup_date",
    "location",
    "loyalty_tier",
    "device",
    "is_returned",
    "return_reason",
    "refund_amount",
    "return_timestamp",
    "silver_updated_at",
)


@dataclass(frozen=True)
class GoldTransactionalConfig:
    iceberg_catalog: str
    iceberg_rest_uri: str
    iceberg_warehouse: str
    iceberg_namespace: str

    clickhouse_host: str
    clickhouse_http_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str

    clickhouse_table: str = "transactional_obt"
    obt_columns: tuple[str, ...] = field(default_factory=lambda: OBT_COLUMNS)



    @classmethod
    def from_env(cls) -> "GoldTransactionalConfig":
        return cls(
            iceberg_catalog=os.environ.get("ICEBERG_CATALOG_NAME", "lakekeeper"),
            iceberg_rest_uri=_require_env("ICEBERG_REST_URI"),
            iceberg_warehouse=os.environ.get("ICEBERG_WAREHOUSE", "silver"),
            iceberg_namespace="transactional",  # 🔥 override
            clickhouse_host=_require_env("CLICKHOUSE_HOST"),
            clickhouse_http_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            clickhouse_db=_require_env("CLICKHOUSE_DB"),
            clickhouse_user=_require_env("CLICKHOUSE_USER"),
            clickhouse_password=_require_env("CLICKHOUSE_PASSWORD"),
        )   




    def silver_table(self, logical_name: str) -> str:
        return f"{self.iceberg_catalog}.{self.iceberg_namespace}.{logical_name}"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value