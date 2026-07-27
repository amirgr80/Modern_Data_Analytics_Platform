# common/gold_transactional_clickhouse.py

from __future__ import annotations

import logging

import clickhouse_connect
from pyspark.sql import DataFrame

from common.gold_transactional_config import GoldTransactionalConfig


logger = logging.getLogger(__name__)


class TransactionalObtWriteError(RuntimeError):
    pass


def write_partition(
    obt_df: DataFrame,
    full_date: str,
    config: GoldTransactionalConfig,
) -> int:
    partition_df = obt_df.where(f"full_date = '{full_date}'")
    pandas_df = partition_df.toPandas()

    if pandas_df.empty:
        logger.info("No rows to write for full_date=%s", full_date)
        return 0

    client = clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_http_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_db,
    )

    try:
        client.command(
            f"ALTER TABLE {config.clickhouse_table} "
            f"DELETE WHERE full_date = '{full_date}' "
            f"SETTINGS mutations_sync = 1"
        )

        client.insert_df(
            table=config.clickhouse_table,
            df=pandas_df,
            column_names=list(config.obt_columns),
        )

        loaded_count = _verify_row_count(client, config, full_date)
        expected_count = len(pandas_df)

        if loaded_count != expected_count:
            raise TransactionalObtWriteError(
                f"Row count mismatch for full_date={full_date}: "
                f"expected={expected_count} loaded={loaded_count}"
            )

        logger.info(
            "Loaded %s rows into %s for full_date=%s",
            loaded_count,
            config.clickhouse_table,
            full_date,
        )
        return loaded_count
    finally:
        client.close()


def _verify_row_count(
    client: "clickhouse_connect.driver.client.Client",
    config: GoldTransactionalConfig,
    full_date: str,
) -> int:
    result = client.query(
        f"SELECT count() FROM {config.clickhouse_table} "
        f"WHERE full_date = '{full_date}'"
    )
    return int(result.result_rows[0][0])