from __future__ import annotations

import sys
import os
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from common.gold_transactional_config import GoldTransactionalConfig

SILVER_DAG_ID = "silver_transactional_daily"
SILVER_SUCCESS_TASK_ID = "write_kimball_tables"

SPARK_APP_PATH = "/opt/airflow/src/jobs/gold_transactional_job.py"
SPARK_CONN_ID = "spark_default"


@task.sensor(poke_interval=30, timeout=600, mode="reschedule")
def check_clickhouse_ready() -> bool:
    import clickhouse_connect

    config = GoldTransactionalConfig.from_env()
    client = clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_http_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_db,
    )
    try:
        client.command("SELECT 1")
        return True
    finally:
        client.close()


with DAG(
    dag_id="gold_transactional_daily",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Tehran"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["gold", "transactional", "clickhouse"],
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id=SILVER_DAG_ID,
        external_task_id=SILVER_SUCCESS_TASK_ID,
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    clickhouse_ready = check_clickhouse_ready()

    run_gold_job = SparkSubmitOperator(
        task_id="run_gold_transactional_job",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP_PATH,
        application_args=["--order-date", "{{ ds }}"],
        name="gold_transactional_job_{{ ds }}",
    )

    wait_for_silver >> clickhouse_ready >> run_gold_job