"""Airflow DAG for Behavioral Silver -> ClickHouse Gold loading."""

from __future__ import annotations

import os
import sys

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


SRC_PATH = os.getenv("PIPELINE_SRC_PATH", "/opt/airflow/src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from common.silver_behavioral_config import spark_packages_csv


TEHRAN_TZ = pendulum.timezone("Asia/Tehran")
GOLD_JOB_PATH = "/opt/airflow/src/jobs/gold_behavioral_job.py"
SPARK_PACKAGES = spark_packages_csv()

PROCESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('execution_date', "
    "data_interval_start.in_timezone('Asia/Tehran').to_date_string()) }}"
)


def check_clickhouse_ready() -> None:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "lakehouse"),
    )
    client.command("SELECT 1")


def check_silver_behavioral_state(execution_date: str) -> None:
    from common.silver_behavioral_config import BehavioralRuntimeConfig
    from common.silver_behavioral_spark_session import create_silver_behavioral_spark_session
    from common.silver_behavioral_schema import TABLE_PIPELINE_STATE

    config = BehavioralRuntimeConfig.from_env()
    spark = create_silver_behavioral_spark_session(
        app_name=f"gold-behavioral-precheck-{execution_date}",
        config=config,
    )
    try:
        state_table = config.qualified_table(TABLE_PIPELINE_STATE)
        succeeded = (
            spark.table(state_table)
            .where(f"execution_date = DATE '{execution_date}' AND status = 'SUCCEEDED'")
            .limit(1)
            .count()
        )
        if succeeded == 0:
            raise RuntimeError(
                f"Silver Behavioral has no SUCCEEDED state for {execution_date}."
            )
    finally:
        spark.stop()


default_args = {
    "owner": "gold-behavioral",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=2),
}

with DAG(
    dag_id="gold_behavioral_clickhouse_etl",
    description="Load Silver Behavioral Iceberg tables into ClickHouse Gold OBT",
    default_args=default_args,
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TEHRAN_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "behavioral", "clickhouse"],
) as dag:
    check_silver = PythonOperator(
        task_id="check_silver_behavioral_succeeded",
        python_callable=check_silver_behavioral_state,
        op_kwargs={"execution_date": PROCESS_DATE_TEMPLATE},
    )

    check_clickhouse = PythonOperator(
        task_id="check_clickhouse_ready",
        python_callable=check_clickhouse_ready,
    )

    run_job = BashOperator(
        task_id="run_gold_behavioral_job",
        append_env=True,
        env={
            "BEHAVIORAL_SPARK_MASTER_URL": "spark://spark-master:7077",
            "BEHAVIORAL_ICEBERG_REST_URI": "http://lakekeeper:8181/catalog",
            "BEHAVIORAL_ICEBERG_WAREHOUSE": "warehouse",
            "BEHAVIORAL_ICEBERG_NAMESPACE": "silver_behavioral",
            "BEHAVIORAL_QUALITY_NAMESPACE": "silver_behavioral_quality",
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_HTTP_PORT": "8123",
            "CLICKHOUSE_DB": "lakehouse",
            "GOLD_BEHAVIORAL_TABLE": "behavioral_obt",
        },
        bash_command=(
            "set -euo pipefail; "
            "spark-submit "
            "--master 'spark://spark-master:7077' "
            f"--packages '{SPARK_PACKAGES}' "
            f"'{GOLD_JOB_PATH}' "
            f"--execution-date '{PROCESS_DATE_TEMPLATE}'"
        ),
    )

    check_silver >> check_clickhouse >> run_job
