"""Airflow DAG for Behavioral Silver -> ClickHouse Gold loading."""

from __future__ import annotations

import os
import sys

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


SRC_PATH = os.getenv("PIPELINE_SRC_PATH", "/opt/airflow/src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from common.silver_behavioral_config import spark_packages_csv


TEHRAN_TZ = pendulum.timezone("Asia/Tehran")
GOLD_JOB_PATH = "/opt/airflow/src/jobs/gold_behavioral_job.py"
SPARK_PACKAGES = spark_packages_csv()

# Upstream Silver Behavioral DAG whose success we wait on before loading Gold.
SILVER_DAG_ID = "silver_behavioral_etl_v2"
SILVER_SUCCESS_TASK_ID = "run_silver_behavioral_job_v2"

PROCESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get("
    "'process_date', "
    "data_interval_end.in_timezone('Asia/Tehran').subtract(days=1).to_date_string()"
    ") }}"
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
    check_silver = ExternalTaskSensor(
        task_id="check_silver_behavioral_succeeded",
        external_dag_id=SILVER_DAG_ID,
        external_task_id=SILVER_SUCCESS_TASK_ID,
        # Silver runs at 02:00 and Gold at 03:00, so Gold's logical date is one
        # hour ahead of the Silver run it depends on. Subtract that hour or the
        # sensor waits for a Silver run that never shares Gold's logical date.
        execution_delta=pendulum.duration(hours=1),
        allowed_states=["success"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
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
            "BEHAVIORAL_ICEBERG_WAREHOUSE": "silver",
            "BEHAVIORAL_ICEBERG_NAMESPACE": "behavioral",
            "BEHAVIORAL_QUALITY_NAMESPACE": "behavioral_quality",
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_HTTP_PORT": "8123",
            "CLICKHOUSE_DB": "lakehouse",
            "GOLD_BEHAVIORAL_TABLE": "behavioral_obt",
        },
        bash_command=(
            "set -euo pipefail; "
            "spark-submit "
            "--master 'spark://spark-master:7077' "
            "--driver-memory '2g' "
            "--executor-memory '4g' "
            "--executor-cores '2' "
            f"--packages '{SPARK_PACKAGES}' "
            f"'{GOLD_JOB_PATH}' "
            f"--execution-date '{PROCESS_DATE_TEMPLATE}'"
        ),
    )

check_clickhouse >> run_job
