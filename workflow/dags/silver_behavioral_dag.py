"""Airflow DAG for the independent Silver Behavioral job."""

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
SILVER_JOB_PATH = "/opt/airflow/src/jobs/silver_behavioral_job.py"
SPARK_PACKAGES = spark_packages_csv()

# Scheduled runs process the date at the beginning of their data interval.
# Manual runs may pass {"execution_date": "YYYY-MM-DD"} in dag_run.conf.
PROCESS_DATE_TEMPLATE = (
    "{{ dag_run.conf['execution_date'] "
    "if dag_run.conf.get('execution_date') "
    "else data_interval_start.in_timezone('Asia/Tehran').to_date_string() }}"
)


def check_bronze_partition_exists(execution_date: str) -> None:
    import boto3

    year, month, day = (int(value) for value in execution_date.split("-"))
    bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    prefix = f"behavioral/events/year={year}/month={month}/day={day}/"
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=(
            os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
        ),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if response.get("KeyCount", 0) == 0:
        raise FileNotFoundError(f"No Bronze Behavioral objects at s3://{bucket}/{prefix}")


default_args = {
    "owner": "silver-behavioral",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=2),
}

with DAG(
    dag_id="silver_behavioral_etl_v2",
    description="Independent Bronze -> Iceberg Silver Behavioral ETL",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TEHRAN_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "behavioral", "iceberg"],
) as dag:
    check_partition = PythonOperator(
        task_id="check_behavioral_bronze_partition",
        python_callable=check_bronze_partition_exists,
        op_kwargs={"execution_date": PROCESS_DATE_TEMPLATE},
    )

    run_job = BashOperator(
        task_id="run_silver_behavioral_job_v2",
        append_env=True,
        env={
            "BEHAVIORAL_SPARK_MASTER_URL": "spark://spark-master:7077",
            "BEHAVIORAL_ICEBERG_REST_URI": "http://lakekeeper:8181/catalog",
            "BEHAVIORAL_ICEBERG_WAREHOUSE": "silver",
            "BEHAVIORAL_ICEBERG_NAMESPACE": "behavioral",
            "BEHAVIORAL_QUALITY_NAMESPACE": "behavioral_quality",
        },
        bash_command=(
            "set -euo pipefail; "
            "spark-submit "
            "--master 'spark://spark-master:7077' "
            "--driver-memory '2g' "
            "--executor-memory '4g' "
            "--executor-cores '2' "
            f"--packages '{SPARK_PACKAGES}' "
            f"'{SILVER_JOB_PATH}' "
            f"--execution-date '{PROCESS_DATE_TEMPLATE}'"
        ),
    )

    check_partition >> run_job
