from __future__ import annotations

import os

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


TEHRAN_TZ = pendulum.timezone("Asia/Tehran")

# Same package set we pass on the command line when running the job by
# hand. spark-submit downloads these from Maven on first run (cached
# afterwards for the life of the worker container).
SPARK_PACKAGES = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "org.apache.spark:spark-avro_2.12:3.5.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    ]
)

# Path to the job as mounted inside the Airflow containers
# (./src -> /opt/airflow/src in docker-compose.yml).
SILVER_JOB_PATH = "/opt/airflow/src/jobs/silver_behavioral_job.py"


# Retry mechanism is an explicit project evaluation criterion, so it's
# set at the DAG level here (3 attempts, 5 min apart).
default_args = {
    "owner": "team5",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}


def check_bronze_partition_exists(**context) -> None:
    """
    Fail fast with a clear message if the Bronze behavioral partition for
    this run's date doesn't exist yet, instead of letting the Spark job
    fail deep inside a read with a less obvious error.
    """
    import boto3

    ds = context["ds"]  # 'YYYY-MM-DD'
    year, month, day = (int(part) for part in ds.split("-"))

    bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    prefix = f"behavioral/events/year={year}/month={month}/day={day}/"

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    if response.get("KeyCount", 0) == 0:
        raise FileNotFoundError(
            f"No Bronze behavioral data at s3://{bucket}/{prefix} for {ds}. "
            "Is the Bronze streaming job producing data?"
        )


with DAG(
    dag_id="silver_behavioral_etl",
    description="Bronze -> Silver batch ETL for behavioral events (star schema on Iceberg)",
    default_args=default_args,
    schedule="0 2 * * *",  # 02:00 Tehran, after the day's Bronze data has landed
    start_date=pendulum.datetime(2026, 1, 1, tz=TEHRAN_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "behavioral", "team5"],
) as dag:

    check_bronze_partition = PythonOperator(
        task_id="check_bronze_partition_exists",
        python_callable=check_bronze_partition_exists,
    )

    run_silver_job = BashOperator(
        task_id="run_silver_behavioral_job",
        # SPARK_MASTER_URL=local[*] makes create_iceberg_spark_session build
        # a local session (the code reads that env var), so the job runs
        # in-process in the Airflow worker -- no Spark-cluster networking.
        bash_command=(
            "SPARK_MASTER_URL='local[*]' "
            "ICEBERG_REST_URI='http://lakekeeper:8181/catalog' "
            "ICEBERG_WAREHOUSE='warehouse' "
            "spark-submit --master 'local[*]' "
            f"--packages {SPARK_PACKAGES} "
            f"{SILVER_JOB_PATH} "
            "--execution-date {{ ds }}"
        ),
    )

    check_bronze_partition >> run_silver_job
