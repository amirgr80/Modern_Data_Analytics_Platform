# Extends the base Airflow image with a JRE + PySpark so the Silver
# behavioral job can run in local mode inside the Airflow worker.
FROM docker.arvancloud.ir/apache/airflow:3.2.1-python3.12

USER root

# spark-submit / PySpark need a JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME so PySpark reliably finds the JVM (path matches default-jre-headless
# on Debian bookworm; adjust if your base image differs).
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# PySpark provides the spark-submit binary; version pinned to match the
# cluster (3.5.3). The spark provider is already in _PIP_ADDITIONAL_REQUIREMENTS
# but is harmless to have here too.
RUN pip install --no-cache-dir pyspark==3.5.3
