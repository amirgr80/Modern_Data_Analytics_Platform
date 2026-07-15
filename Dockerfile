FROM docker.arvancloud.ir/apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade \
    pip \
    setuptools \
    wheel

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

ENV PYTHONPATH=/app

CMD ["spark-submit", \
     "--packages", \
     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "jobs/bronze_transactional_job.py"]
