FROM apache/airflow:2.9.3-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    curl \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

