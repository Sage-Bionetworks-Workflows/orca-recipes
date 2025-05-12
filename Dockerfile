ARG BASE_IMAGE=apache/airflow:2.10.5-python3.10
FROM $BASE_IMAGE

RUN pip install --upgrade pip

RUN pip install apache-airflow[amazon,celery,snowflake]==2.10.5 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
