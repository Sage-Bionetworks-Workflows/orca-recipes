ARG BASE_IMAGE=apache/airflow:2.10.5-python3.10
FROM $BASE_IMAGE

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg \
    | gpg --dearmor -o /etc/apt/keyrings/yarn-archive-keyring.gpg \
 && echo "deb [signed-by=/etc/apt/keyrings/yarn-archive-keyring.gpg] https://dl.yarnpkg.com/debian stable main" \
    > /etc/apt/sources.list.d/yarn.list \
 && apt-get update

RUN pip install --upgrade pip

RUN pip install apache-airflow[amazon,celery,snowflake]==2.10.5 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
