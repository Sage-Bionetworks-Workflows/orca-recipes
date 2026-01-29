ARG BASE_IMAGE=apache/airflow:2.10.5-python3.10
FROM $BASE_IMAGE

# remove yarn repo and then add it back with refreshed key
RUN set -eux; \
    rm -f /etc/apt/sources.list.d/yarn.list /etc/apt/sources.list.d/yarn*.list || true; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    install -d -m 0755 /etc/apt/keyrings; \
    curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg \
      | gpg --dearmor -o /etc/apt/keyrings/yarn-archive-keyring.gpg; \
    echo "deb [signed-by=/etc/apt/keyrings/yarn-archive-keyring.gpg] https://dl.yarnpkg.com/debian stable main" \
      > /etc/apt/sources.list.d/yarn.list; \
    apt-get update; \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install apache-airflow[amazon,celery,snowflake]==2.10.5 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
