ARG BASE_IMAGE=apache/airflow:2.10.5-python3.10
FROM $BASE_IMAGE

# Disable Yarn apt repo if present (it can break apt-get update when its signing key rotates/expires)
RUN if [ -f /etc/apt/sources.list.d/yarn.list ]; then \
      sed -i 's/^deb /# deb /' /etc/apt/sources.list.d/yarn.list; \
    fi \
 && rm -f /usr/share/keyrings/yarn* /etc/apt/keyrings/yarn* 2>/dev/null || true

RUN pip install --upgrade pip

RUN pip install apache-airflow[amazon,celery,snowflake]==2.10.5 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.10.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
