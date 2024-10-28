ARG BASE_IMAGE=apache/airflow:2.9.3-python3.10
FROM $BASE_IMAGE

RUN pip install --upgrade pip

RUN pip install apache-airflow[amazon,celery,snowflake]==2.9.3 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

USER root
RUN apt-get update
RUN apt-get -y install git

# change back to base image user
USER 50000

# Installed from git a version of py-orca that is compatible with the version of the airflow image
RUN pip install -e git+https://github.com/Sage-Bionetworks-Workflows/py-orca@7c4781c4750ca8025deb1c6665730d7d46844fbc#egg=py-orca[all]

COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
